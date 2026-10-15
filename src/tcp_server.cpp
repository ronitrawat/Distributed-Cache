#include "tcp_server.h"

#include "lru_cache.h"
#include "protocol_parser.h"

#include <array>
#include <cerrno>
#include <condition_variable>
#include <csignal>
#include <cstdint>
#include <deque>
#include <exception>
#include <functional>
#include <iostream>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <system_error>
#include <thread>
#include <unordered_map>
#include <utility>
#include <variant>
#include <vector>

#include <arpa/inet.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

constexpr std::size_t kWorkerCount = 4;
constexpr std::size_t kReceiveBufferSize = 4096;
constexpr int kMaxEpollEvents = 64;

volatile std::sig_atomic_t stopRequested = 0;
volatile std::sig_atomic_t signalWakeDescriptor = -1;

extern "C" void handleStopSignal(int) {
    const int savedErrno = errno;
    stopRequested = 1;
    const int wakeDescriptor = signalWakeDescriptor;
    if (wakeDescriptor >= 0) {
        const std::uint64_t one = 1;
        ::write(wakeDescriptor, &one, sizeof(one));
    }
    errno = savedErrno;
}

class FileDescriptor {
public:
    explicit FileDescriptor(int value = -1) noexcept : value_(value) {}
    ~FileDescriptor() { reset(); }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept
        : value_(std::exchange(other.value_, -1)) {}

    FileDescriptor& operator=(FileDescriptor&& other) noexcept {
        if (this != &other) {
            reset();
            value_ = std::exchange(other.value_, -1);
        }
        return *this;
    }

    int get() const noexcept { return value_; }
    explicit operator bool() const noexcept { return value_ >= 0; }

private:
    void reset() noexcept {
        if (value_ >= 0) {
            ::close(value_);
            value_ = -1;
        }
    }

    int value_;
};

struct SharedCache {
    SharedCache(std::size_t capacity, std::size_t shardCount)
        : cache(capacity, shardCount) {}
    LRUCache cache;
};

struct Completion {
    int socket;
    std::uint64_t generation;
    std::string response;
};

struct CompletionQueue {
    explicit CompletionQueue(int eventDescriptor) : eventDescriptor(eventDescriptor) {}

    void push(Completion completion) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            items.push_back(std::move(completion));
        }

        const std::uint64_t one = 1;
        while (::write(eventDescriptor, &one, sizeof(one)) < 0 && errno == EINTR) {
        }
    }

    std::deque<Completion> takeAll() {
        std::deque<Completion> result;
        std::lock_guard<std::mutex> lock(mutex);
        result.swap(items);
        return result;
    }

    int eventDescriptor;
    std::mutex mutex;
    std::deque<Completion> items;
};

class ThreadPool {
public:
    explicit ThreadPool(std::size_t workerCount) {
        workers.reserve(workerCount);
        for (std::size_t index = 0; index < workerCount; ++index) {
            workers.emplace_back([this] { workerLoop(); });
        }
    }

    ~ThreadPool() { shutdown(); }

    ThreadPool(const ThreadPool&) = delete;
    ThreadPool& operator=(const ThreadPool&) = delete;

    bool submit(std::function<void()> task) {
        {
            std::lock_guard<std::mutex> lock(mutex);
            if (stopping) {
                return false;
            }
            tasks.push(std::move(task));
        }
        ready.notify_one();
        return true;
    }

    void shutdown() noexcept {
        {
            std::lock_guard<std::mutex> lock(mutex);
            stopping = true;
        }
        ready.notify_all();
        for (auto& worker : workers) {
            if (worker.joinable()) {
                worker.join();
            }
        }
        workers.clear();
    }

private:
    void workerLoop() {
        while (true) {
            std::function<void()> task;
            {
                std::unique_lock<std::mutex> lock(mutex);
                ready.wait(lock, [this] { return stopping || !tasks.empty(); });
                if (stopping && tasks.empty()) {
                    return;
                }
                task = std::move(tasks.front());
                tasks.pop();
            }
            task();
        }
    }

    std::mutex mutex;
    std::condition_variable ready;
    std::queue<std::function<void()>> tasks;
    std::vector<std::thread> workers;
    bool stopping = false;
};

struct Connection {
    Connection(FileDescriptor socketDescriptor, std::uint64_t id)
        : descriptor(std::move(socketDescriptor)), generation(id) {}

    FileDescriptor descriptor;
    std::uint64_t generation;
    std::string input;
    std::deque<cache_protocol::ParsedItem> pendingCommands;
    std::string output;
    std::size_t outputOffset = 0;
    bool commandInFlight = false;
    bool peerClosed = false;
};

[[noreturn]] void throwSystemError(const char* operation) {
    throw std::system_error(errno, std::generic_category(), operation);
}

std::string makeResponse(
    const cache_protocol::ParsedItem& item,
    SharedCache& sharedCache) {
    if (const auto* error = std::get_if<cache_protocol::ParseError>(&item)) {
        return cache_protocol::formatErrorResponse(error->message);
    }

    const auto& command = std::get<cache_protocol::Command>(item);
    switch (command.type) {
        case cache_protocol::CommandType::Set: {
            sharedCache.cache.put(command.key, command.value, command.ttl);
            return cache_protocol::formatOkResponse();
        }
        case cache_protocol::CommandType::Get: {
            const auto value = sharedCache.cache.get(command.key);
            return value ? cache_protocol::formatValueResponse(*value)
                         : cache_protocol::formatNotFoundResponse();
        }
        case cache_protocol::CommandType::Del: {
            const bool erased = sharedCache.cache.erase(command.key);
            return erased ? cache_protocol::formatOkResponse()
                          : cache_protocol::formatNotFoundResponse();
        }
    }
    return cache_protocol::formatErrorResponse("invalid command");
}

std::uint32_t desiredEvents(const Connection& connection) {
    std::uint32_t events = EPOLLRDHUP;
    if (!connection.peerClosed) {
        events |= EPOLLIN;
    }
    if (connection.outputOffset < connection.output.size()) {
        events |= EPOLLOUT;
    }
    return events;
}

void updateInterest(int epollDescriptor, const Connection& connection) {
    epoll_event event{};
    event.events = desiredEvents(connection);
    event.data.fd = connection.descriptor.get();
    if (::epoll_ctl(
            epollDescriptor, EPOLL_CTL_MOD, connection.descriptor.get(), &event) < 0) {
        throwSystemError("epoll_ctl MOD client");
    }
}

void closeConnection(
    int epollDescriptor,
    std::unordered_map<int, Connection>& connections,
    int socket) {
    ::epoll_ctl(epollDescriptor, EPOLL_CTL_DEL, socket, nullptr);
    connections.erase(socket);
}

bool flushOutput(int socket, Connection& connection) {
    while (connection.outputOffset < connection.output.size()) {
        const ssize_t sent = ::send(
            socket,
            connection.output.data() + connection.outputOffset,
            connection.output.size() - connection.outputOffset,
            MSG_NOSIGNAL);
        if (sent < 0 && errno == EINTR) {
            continue;
        }
        if (sent < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true;
        }
        if (sent <= 0) {
            return false;
        }
        connection.outputOffset += static_cast<std::size_t>(sent);
    }
    connection.output.clear();
    connection.outputOffset = 0;
    return true;
}

bool readAvailable(int socket, Connection& connection) {
    std::array<char, kReceiveBufferSize> buffer{};
    while (true) {
        const ssize_t received = ::recv(socket, buffer.data(), buffer.size(), 0);
        if (received < 0 && errno == EINTR) {
            continue;
        }
        if (received < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
            return true;
        }
        if (received == 0) {
            connection.peerClosed = true;
            return true;
        }
        if (received < 0) {
            return false;
        }

        connection.input.append(buffer.data(), static_cast<std::size_t>(received));
        auto parsed = cache_protocol::ProtocolParser{}.parse(connection.input);
        connection.input = std::move(parsed.remainder);
        for (auto& item : parsed.items) {
            connection.pendingCommands.push_back(std::move(item));
        }
    }
}

void dispatchNext(
    int socket,
    Connection& connection,
    ThreadPool& workers,
    CompletionQueue& completions,
    const std::shared_ptr<SharedCache>& sharedCache) {
    if (connection.commandInFlight || connection.pendingCommands.empty()) {
        return;
    }

    auto item = std::move(connection.pendingCommands.front());
    connection.pendingCommands.pop_front();
    connection.commandInFlight = true;
    const std::uint64_t generation = connection.generation;

    const bool accepted = workers.submit(
        [socket, generation, item = std::move(item), &completions, sharedCache] {
            std::string response;
            try {
                response = makeResponse(item, *sharedCache);
            } catch (const std::exception& error) {
                response = cache_protocol::formatErrorResponse(error.what());
            }
            completions.push(Completion{socket, generation, std::move(response)});
        });

    if (!accepted) {
        connection.commandInFlight = false;
    }
}

class SignalHandlers {
public:
    SignalHandlers() {
        struct sigaction action {};
        action.sa_handler = handleStopSignal;
        ::sigemptyset(&action.sa_mask);
        if (::sigaction(SIGINT, &action, &previousInt) < 0) {
            throwSystemError("sigaction SIGINT");
        }
        if (::sigaction(SIGTERM, &action, &previousTerm) < 0) {
            ::sigaction(SIGINT, &previousInt, nullptr);
            throwSystemError("sigaction SIGTERM");
        }
        stopRequested = 0;
        installed = true;
    }

    ~SignalHandlers() {
        if (installed) {
            signalWakeDescriptor = -1;
            ::sigaction(SIGINT, &previousInt, nullptr);
            ::sigaction(SIGTERM, &previousTerm, nullptr);
        }
    }

private:
    struct sigaction previousInt {};
    struct sigaction previousTerm {};
    bool installed = false;
};

}  // namespace

void runTcpServer(
    std::uint16_t port,
    std::size_t cacheCapacity,
    std::size_t shardCount) {
    FileDescriptor listener(
        ::socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, 0));
    if (!listener) {
        throwSystemError("socket");
    }

    int reuseAddress = 1;
    if (::setsockopt(
            listener.get(), SOL_SOCKET, SO_REUSEADDR,
            &reuseAddress, sizeof(reuseAddress)) < 0) {
        throwSystemError("setsockopt SO_REUSEADDR");
    }

    sockaddr_in address{};
    address.sin_family = AF_INET;
    address.sin_addr.s_addr = htonl(INADDR_ANY);
    address.sin_port = htons(port);
    if (::bind(listener.get(), reinterpret_cast<const sockaddr*>(&address), sizeof(address)) < 0) {
        throwSystemError("bind");
    }
    if (::listen(listener.get(), SOMAXCONN) < 0) {
        throwSystemError("listen");
    }

    FileDescriptor epollDescriptor(::epoll_create1(EPOLL_CLOEXEC));
    if (!epollDescriptor) {
        throwSystemError("epoll_create1");
    }
    FileDescriptor completionEvent(::eventfd(0, EFD_NONBLOCK | EFD_CLOEXEC));
    if (!completionEvent) {
        throwSystemError("eventfd");
    }
    SignalHandlers signalHandlers;
    signalWakeDescriptor = completionEvent.get();

    epoll_event listenerEvent{};
    listenerEvent.events = EPOLLIN;
    listenerEvent.data.fd = listener.get();
    if (::epoll_ctl(epollDescriptor.get(), EPOLL_CTL_ADD, listener.get(), &listenerEvent) < 0) {
        throwSystemError("epoll_ctl ADD listener");
    }

    epoll_event completionConfig{};
    completionConfig.events = EPOLLIN;
    completionConfig.data.fd = completionEvent.get();
    if (::epoll_ctl(
            epollDescriptor.get(), EPOLL_CTL_ADD, completionEvent.get(), &completionConfig) < 0) {
        throwSystemError("epoll_ctl ADD completion event");
    }

    auto sharedCache = std::make_shared<SharedCache>(cacheCapacity, shardCount);
    CompletionQueue completions(completionEvent.get());
    ThreadPool workers(kWorkerCount);
    std::unordered_map<int, Connection> connections;
    std::uint64_t nextGeneration = 1;
    std::array<epoll_event, kMaxEpollEvents> events{};

    std::cout << "Cache server listening on port " << port
              << " with " << kWorkerCount << " workers\n";

    while (!stopRequested) {
        const int count = ::epoll_wait(
            epollDescriptor.get(), events.data(),
            static_cast<int>(events.size()), -1);
        if (count < 0) {
            if (errno == EINTR) {
                continue;
            }
            throwSystemError("epoll_wait");
        }

        for (int index = 0; index < count; ++index) {
            const int ready = events[index].data.fd;
            const std::uint32_t flags = events[index].events;

            if (ready == listener.get()) {
                while (true) {
                    sockaddr_in clientAddress{};
                    socklen_t addressSize = sizeof(clientAddress);
                    const int accepted = ::accept4(
                        listener.get(),
                        reinterpret_cast<sockaddr*>(&clientAddress),
                        &addressSize,
                        SOCK_NONBLOCK | SOCK_CLOEXEC);
                    if (accepted < 0 && errno == EINTR) {
                        continue;
                    }
                    if (accepted < 0 && (errno == EAGAIN || errno == EWOULDBLOCK)) {
                        break;
                    }
                    if (accepted < 0) {
                        throwSystemError("accept4");
                    }

                    FileDescriptor client(accepted);
                    epoll_event clientEvent{};
                    clientEvent.events = EPOLLIN | EPOLLRDHUP;
                    clientEvent.data.fd = accepted;
                    if (::epoll_ctl(
                            epollDescriptor.get(), EPOLL_CTL_ADD, accepted, &clientEvent) < 0) {
                        continue;
                    }
                    connections.emplace(
                        std::piecewise_construct,
                        std::forward_as_tuple(accepted),
                        std::forward_as_tuple(std::move(client), nextGeneration++));
                }
                continue;
            }

            if (ready == completionEvent.get()) {
                std::uint64_t counter = 0;
                while (::read(completionEvent.get(), &counter, sizeof(counter)) < 0 &&
                       errno == EINTR) {
                }

                for (auto& completion : completions.takeAll()) {
                    const auto found = connections.find(completion.socket);
                    if (found == connections.end() ||
                        found->second.generation != completion.generation) {
                        continue;
                    }
                    Connection& connection = found->second;
                    connection.commandInFlight = false;
                    connection.output.append(completion.response);
                    dispatchNext(
                        completion.socket, connection, workers, completions, sharedCache);
                    if (!flushOutput(completion.socket, connection)) {
                        closeConnection(epollDescriptor.get(), connections, completion.socket);
                        continue;
                    }
                    if (connection.peerClosed && !connection.commandInFlight &&
                        connection.pendingCommands.empty() && connection.output.empty()) {
                        closeConnection(epollDescriptor.get(), connections, completion.socket);
                        continue;
                    }
                    updateInterest(epollDescriptor.get(), connection);
                }
                continue;
            }

            const auto found = connections.find(ready);
            if (found == connections.end()) {
                continue;
            }
            Connection& connection = found->second;
            bool keep = true;
            if ((flags & (EPOLLIN | EPOLLRDHUP | EPOLLHUP)) != 0) {
                keep = readAvailable(ready, connection);
            }
            if (keep && (flags & EPOLLERR) != 0) {
                keep = false;
            }
            if (!keep) {
                closeConnection(epollDescriptor.get(), connections, ready);
                continue;
            }

            dispatchNext(ready, connection, workers, completions, sharedCache);
            if ((flags & EPOLLOUT) != 0 && !flushOutput(ready, connection)) {
                closeConnection(epollDescriptor.get(), connections, ready);
                continue;
            }
            if (connection.peerClosed && !connection.commandInFlight &&
                connection.pendingCommands.empty() && connection.output.empty()) {
                closeConnection(epollDescriptor.get(), connections, ready);
                continue;
            }
            updateInterest(epollDescriptor.get(), connection);
        }
    }

    ::epoll_ctl(epollDescriptor.get(), EPOLL_CTL_DEL, listener.get(), nullptr);
    workers.shutdown();
    connections.clear();
}