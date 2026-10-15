#include <algorithm>
#include <charconv>
#include <cerrno>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstdint>
#include <exception>
#include <iomanip>
#include <iostream>
#include <mutex>
#include <random>
#include <stdexcept>
#include <string>
#include <string_view>
#include <system_error>
#include <thread>
#include <vector>

#include <netdb.h>
#include <sys/socket.h>
#include <unistd.h>

namespace {

class FileDescriptor {
public:
    explicit FileDescriptor(int value = -1) noexcept : value_(value) {}
    ~FileDescriptor() {
        if (value_ >= 0) {
            ::close(value_);
        }
    }

    FileDescriptor(const FileDescriptor&) = delete;
    FileDescriptor& operator=(const FileDescriptor&) = delete;

    FileDescriptor(FileDescriptor&& other) noexcept : value_(other.value_) {
        other.value_ = -1;
    }

    int get() const noexcept { return value_; }

private:
    int value_;
};

struct ClientConnection {
    explicit ClientConnection(int socket) : socket(socket) {}

    int socket;
    std::string unreadBytes;

    void sendCommand(const std::string& command) {
        std::size_t offset = 0;
        while (offset < command.size()) {
            const ssize_t sent = ::send(
                socket,
                command.data() + offset,
                command.size() - offset,
                MSG_NOSIGNAL);
            if (sent < 0 && errno == EINTR) {
                continue;
            }
            if (sent <= 0) {
                throw std::system_error(errno, std::generic_category(), "send");
            }
            offset += static_cast<std::size_t>(sent);
        }
    }

    std::string readLine() {
        while (true) {
            const std::size_t newline = unreadBytes.find('\n');
            if (newline != std::string::npos) {
                std::string line = unreadBytes.substr(0, newline);
                unreadBytes.erase(0, newline + 1);
                return line;
            }

            char buffer[4096];
            const ssize_t received = ::recv(socket, buffer, sizeof(buffer), 0);
            if (received < 0 && errno == EINTR) {
                continue;
            }
            if (received <= 0) {
                throw std::system_error(
                    received == 0 ? ECONNRESET : errno,
                    std::generic_category(),
                    "recv");
            }
            unreadBytes.append(buffer, static_cast<std::size_t>(received));
        }
    }
};

std::uint64_t parseUnsigned(std::string_view text, const char* name) {
    std::uint64_t value = 0;
    const auto [end, error] =
        std::from_chars(text.data(), text.data() + text.size(), value);
    if (error != std::errc{} || end != text.data() + text.size()) {
        throw std::invalid_argument(std::string("invalid ") + name);
    }
    return value;
}

FileDescriptor connectToServer(const addrinfo* addresses) {
    int lastError = ECONNREFUSED;
    for (const addrinfo* address = addresses; address != nullptr;
         address = address->ai_next) {
        FileDescriptor socket(
            ::socket(address->ai_family, address->ai_socktype, address->ai_protocol));
        if (socket.get() < 0) {
            lastError = errno;
            continue;
        }
        if (::connect(socket.get(), address->ai_addr, address->ai_addrlen) == 0) {
            return socket;
        }
        lastError = errno;
    }
    throw std::system_error(lastError, std::generic_category(), "connect");
}

struct ThreadResult {
    std::vector<std::uint64_t> latencyNanoseconds;
    std::uint64_t gets = 0;
    std::uint64_t sets = 0;
    std::string error;
};

std::uint64_t percentile(
    const std::vector<std::uint64_t>& sortedSamples,
    double fraction) {
    const auto rank = static_cast<std::size_t>(
        std::ceil(fraction * static_cast<double>(sortedSamples.size())));
    const std::size_t index = std::max<std::size_t>(1, rank) - 1;
    return sortedSamples[std::min(index, sortedSamples.size() - 1)];
}

void printUsage(const char* executable) {
    std::cerr << "Usage: " << executable
              << " <host> <port> <clients> <ops_per_client> <get_percent>"
                 " <key_count> <shards> [seed]\n"
              << "Example: " << executable
              << " 127.0.0.1 6379 32 3000 80 512 16 1\n";
}

}  // namespace

int main(int argc, char* argv[]) {
    if (argc < 8 || argc > 9) {
        printUsage(argv[0]);
        return 2;
    }

    try {
        const std::string host(argv[1]);
        const std::uint64_t port = parseUnsigned(argv[2], "port");
        const std::uint64_t clients = parseUnsigned(argv[3], "clients");
        const std::uint64_t operationsPerClient = parseUnsigned(argv[4], "ops_per_client");
        const std::uint64_t getPercent = parseUnsigned(argv[5], "get_percent");
        const std::uint64_t keyCount = parseUnsigned(argv[6], "key_count");
        const std::uint64_t shards = parseUnsigned(argv[7], "shards");
        const std::uint64_t seed = argc == 9 ? parseUnsigned(argv[8], "seed") : 1;

        if (port == 0 || port > 65535 || clients == 0 || operationsPerClient == 0 ||
            getPercent > 100 || keyCount == 0 || shards == 0) {
            throw std::invalid_argument("arguments must be positive and get_percent <= 100");
        }

        addrinfo hints{};
        hints.ai_family = AF_UNSPEC;
        hints.ai_socktype = SOCK_STREAM;
        addrinfo* addressList = nullptr;
        const std::string service = std::to_string(port);
        const int resolveResult =
            ::getaddrinfo(host.c_str(), service.c_str(), &hints, &addressList);
        if (resolveResult != 0) {
            throw std::runtime_error(::gai_strerror(resolveResult));
        }

        const std::size_t clientCount = static_cast<std::size_t>(clients);
        const std::size_t operationCount =
            static_cast<std::size_t>(operationsPerClient);
        const std::size_t keyCountSize = static_cast<std::size_t>(keyCount);
        std::vector<ThreadResult> results(clientCount);
        std::mutex startMutex;
        std::condition_variable startCondition;
        std::size_t readyClients = 0;
        bool startWorkload = false;
        std::vector<std::thread> threads;
        threads.reserve(clientCount);

        for (std::size_t clientIndex = 0; clientIndex < clientCount; ++clientIndex) {
            threads.emplace_back([&, clientIndex] {
                bool signaledReady = false;
                try {
                    FileDescriptor socket = connectToServer(addressList);
                    ClientConnection connection(socket.get());

                    for (std::size_t keyIndex = clientIndex;
                         keyIndex < keyCountSize;
                         keyIndex += clientCount) {
                        const std::string key = "benchkey" + std::to_string(keyIndex);
                        connection.sendCommand("SET " + key + " seed\n");
                        if (connection.readLine() != "OK") {
                            throw std::runtime_error("server rejected preload SET");
                        }
                    }

                    {
                        std::lock_guard<std::mutex> lock(startMutex);
                        ++readyClients;
                        signaledReady = true;
                    }
                    startCondition.notify_all();
                    {
                        std::unique_lock<std::mutex> lock(startMutex);
                        startCondition.wait(lock, [&] { return startWorkload; });
                    }

                    auto& result = results[clientIndex];
                    result.latencyNanoseconds.reserve(operationCount);
                    std::mt19937_64 random(seed + clientIndex);
                    std::uniform_int_distribution<std::uint64_t> choosePercent(0, 99);
                    std::uniform_int_distribution<std::uint64_t> chooseKey(0, keyCount - 1);

                    for (std::size_t operation = 0;
                         operation < operationCount;
                         ++operation) {
                        const std::string key =
                            "benchkey" + std::to_string(chooseKey(random));
                        const bool isGet = choosePercent(random) < getPercent;
                        std::string request;
                        if (isGet) {
                            request = "GET " + key + "\n";
                        } else {
                            request = "SET " + key + " value" +
                                std::to_string(clientIndex) + "_" +
                                std::to_string(operation) + "\n";
                        }

                        const auto operationStart = std::chrono::steady_clock::now();
                        connection.sendCommand(request);
                        const std::string response = connection.readLine();
                        const auto operationEnd = std::chrono::steady_clock::now();

                        if (response.empty() || response.compare(0, 4, "ERR ") == 0) {
                            throw std::runtime_error("server returned an error response");
                        }
                        result.latencyNanoseconds.push_back(
                            static_cast<std::uint64_t>(
                                std::chrono::duration_cast<std::chrono::nanoseconds>(
                                    operationEnd - operationStart)
                                    .count()));
                        if (isGet) {
                            ++result.gets;
                        } else {
                            ++result.sets;
                        }
                    }
                } catch (const std::exception& error) {
                    results[clientIndex].error = error.what();
                    if (!signaledReady) {
                        {
                            std::lock_guard<std::mutex> lock(startMutex);
                            ++readyClients;
                        }
                        startCondition.notify_all();
                    }
                }
            });
        }

        {
            std::unique_lock<std::mutex> lock(startMutex);
            startCondition.wait(lock, [&] { return readyClients == clientCount; });
        }
        const auto workloadStart = std::chrono::steady_clock::now();
        {
            std::lock_guard<std::mutex> lock(startMutex);
            startWorkload = true;
        }
        startCondition.notify_all();

        for (auto& thread : threads) {
            thread.join();
        }
        const auto workloadEnd = std::chrono::steady_clock::now();
        ::freeaddrinfo(addressList);

        std::vector<std::uint64_t> allLatencies;
        allLatencies.reserve(clientCount * operationCount);
        std::uint64_t totalGets = 0;
        std::uint64_t totalSets = 0;
        for (const auto& result : results) {
            if (!result.error.empty()) {
                throw std::runtime_error(result.error);
            }
            totalGets += result.gets;
            totalSets += result.sets;
            allLatencies.insert(
                allLatencies.end(),
                result.latencyNanoseconds.begin(),
                result.latencyNanoseconds.end());
        }

        std::sort(allLatencies.begin(), allLatencies.end());
        const std::uint64_t totalOperations = allLatencies.size();
        const double elapsedSeconds =
            std::chrono::duration<double>(workloadEnd - workloadStart).count();
        const double throughput = static_cast<double>(totalOperations) / elapsedSeconds;
        const auto toMicroseconds = [](std::uint64_t nanoseconds) {
            return static_cast<double>(nanoseconds) / 1000.0;
        };

        std::cout << "shards,clients,ops_per_client,total_ops,gets,sets,ops_per_sec"
                     ",p50_us,p95_us,p99_us\n";
        std::cout << shards << ',' << clients << ',' << operationsPerClient << ','
                  << totalOperations << ',' << totalGets << ',' << totalSets << ','
                  << std::fixed << std::setprecision(2) << throughput << ','
                  << std::setprecision(3)
                  << toMicroseconds(percentile(allLatencies, 0.50)) << ','
                  << toMicroseconds(percentile(allLatencies, 0.95)) << ','
                  << toMicroseconds(percentile(allLatencies, 0.99)) << '\n';
        return 0;
    } catch (const std::exception& error) {
        std::cerr << "Benchmark error: " << error.what() << '\n';
        printUsage(argv[0]);
        return 1;
    }
}