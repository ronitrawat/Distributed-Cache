#pragma once

#include <chrono>
#include <cstddef>
#include <functional>
#include <list>
#include <memory>
#include <mutex>
#include <optional>
#include <queue>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

class LRUCache {
public:
    using Clock = std::chrono::steady_clock;
    using TimePoint = Clock::time_point;
    using NowFunction = std::function<TimePoint()>;

    explicit LRUCache(
        std::size_t capacity,
        NowFunction now = [] { return Clock::now(); });
    LRUCache(
        std::size_t capacity,
        std::size_t shardCount,
        NowFunction now = [] { return Clock::now(); });

    std::optional<std::string> get(const std::string& key);
    void put(
        const std::string& key,
        const std::string& value,
        std::optional<std::chrono::seconds> ttl = std::nullopt);
    bool erase(const std::string& key);
    std::size_t cleanup();

    std::size_t size() const;
    std::size_t capacity() const noexcept;
    std::size_t shardCount() const noexcept;

private:
    struct Entry {
        std::string key;
        std::string value;
        std::optional<TimePoint> expiry;
    };

    struct Shard {
        explicit Shard(std::size_t shardCapacity) : capacity(shardCapacity) {}

        mutable std::mutex mutex;
        const std::size_t capacity;
        std::list<Entry> entries;
        std::unordered_map<std::string, std::list<Entry>::iterator> index;
        std::priority_queue<
            std::pair<TimePoint, std::string>,
            std::vector<std::pair<TimePoint, std::string>>,
            std::greater<std::pair<TimePoint, std::string>>>
            expiryHeap;
    };

    Shard& shardFor(const std::string& key) const;
    static void moveToFront(Shard& shard, std::list<Entry>::iterator entry);

    const std::size_t capacity_;
    NowFunction now_;
    std::vector<std::unique_ptr<Shard>> shards_;
};