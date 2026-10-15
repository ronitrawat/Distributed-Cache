#include "lru_cache.h"

#include <stdexcept>

LRUCache::LRUCache(std::size_t capacity, NowFunction now)
    : LRUCache(capacity, 1, std::move(now)) {}

LRUCache::LRUCache(
    std::size_t capacity,
    std::size_t shardCount,
    NowFunction now)
    : capacity_(capacity), now_(std::move(now)) {
    if (shardCount == 0) {
        throw std::invalid_argument("shard count must be greater than zero");
    }

    shards_.reserve(shardCount);
    const std::size_t baseCapacity = capacity / shardCount;
    const std::size_t extraCapacity = capacity % shardCount;
    for (std::size_t index = 0; index < shardCount; ++index) {
        const std::size_t shardCapacity = baseCapacity + (index < extraCapacity ? 1 : 0);
        shards_.push_back(std::make_unique<Shard>(shardCapacity));
    }
}

std::optional<std::string> LRUCache::get(const std::string& key) {
    Shard& shard = shardFor(key);
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto found = shard.index.find(key);
    if (found == shard.index.end()) {
        return std::nullopt;
    }

    const auto entry = found->second;
    if (entry->expiry && *entry->expiry <= now_()) {
        shard.entries.erase(entry);
        shard.index.erase(found);
        return std::nullopt;
    }

    moveToFront(shard, found->second);
    return found->second->value;
}

void LRUCache::put(
    const std::string& key,
    const std::string& value,
    std::optional<std::chrono::seconds> ttl) {
    Shard& shard = shardFor(key);
    std::lock_guard<std::mutex> lock(shard.mutex);

    std::optional<TimePoint> expiry;
    if (ttl) {
        expiry = now_() + *ttl;
    }

    const auto found = shard.index.find(key);
    if (found != shard.index.end()) {
        found->second->value = value;
        found->second->expiry = expiry;
        moveToFront(shard, found->second);
        if (expiry) {
            shard.expiryHeap.emplace(*expiry, key);
        }
        return;
    }

    if (shard.capacity == 0) {
        return;
    }

    if (shard.entries.size() == shard.capacity) {
        const std::string& leastRecentlyUsedKey = shard.entries.back().key;
        shard.index.erase(leastRecentlyUsedKey);
        shard.entries.pop_back();
    }

    shard.entries.push_front(Entry{key, value, expiry});
    shard.index.emplace(shard.entries.front().key, shard.entries.begin());
    if (expiry) {
        shard.expiryHeap.emplace(*expiry, key);
    }
}

bool LRUCache::erase(const std::string& key) {
    Shard& shard = shardFor(key);
    std::lock_guard<std::mutex> lock(shard.mutex);
    const auto found = shard.index.find(key);
    if (found == shard.index.end()) {
        return false;
    }

    shard.entries.erase(found->second);
    shard.index.erase(found);
    return true;
}

std::size_t LRUCache::cleanup() {
    const auto currentTime = now_();
    std::size_t removed = 0;

    for (const auto& shardPointer : shards_) {
        Shard& shard = *shardPointer;
        std::lock_guard<std::mutex> lock(shard.mutex);
        while (!shard.expiryHeap.empty() &&
               shard.expiryHeap.top().first <= currentTime) {
            const auto [expiry, key] = shard.expiryHeap.top();
            shard.expiryHeap.pop();

            const auto found = shard.index.find(key);
            if (found == shard.index.end()) {
                continue;
            }

            const auto entry = found->second;
            if (entry->expiry && *entry->expiry == expiry) {
                shard.entries.erase(entry);
                shard.index.erase(found);
                ++removed;
            }
        }
    }

    return removed;
}

std::size_t LRUCache::size() const {
    std::size_t total = 0;
    for (const auto& shardPointer : shards_) {
        const Shard& shard = *shardPointer;
        std::lock_guard<std::mutex> lock(shard.mutex);
        total += shard.entries.size();
    }
    return total;
}

std::size_t LRUCache::capacity() const noexcept {
    return capacity_;
}

std::size_t LRUCache::shardCount() const noexcept {
    return shards_.size();
}

LRUCache::Shard& LRUCache::shardFor(const std::string& key) const {
    const std::size_t index = std::hash<std::string>{}(key) % shards_.size();
    return *shards_[index];
}

void LRUCache::moveToFront(Shard& shard, std::list<Entry>::iterator entry) {
    shard.entries.splice(shard.entries.begin(), shard.entries, entry);
}