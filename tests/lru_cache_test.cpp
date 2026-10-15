#include "lru_cache.h"

#include <chrono>
#include <gtest/gtest.h>
#include <algorithm>
#include <functional>
#include <stdexcept>
#include <string>
#include <thread>
#include <vector>

namespace {

std::vector<std::vector<std::string>> keysForShards(
    std::size_t shardCount,
    std::size_t keysPerShard) {
    std::vector<std::vector<std::string>> keys(shardCount);
    std::size_t remaining = shardCount * keysPerShard;
    for (std::size_t candidate = 0; remaining > 0; ++candidate) {
        const std::string key = "shard-key-" + std::to_string(candidate);
        const std::size_t shard = std::hash<std::string>{}(key) % shardCount;
        if (keys[shard].size() < keysPerShard) {
            keys[shard].push_back(key);
            --remaining;
        }
    }
    return keys;
}

}  // namespace

TEST(LRUCacheTest, EvictsLeastRecentlyUsedEntry) {
    LRUCache cache(2);
    cache.put("first", "1");
    cache.put("second", "2");

    ASSERT_EQ(cache.get("first"), std::optional<std::string>("1"));
    cache.put("third", "3");

    EXPECT_FALSE(cache.get("second").has_value());
    EXPECT_EQ(cache.get("first"), std::optional<std::string>("1"));
    EXPECT_EQ(cache.get("third"), std::optional<std::string>("3"));
}

TEST(LRUCacheTest, UpdatingExistingKeyChangesValueAndRecency) {
    LRUCache cache(2);
    cache.put("first", "old");
    cache.put("second", "2");

    cache.put("first", "new");
    cache.put("third", "3");

    EXPECT_EQ(cache.get("first"), std::optional<std::string>("new"));
    EXPECT_FALSE(cache.get("second").has_value());
    EXPECT_EQ(cache.size(), 2U);
}

TEST(LRUCacheTest, CapacityOneKeepsOnlyMostRecentEntry) {
    LRUCache cache(1);
    cache.put("first", "1");

    EXPECT_EQ(cache.get("first"), std::optional<std::string>("1"));
    cache.put("second", "2");

    EXPECT_FALSE(cache.get("first").has_value());
    EXPECT_EQ(cache.get("second"), std::optional<std::string>("2"));
    EXPECT_EQ(cache.size(), 1U);
    EXPECT_EQ(cache.capacity(), 1U);
}

TEST(LRUCacheTest, GetLazilyRemovesExpiredEntry) {
    auto currentTime = LRUCache::TimePoint{};
    LRUCache cache(2, [&currentTime] { return currentTime; });
    cache.put("key", "value", std::chrono::seconds(5));

    currentTime += std::chrono::seconds(5);

    EXPECT_FALSE(cache.get("key").has_value());
    EXPECT_EQ(cache.size(), 0U);
}

TEST(LRUCacheTest, CleanupRemovesExpiredEntriesAndKeepsLiveOnes) {
    auto currentTime = LRUCache::TimePoint{};
    LRUCache cache(3, [&currentTime] { return currentTime; });
    cache.put("expired", "old", std::chrono::seconds(5));
    cache.put("live", "new", std::chrono::seconds(10));

    currentTime += std::chrono::seconds(6);

    EXPECT_EQ(cache.cleanup(), 1U);
    EXPECT_FALSE(cache.get("expired").has_value());
    EXPECT_EQ(cache.get("live"), std::optional<std::string>("new"));
}

TEST(LRUCacheTest, CleanupIgnoresStaleExpiryAfterUpdate) {
    auto currentTime = LRUCache::TimePoint{};
    LRUCache cache(2, [&currentTime] { return currentTime; });
    cache.put("key", "old", std::chrono::seconds(5));

    currentTime += std::chrono::seconds(2);
    cache.put("key", "new", std::chrono::seconds(10));
    currentTime += std::chrono::seconds(4);

    EXPECT_EQ(cache.cleanup(), 0U);
    EXPECT_EQ(cache.get("key"), std::optional<std::string>("new"));

    currentTime += std::chrono::seconds(6);
    EXPECT_EQ(cache.cleanup(), 1U);
    EXPECT_FALSE(cache.get("key").has_value());
}

TEST(LRUCacheTest, CleanupIgnoresStaleExpiryAfterDeleteAndReinsert) {
    auto currentTime = LRUCache::TimePoint{};
    LRUCache cache(2, [&currentTime] { return currentTime; });
    cache.put("key", "old", std::chrono::seconds(5));
    ASSERT_TRUE(cache.erase("key"));
    cache.put("key", "new");

    currentTime += std::chrono::seconds(5);

    EXPECT_EQ(cache.cleanup(), 0U);
    EXPECT_EQ(cache.get("key"), std::optional<std::string>("new"));
}

TEST(LRUCacheTest, ShardsRouteKeysAndEnforceTheirCapacityQuotas) {
    constexpr std::size_t shardCount = 4;
    LRUCache cache(8, shardCount);
    const auto keys = keysForShards(shardCount, 3);

    ASSERT_EQ(cache.shardCount(), shardCount);
    ASSERT_EQ(cache.capacity(), 8U);
    for (std::size_t shard = 0; shard < shardCount; ++shard) {
        ASSERT_EQ(keys[shard].size(), 3U);
        cache.put(keys[shard][0], "oldest");
        cache.put(keys[shard][1], "newer");
    }
    ASSERT_EQ(cache.size(), 8U);

    cache.put(keys[2][2], "new entry");

    EXPECT_FALSE(cache.get(keys[2][0]).has_value());
    EXPECT_EQ(cache.get(keys[2][1]), std::optional<std::string>("newer"));
    EXPECT_EQ(cache.get(keys[2][2]), std::optional<std::string>("new entry"));
    EXPECT_EQ(cache.get(keys[1][0]), std::optional<std::string>("oldest"));
    EXPECT_EQ(cache.size(), 8U);
}

TEST(LRUCacheTest, RejectsZeroShards) {
    EXPECT_THROW(LRUCache(10, 0), std::invalid_argument);
}

TEST(LRUCacheTest, CleanupVisitsEachShardExpiryHeap) {
    constexpr std::size_t shardCount = 4;
    auto currentTime = LRUCache::TimePoint{};
    LRUCache cache(8, shardCount, [&currentTime] { return currentTime; });
    const auto keys = keysForShards(shardCount, 1);

    for (std::size_t shard = 0; shard < shardCount; ++shard) {
        cache.put(keys[shard][0], "value", std::chrono::seconds(5));
    }
    currentTime += std::chrono::seconds(5);

    EXPECT_EQ(cache.cleanup(), shardCount);
    EXPECT_EQ(cache.size(), 0U);
}

TEST(LRUCacheTest, ConcurrentOperationsAreSafeAcrossShards) {
    constexpr std::size_t shardCount = 8;
    constexpr std::size_t threadCount = 16;
    LRUCache cache(128, shardCount);
    const auto keys = keysForShards(shardCount, 2);
    std::vector<std::thread> threads;
    threads.reserve(threadCount);

    for (std::size_t threadIndex = 0; threadIndex < threadCount; ++threadIndex) {
        const std::size_t shard = threadIndex % shardCount;
        const std::string key = keys[shard][threadIndex / shardCount];
        threads.emplace_back([&cache, key, threadIndex] {
            for (std::size_t iteration = 0; iteration < 2000; ++iteration) {
                cache.put(key, std::to_string(threadIndex));
                EXPECT_TRUE(cache.get(key).has_value());
                if (iteration % 5 == 0) {
                    cache.erase(key);
                }
                cache.size();
                cache.cleanup();
            }
            cache.erase(key);
        });
    }

    for (auto& thread : threads) {
        thread.join();
    }
    EXPECT_EQ(cache.size(), 0U);
}