#include "tcp_server.h"

#include <charconv>
#include <cstdint>
#include <exception>
#include <iostream>
#include <limits>
#include <string_view>
#include <system_error>

int main(int argc, char* argv[]) {
    constexpr std::uint16_t defaultPort = 6379;
    constexpr std::size_t cacheCapacity = 1024;
    constexpr std::size_t defaultShardCount = 16;

    if (argc > 3) {
        std::cerr << "Usage: cache_server [port] [shard_count]\n";
        return 2;
    }

    unsigned int port = defaultPort;
    if (argc >= 2) {
        const std::string_view portText(argv[1]);
        const auto [end, error] = std::from_chars(
            portText.data(), portText.data() + portText.size(), port);
        if (error != std::errc{} || end != portText.data() + portText.size() ||
            port == 0 || port > std::numeric_limits<std::uint16_t>::max()) {
            std::cerr << "Port must be an integer between 1 and 65535\n";
            return 2;
        }
    }

    unsigned int shardCount = static_cast<unsigned int>(defaultShardCount);
    if (argc == 3) {
        const std::string_view shardText(argv[2]);
        const auto [end, error] = std::from_chars(
            shardText.data(), shardText.data() + shardText.size(), shardCount);
        if (error != std::errc{} || end != shardText.data() + shardText.size() ||
            shardCount == 0) {
            std::cerr << "Shard count must be a positive integer\n";
            return 2;
        }
    }

    try {
        runTcpServer(
            static_cast<std::uint16_t>(port),
            cacheCapacity,
            static_cast<std::size_t>(shardCount));
    } catch (const std::exception& error) {
        std::cerr << "Cache server error: " << error.what() << '\n';
        return 1;
    }

    return 0;
}