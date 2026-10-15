#pragma once

#include <cstddef>
#include <cstdint>

void runTcpServer(
	std::uint16_t port,
	std::size_t cacheCapacity,
	std::size_t shardCount);