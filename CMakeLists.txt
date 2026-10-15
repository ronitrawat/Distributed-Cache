cmake_minimum_required(VERSION 3.16)

project(distributed_cache LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

option(CACHE_ENABLE_TSAN "Build all targets with ThreadSanitizer" OFF)
if(CACHE_ENABLE_TSAN)
    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        message(FATAL_ERROR "CACHE_ENABLE_TSAN requires GCC or Clang")
    endif()
    add_compile_options(-fsanitize=thread -fno-omit-frame-pointer -g)
    add_link_options(-fsanitize=thread)
endif()

add_library(cache_lru
    src/lru_cache.cpp
    src/protocol_parser.cpp
)
target_include_directories(cache_lru PUBLIC include)

if(CMAKE_SYSTEM_NAME STREQUAL "Linux")
    find_package(Threads REQUIRED)

    add_executable(cache_server
        src/tcp_server.cpp
        src/tcp_server_main.cpp
    )
    target_link_libraries(cache_server PRIVATE cache_lru Threads::Threads)

    add_executable(cache_bench
        bench/cache_bench.cpp
    )
    target_link_libraries(cache_bench PRIVATE Threads::Threads)
endif()

include(CTest)

if(BUILD_TESTING)
    find_package(GTest REQUIRED)

    add_executable(lru_cache_tests
        tests/lru_cache_test.cpp
    )
    target_link_libraries(lru_cache_tests PRIVATE cache_lru GTest::gtest_main)

    include(GoogleTest)
    gtest_discover_tests(lru_cache_tests)

    add_executable(protocol_parser_tests
        tests/protocol_parser_test.cpp
    )
    target_link_libraries(protocol_parser_tests PRIVATE cache_lru GTest::gtest_main)
    gtest_discover_tests(protocol_parser_tests)
endif()