cmake_minimum_required(VERSION 3.16)
project(FlowLoopX VERSION 0.1.0 LANGUAGES CXX)

if(NOT CMAKE_SYSTEM_NAME STREQUAL "Linux")
  message(FATAL_ERROR "FlowLoopX uses epoll and requires Linux.")
endif()

set(FLX_CXX_STANDARD 20 CACHE STRING "C++ standard to build with (17 or 20)")
set(CMAKE_CXX_STANDARD ${FLX_CXX_STANDARD})
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS ON)   # need GNU extensions for memrchr/accept4

if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(FLX_BUILD_TESTS "Build unit/integration tests" ON)
option(FLX_BUILD_BENCH "Build the load generator" ON)
set(FLX_SANITIZE "" CACHE STRING "Sanitizer to enable: address, thread, undefined")

find_package(Threads REQUIRED)

add_library(flowloopx STATIC
  src/server.cpp
  src/connection.cpp
  src/buffer.cpp
  src/thread_pool.cpp
  src/handler.cpp
  src/socket_util.cpp)
target_include_directories(flowloopx PUBLIC include)
target_link_libraries(flowloopx PUBLIC Threads::Threads)
target_compile_options(flowloopx PRIVATE -Wall -Wextra -Wpedantic)

if(FLX_SANITIZE)
  target_compile_options(flowloopx PUBLIC -fsanitize=${FLX_SANITIZE} -fno-omit-frame-pointer -g)
  target_link_options(flowloopx PUBLIC -fsanitize=${FLX_SANITIZE})
endif()

add_executable(flowloopx_server apps/flowloopx_server.cpp)
target_link_libraries(flowloopx_server PRIVATE flowloopx)
target_compile_options(flowloopx_server PRIVATE -Wall -Wextra)

if(FLX_BUILD_BENCH)
  add_executable(flowloopx_bench apps/flowloopx_bench.cpp)
  target_link_libraries(flowloopx_bench PRIVATE Threads::Threads)
  target_compile_options(flowloopx_bench PRIVATE -Wall -Wextra)
endif()

if(FLX_BUILD_TESTS)
  enable_testing()
  add_executable(flowloopx_tests tests/test_main.cpp)
  target_link_libraries(flowloopx_tests PRIVATE flowloopx)
  target_compile_options(flowloopx_tests PRIVATE -Wall -Wextra)
  add_test(NAME flowloopx_tests COMMAND flowloopx_tests)
  set_tests_properties(flowloopx_tests PROPERTIES TIMEOUT 120)

  # Replaces global operator new, which sanitizers also interpose: skip it in sanitizer builds.
  if(NOT FLX_SANITIZE)
    add_executable(flowloopx_alloc_test tests/test_alloc.cpp)
    target_link_libraries(flowloopx_alloc_test PRIVATE flowloopx)
    target_compile_options(flowloopx_alloc_test PRIVATE -Wall -Wextra)
    add_test(NAME flowloopx_alloc_test COMMAND flowloopx_alloc_test)
    set_tests_properties(flowloopx_alloc_test PROPERTIES TIMEOUT 120)
  endif()
endif()
