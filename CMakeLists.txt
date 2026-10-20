cmake_minimum_required(VERSION 3.21)
project(haptics_latest_value LANGUAGES CXX)

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

# Single-config generators (Makefiles, Ninja) default to Release; multi-config
# generators (Visual Studio) pick the configuration at build time. Reading
# CMAKE_BUILD_TYPE here also keeps presets that set it warning-free on Visual Studio.
if(NOT CMAKE_BUILD_TYPE AND NOT CMAKE_CONFIGURATION_TYPES)
  set(CMAKE_BUILD_TYPE Release CACHE STRING "Build type" FORCE)
endif()

option(HAPTICS_SANITIZE_THREAD  "Build with ThreadSanitizer (GCC/Clang only)" OFF)
option(HAPTICS_SANITIZE_ADDRESS "Build with AddressSanitizer"                  OFF)

find_package(Threads REQUIRED)

# Header-only library holding ForceCommand and the channel.
add_library(haptics_channel INTERFACE)
target_include_directories(haptics_channel INTERFACE ${CMAKE_CURRENT_SOURCE_DIR}/include)
target_link_libraries(haptics_channel INTERFACE Threads::Threads)

if(MSVC)
  # C4324 ("structure was padded due to alignment specifier") flags the cache-line
  # padding in LatestValueChannel, which is intentional.
  target_compile_options(haptics_channel INTERFACE /W4 /permissive- /wd4324)
else()
  target_compile_options(haptics_channel INTERFACE -Wall -Wextra -Wpedantic)
endif()

if(HAPTICS_SANITIZE_THREAD)
  if(MSVC)
    message(FATAL_ERROR "ThreadSanitizer is not available with MSVC; use WSL/Linux with clang or gcc.")
  endif()
  target_compile_options(haptics_channel INTERFACE -fsanitize=thread -g -O1)
  target_link_options(haptics_channel INTERFACE -fsanitize=thread)
endif()

if(HAPTICS_SANITIZE_ADDRESS)
  if(MSVC)
    target_compile_options(haptics_channel INTERFACE /fsanitize=address)
  else()
    target_compile_options(haptics_channel INTERFACE -fsanitize=address -fno-omit-frame-pointer -g)
    target_link_options(haptics_channel INTERFACE -fsanitize=address)
  endif()
endif()

# Demo: 6 kHz producer -> 4 kHz consumer, mirrors the scenario in the brief.
add_executable(demo src/main.cpp)
target_link_libraries(demo PRIVATE haptics_channel)

# Focused concurrency test (no external framework so it builds offline).
enable_testing()
add_executable(concurrency_test tests/concurrency_test.cpp)
target_link_libraries(concurrency_test PRIVATE haptics_channel)
add_test(NAME concurrency_test COMMAND concurrency_test)
