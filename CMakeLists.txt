cmake_minimum_required(VERSION 3.9)
project(elasticmem C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -lstdc++fs")

set(PROJECT_NAME "ElasticMem")
set(PROJECT_VERSION_MAJOR "0")
set(PROJECT_VERSION_MINOR "1")
set(PROJECT_VERSION_PATCH "0")
set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake-modules")

# CMake modules
include(BuildOptions)
include(Dependencies)

if (GENERATE_THRIFT)
  add_subdirectory(thrift)
endif ()

add_subdirectory(libdirectory)
add_subdirectory(libkv)

add_subdirectory(directory)
add_subdirectory(kv)

if (BUILD_TESTS)
  enable_testing(true)
endif ()