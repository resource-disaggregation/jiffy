cmake_minimum_required(VERSION 3.9)
project(elasticmem C CXX)

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(PROJECT_NAME "ElasticMem")
set(PROJECT_VERSION_MAJOR "0")
set(PROJECT_VERSION_MINOR "1")
set(PROJECT_VERSION_PATCH "0")
set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake-modules")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -O0 -Wall -pedantic -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_DEBUG} -Wall -pedantic -Wextra")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -Wall -pedantic -Wextra")

set(CMAKE_BUILD_TYPE RelWithDebInfo)

# CMake modules
include(Functions)
include(BuildOptions)
include(Dependencies)

if (GENERATE_THRIFT)
  add_subdirectory(thrift)
endif ()

add_subdirectory(libelasticmem)

add_subdirectory(directory)
add_subdirectory(storage)

add_subdirectory(pyclient)

if (BUILD_TESTS)
  enable_testing(true)
endif ()