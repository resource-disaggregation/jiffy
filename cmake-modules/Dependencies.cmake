include(ExternalProject)
include(GNUInstallDirs)

# External C/C++ Flags
set(EXTERNAL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EXTERNAL_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

# Prefer static to dynamic libraries
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX} ${CMAKE_FIND_LIBRARY_SUFFIXES})

if (USE_SYSTEM_THRIFT)
  set(USE_SYSTEM_BOOST ON)
  set(USE_SYSTEM_LIBEVENT ON)
endif()

if (USE_SYSTEM_AWSSDK)
  set(USE_SYSTEM_ZLIB ON)
  set(USE_SYSTEM_CURL ON)
endif()

# Threads
find_package(Threads REQUIRED)

# Boost (program_options; headers required for Thrift)
include(BoostExternal)

# OpenSSL (Required for AWS SDK, Apache Thrift)
include(OpenSSLExternal)

# Curl (required for AWS SDK)
include(CurlExternal)

# Zlib (required for AWS SDK)
include(ZlibExternal)

# AWS SDK
include(AwsSDKExternal)

# Libevent (Required for thrift)
include(LibeventExternal)

# Apache Thrift
include(ThriftExternal)

# Jemalloc
include(JemallocExternal)

# Catch2
include(CatchExternal)

# Documentation tools
if (BUILD_DOC)
    find_package(MkDocs REQUIRED)
    find_package(Doxygen ${DOXYGEN_VERSION} REQUIRED)
endif ()
