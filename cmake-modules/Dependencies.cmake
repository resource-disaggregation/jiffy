include(ExternalProject)
include(GNUInstallDirs)

set(LIBEVENT_VERSION "2.1.8")
set(THRIFT_VERSION "0.11.0")
set(ZLIB_VERSION "1.2.11")
set(OPENSSL_VERSION "1.1.1-pre7")
set(CURL_VERSION "7.60.0")
set(AWSSDK_VERSION "1.4.26")
set(BOOST_VERSION "1.69.0")
set(CATCH_VERSION "2.2.1")
set(LIBCUCKOO_VERSION "0.2")
set(DOXYGEN_VERSION "1.8")

# External C/C++ Flags
set(EXTERNAL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EXTERNAL_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

# Prefer static to dynamic libraries
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX} ${CMAKE_FIND_LIBRARY_SUFFIXES})

# Threads
find_package(Threads REQUIRED)

# Boost
include(BoostExternal)

# OpenSSL
include(OpenSSLExternal)

# AWS SDK
include(AwsSDKExternal)

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
