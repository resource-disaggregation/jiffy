include(ExternalProject)
include(GNUInstallDirs)

# External C/C++ Flags
set(EXTERNAL_CXX_FLAGS "${CMAKE_CXX_FLAGS} ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EXTERNAL_C_FLAGS "${CMAKE_C_FLAGS} ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

# Prefer static to dynamic libraries
set(CMAKE_FIND_LIBRARY_SUFFIXES ${CMAKE_STATIC_LIBRARY_SUFFIX} ${CMAKE_FIND_LIBRARY_SUFFIXES})

# Threads
find_package(Threads REQUIRED)

# Apache Thrift
include(ThriftExternal)

# AWS SDK
include(AwsSDKExternal)

# Jemalloc
include(JemallocExternal)

# If testing is enabled
if (BUILD_TESTS)
	# Catch2
	include(CatchExternal)
endif()

# Documentation tools
if (BUILD_DOC)
    find_package(MkDocs REQUIRED)
    find_package(Doxygen ${DOXYGEN_VERSION} REQUIRED)
endif ()
