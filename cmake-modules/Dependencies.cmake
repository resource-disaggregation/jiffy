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
if (BUILD_S3_SUPPORT)
  CheckHasModule(AWSSDK)
  if (HAS_MODULE_AWSSDK)
    find_package(AWSSDK REQUIRED COMPONENTS "${AWSSDK_MODULES}")
  endif ()
  if (NOT AWSSDK_FOUND)
    add_definitions(-DS3_EXTERNAL)
  else(AWSSDK_FOUND)
    message(FATAL_ERROR "Could not find aws-sdk-cpp, required for building support for S3 as external store.")
  endif(AWSSDK_FOUND)
endif ()

# Memkind
if (BUILD_MEMKIND_SUPPORT)
  # Numa
  find_package(Numa REQUIRED)
  include(MemkindExternal)
  add_definitions(-DMEMKIND_IN_USE)
  set(HEAP_MANAGER_EP "memkind_ep")
  set(HEAP_MANAGER_LIBRARY ${MEMKIND_LIBRARY} ${NUMA_LIBRARY})
else()
  # Jemalloc
  include(JemallocExternal)
  set(HEAP_MANAGER_EP "jemalloc_ep")
  set(HEAP_MANAGER_LIBRARY ${JEMALLOC_LIBRARY})
endif ()

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
