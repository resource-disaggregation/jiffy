# Memkind external project
# target:
#  - zlib_ep
# defines:
#  - MEMKIND_HOME
#  - MEMKIND_INCLUDE_DIR
#  - MEMKIND_LIBRARY

set(MEMKIND_VERSION "1.10.1")
set(MEMKIND_BUILD ON)

if (DEFINED ENV{MEMKIND_ROOT} AND EXISTS $ENV{MEMKIND_ROOT})
  set(MEMKIND_ROOT_DIR "$ENV{MEMKIND_ROOT}")
  set(USE_SYSTEM_MEMKIND ON)
endif ()

if (USE_SYSTEM_MEMKIND)
  find_package(Memkind ${MEMKIND_VERSION})
  if (MEMKIND_FOUND)
    set(MEMKIND_BUILD OFF)
    get_filename_component(MEMKIND_HOME ${MEMKIND_INCLUDE_DIR} DIRECTORY)
    add_custom_target(memkind_ep)
  else (MEMKIND_FOUND)
    message(STATUS "${Red}Could not use system Memkind, will download and build${ColorReset}")
  endif (MEMKIND_FOUND)
endif ()

if (MEMKIND_BUILD)
  set(MEMKIND_LD_FLAGS "-Wl,--no-as-needed")
  set(MEMKIND_PREFIX "${PROJECT_BINARY_DIR}/external/memkind_ep")
  set(MEMKIND_HOME "${MEMKIND_PREFIX}")
  set(MEMKIND_INCLUDE_DIR "${MEMKIND_PREFIX}/include")
  set(MEMKIND_FUNCTION_PREFIX "")
  set(MEMKIND_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
  set(MEMKIND_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}memkind")
  set(MEMKIND_LIBRARY "${MEMKIND_PREFIX}/lib/${MEMKIND_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(memkind_ep
          URL https://github.com/memkind/memkind/archive/v${MEMKIND_VERSION}.tar.gz
          BUILD_IN_SOURCE 1
          PREFIX ${MEMKIND_PREFIX}
          CONFIGURE_COMMAND ./autogen.sh COMMAND ./configure --prefix=${MEMKIND_PREFIX} MEMKIND_PREFIX=${MEMKIND_FUNCTION_PREFIX} CFLAGS=${MEMKIND_C_FLAGS} CXXFLAGS=${MEMKIND_CXX_FLAGS}
          INSTALL_COMMAND make install
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${MEMKIND_INCLUDE_DIR})
message(STATUS "Memkind include dir: ${MEMKIND_INCLUDE_DIR}")
message(STATUS "Memkind static library: ${MEMKIND_LIBRARY}")
