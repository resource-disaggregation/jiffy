# Jemaloc external project
# target:
#  - zlib_ep
# defines:
#  - JEMALLOC_HOME
#  - JEMALLOC_INCLUDE_DIR
#  - JEMALLOC_LIBRARY

set(JEMALLOC_VERSION "5.0.1")
set(JEMALLOC_BUILD ON)

if (DEFINED ENV{JEMALLOC_ROOT} AND EXISTS $ENV{JEMALLOC_ROOT})
  set(JEMALLOC_ROOT_DIR "$ENV{JEMALLOC_ROOT}")
  set(USE_SYSTEM_JEMALLOC ON)
endif ()

if (USE_SYSTEM_JEMALLOC)
  find_package(Jemalloc ${JEMALLOC_VERSION})
  if (JEMALLOC_FOUND)
    set(JEMALLOC_BUILD OFF)
    get_filename_component(JEMALLOC_HOME ${JEMALLOC_INCLUDE_DIR} DIRECTORY)
    add_custom_target(jemalloc_ep)
  else (JEMALLOC_FOUND)
    message(STATUS "${Red}Could not use system Jemalloc, will download and build${ColorReset}")
  endif (JEMALLOC_FOUND)
endif ()

if (JEMALLOC_BUILD)
  set(JEMALLOC_LD_FLAGS "-Wl,--no-as-needed")
  set(JEMALLOC_PREFIX "${PROJECT_BINARY_DIR}/external/jemalloc_ep")
  set(JEMALLOC_HOME "${JEMALLOC_PREFIX}")
  set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
  set(JEMALLOC_FUNCTION_PREFIX "")
  set(JEMALLOC_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}")
  set(JEMALLOC_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc")
  set(JEMALLOC_LIBRARY "${JEMALLOC_PREFIX}/lib/${JEMALLOC_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(jemalloc_ep
          URL https://github.com/jemalloc/jemalloc/releases/download/${JEMALLOC_VERSION}/jemalloc-${JEMALLOC_VERSION}.tar.bz2
          BUILD_IN_SOURCE 1
          PREFIX ${JEMALLOC_PREFIX}
          CONFIGURE_COMMAND ./configure --with-jemalloc-prefix=${JEMALLOC_FUNCTION_PREFIX} --prefix=${JEMALLOC_PREFIX} --enable-autogen --enable-prof-libunwind CFLAGS=${JEMALLOC_C_FLAGS} CXXFLAGS=${JEMALLOC_CXX_FLAGS}
          INSTALL_COMMAND make install_include install_lib
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
message(STATUS "Jemalloc include dir: ${JEMALLOC_INCLUDE_DIR}")
message(STATUS "Jemalloc static library: ${JEMALLOC_LIBRARY}")
