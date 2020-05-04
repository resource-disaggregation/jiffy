# Zlib external project
# target:
#  - zlib_ep
# defines:
#  - ZLIB_HOME
#  - ZLIB_INCLUDE_DIR
#  - ZLIB_LIBRARY

set(ZLIB_VERSION "1.2.11")
set(ZLIB_BUILD ON)

if (DEFINED ENV{ZLIB_ROOT} AND EXISTS $ENV{ZLIB_ROOT})
  set(ZLIB_ROOT "$ENV{ZLIB_ROOT}")
  set(USE_SYSTEM_ZLIB)
endif()

if (USE_SYSTEM_ZLIB)
  find_package(ZLIB ${ZLIB_VERSION})
  if (ZLIB_FOUND)
    set(ZLIB_BUILD OFF)
    set(ZLIB_INCLUDE_DIR ${ZLIB_INCLUDE_DIRS})
    set(ZLIB_LIBRARY ${ZLIB_LIBRARIES})
    get_filename_component(ZLIB_HOME ${ZLIB_INCLUDE_DIR} DIRECTORY)
    add_custom_target(zlib_ep)
  else ()
    message(STATUS "Could not use system zlib, will download and build")
  endif (ZLIB_FOUND)
endif ()

if (ZLIB_BUILD)
  set(ZLIB_PREFIX "${PROJECT_BINARY_DIR}/external/zlib_ep")
  set(ZLIB_HOME "${ZLIB_PREFIX}")
  set(ZLIB_INCLUDE_DIR "${ZLIB_PREFIX}/include")
  set(ZLIB_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${ZLIB_PREFIX}"
          "-DBUILD_SHARED_LIBS=OFF")

  set(ZLIB_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}z")
  set(ZLIB_LIBRARY "${ZLIB_PREFIX}/lib/${ZLIB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(zlib_ep
          URL http://zlib.net/zlib-${ZLIB_VERSION}.tar.gz
          CMAKE_ARGS ${ZLIB_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
message(STATUS "ZLib include dir: ${ZLIB_INCLUDE_DIR}")
message(STATUS "ZLib static library: ${ZLIB_LIBRARY}")
