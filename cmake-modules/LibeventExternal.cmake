# Libevent external project
# target:
#  - libevent_ep
# defines:
#  - LIBEVENT_HOME
#  - LIBEVENT_INCLUDE_DIR
#  - LIBEVENT_LIBRARY

set(LIBEVENT_VERSION "2.1.8")
set(LIBEVENT_BUILD ON)

if (DEFINED ENV{LIBEVENT_ROOT} AND EXISTS $ENV{LIBEVENT_ROOT})
  set(LIBEVENT_ROOT "$ENV{LIBEVENT_ROOT}")
  set(USE_SYSTEM_LIBEVENT ON)
endif ()

if (USE_SYSTEM_LIBEVENT)
  find_package(Libevent ${LIBEVENT_VERSION})
  if (LIBEVENT_FOUND)
    set(LIBEVENT_BUILD OFF)
    set(LIBEVENT_INCLUDE_DIR ${LIBEVENT_INCLUDE_DIRS})
    set(LIBEVENT_LIBRARY ${LIBEVENT_LIBRARIES})
    get_filename_component(LIBEVENT_HOME ${LIBEVENT_INCLUDE_DIR} DIRECTORY)
    add_custom_target(libevent_ep)
  else (LIBEVENT_FOUND)
    message(STATUS "${Red}Could not use system libevent, will download and build${ColorReset}")
  endif (LIBEVENT_FOUND)
endif ()

if (LIBEVENT_BUILD)
  set(LIBEVENT_PREFIX "${PROJECT_BINARY_DIR}/external/libevent_ep")
  set(LIBEVENT_HOME "${LIBEVENT_PREFIX}")
  set(LIBEVENT_INCLUDE_DIR "${LIBEVENT_PREFIX}/include")
  set(LIBEVENT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}event")
  set(LIBEVENT_CORE_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}event_core")
  set(LIBEVENT_EXTRA_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}event_extra")
  set(LIBEVENT_LIBRARY "${LIBEVENT_PREFIX}/lib/${LIBEVENT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(LIBEVENT_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_INSTALL_PREFIX=${LIBEVENT_PREFIX}"
          "-DENABLE_TESTING=OFF"
          "-DBUILD_SHARED_LIBS=OFF"
          "-DEVENT__DISABLE_OPENSSL=ON"
          "-DEVENT__DISABLE_BENCHMARK=ON"
          "-DEVENT__DISABLE_TESTS=ON")
  ExternalProject_Add(libevent_ep
          URL https://github.com/nmathewson/Libevent/archive/release-${LIBEVENT_VERSION}-stable.tar.gz
          CMAKE_ARGS ${LIBEVENT_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${LIBEVENT_INCLUDE_DIR})
message(STATUS "Libevent include dir: ${LIBEVENT_INCLUDE_DIR}")
message(STATUS "Libevent static library: ${LIBEVENT_LIBRARY}")
