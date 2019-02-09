# Thrift external project
# target:
#  - thrift_ep
# defines:
#  - THRIFT_HOME
#  - THRIFT_INCLUDE_DIR
#  - THRIFT_LIBRARY

set(THRIFT_VERSION "0.11.0")
set(THRIFT_BUILD ON)

if (DEFINED ENV{THRIFT_ROOT} AND EXISTS $ENV{THRIFT_ROOT})
  set(THRIFT_ROOT "$ENV{THRIFT_ROOT}")
  find_package(THRIFT ${THRIFT_VERSION})
  if (THRIFT_FOUND)
    set(THRIFT_BUILD OFF)
    add_custom_target(thrift_ep)
  endif (THRIFT_FOUND)
endif ()

if (THRIFT_BUILD)
  set(THRIFT_PREFIX "${PROJECT_BINARY_DIR}/external/thrift_ep")
  set(THRIFT_HOME "${THRIFT_PREFIX}")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  set(THRIFT_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
          "-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
          "-DBUILD_COMPILER=ON"
          "-DBUILD_TESTING=OFF"
          "-DWITH_SHARED_LIB=OFF"
          "-DWITH_QT4=OFF"
          "-DWITH_QT5=OFF"
          "-DWITH_C_GLIB=OFF"
          "-DWITH_HASKELL=OFF"
          "-DWITH_LIBEVENT=OFF"
          "-DWITH_ZLIB=OFF"
          "-DWITH_JAVA=OFF"
          "-DWITH_PYTHON=OFF"
          "-DWITH_CPP=ON"
          "-DWITH_STDTHREADS=OFF"
          "-DWITH_BOOSTTHREADS=OFF"
          "-DWITH_STATIC_LIB=ON"
          "-DCMAKE_PREFIX_PATH=${OPENSSL_PREFIX}")

  set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
  endif ()
  set(THRIFT_LIBRARY "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  ExternalProject_Add(thrift_ep
          URL "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
          DEPENDS openssl_ep
          CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
message(STATUS "ZLib include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "ZLib static library: ${THRIFT_LIBRARY}")

if (GENERATE_THRIFT)
  message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
  add_executable(thriftcompiler IMPORTED GLOBAL)
  set_target_properties(thriftcompiler PROPERTIES IMPORTED_LOCATION ${THRIFT_COMPILER})
  add_dependencies(thriftcompiler thrift)
endif ()