include(ExternalProject)

set(THRIFT_VERSION "0.11.0")
set(BOOST_VERSION "1.40.0")

find_package(Threads REQUIRED)
find_package(Boost ${BOOST_VERSION} COMPONENTS program_options REQUIRED)

include(FindPythonInterp)
if (NOT PYTHONINTERP_FOUND)
  message(FATAL_ERROR "Cannot build python client without python interpretor")
endif ()
find_python_module(setuptools REQUIRED)
if (NOT PY_SETUPTOOLS)
  message(FATAL_ERROR "Python setuptools is required for python client")
endif ()

set(EXTERNAL_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EXTERNAL_C_FLAGS "${CMAKE_C_FLAGS} -fPIC ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (USE_SYSTEM_AWS_SDK)
  find_package(aws-sdk-cpp REQUIRED)
else ()
  set(CURL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(CURL_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(CURL_PREFIX "${PROJECT_BINARY_DIR}/external/curl")
  set(CURL_HOME "${CURL_PREFIX}")
  set(CURL_INCLUDE_DIR "${CURL_PREFIX}/include")
  set(CURL_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${CURL_PREFIX}"
          "-DCURL_STATICLIB=ON"
          "-DBUILD_CURL_EXE=OFF"
          "-DCMAKE_USE_OPENSSL=OFF"
          "-DHTTP_ONLY=ON"
          "-DCURL_CA_PATH=none")

  set(CURL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}curl")
  set(CURL_LIBRARY "${CURL_PREFIX}/lib/${CURL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  ExternalProject_Add(curl
          URL https://github.com/curl/curl/releases/download/curl-7_60_0/curl-7.60.0.tar.gz
          CMAKE_ARGS ${CURL_CMAKE_ARGS}
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${CURL_INCLUDE_DIR})
  message(STATUS "Curl include dir: ${CURL_INCLUDE_DIR}")
  message(STATUS "Curl static library: ${CURL_LIBRARY}")

  install(FILES ${CURL_LIBRARY} DESTINATION lib)
  install(DIRECTORY ${CURL_INCLUDE_DIR}/curl DESTINATION include)

  set(AWS_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(AWS_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(AWS_PREFIX "${PROJECT_BINARY_DIR}/external/aws")
  set(AWS_HOME "${AWS_PREFIX}")
  set(AWS_BUILD_PROJECTS "s3")
  set(AWS_INCLUDE_DIR "${AWS_PREFIX}/include")
  set(AWS_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DBUILD_ONLY=${AWS_BUILD_PROJECTS}"
          "-DCMAKE_INSTALL_PREFIX=${AWS_PREFIX}"
          "-DENABLE_TESTING=OFF"
          "-DBUILD_SHARED_LIBS=OFF"
          "-DCMAKE_PREFIX_PATH=${CURL_PREFIX}")

  set(AWS_STATIC_CORE_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-core")
  set(AWS_STATIC_S3_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-s3")
  set(AWS_LIBRARIES "${AWS_PREFIX}/lib/${AWS_STATIC_CORE_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${AWS_PREFIX}/lib/${AWS_STATIC_S3_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")

  ExternalProject_Add(awssdk
          URL https://github.com/aws/aws-sdk-cpp/archive/1.4.26.tar.gz
          CMAKE_ARGS ${AWS_CMAKE_ARGS}
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${AWS_INCLUDE_DIR})
  message(STATUS "AWS include dir: ${AWS_INCLUDE_DIR}")
  message(STATUS "AWS static libraries: ${AWS_LIBRARIES}")

  install(FILES ${AWS_LIBRARIES} DESTINATION lib)
  install(DIRECTORY ${AWS_INCLUDE_DIR}/aws DESTINATION include)

  add_dependencies(awssdk curl)
endif ()

if (USE_SYSTEM_THRIFT)
  find_package(Thrift ${THRIFT_VERSION} REQUIRED)
else ()
  set(LIBEVENT_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(LIBEVENT_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(LIBEVENT_PREFIX "${PROJECT_BINARY_DIR}/external/libevent")
  set(LIBEVENT_HOME "${LIBEVENT_PREFIX}")
  set(LIBEVENT_INCLUDE_DIR "${LIBEVENT_PREFIX}/include")
  set(LIBEVENT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}event")
  set(LIBEVENT_CORE_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}event_core")
  set(LIBEVENT_EXTRA_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}event_extra")
  set(LIBEVENT_LIBRARIES "${LIBEVENT_PREFIX}/lib/${LIBEVENT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${LIBEVENT_PREFIX}/lib/${LIBEVENT_CORE_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${LIBEVENT_PREFIX}/lib/${LIBEVENT_EXTRA_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(LIBEVENT_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${LIBEVENT_PREFIX}"
          "-DENABLE_TESTING=OFF"
          "-DBUILD_SHARED_LIBS=OFF"
          "-DEVENT__DISABLE_OPENSSL=ON")
  ExternalProject_Add(libevent
          URL https://github.com/nmathewson/Libevent/archive/release-2.1.8-stable.tar.gz
          CMAKE_ARGS ${LIBEVENT_CMAKE_ARGS}
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
  include_directories(SYSTEM ${LIBEVENT_INCLUDE_DIR})
  message(STATUS "Libevent include dir: ${LIBEVENT_INCLUDE_DIR}")
  message(STATUS "Libevent static libraries: ${LIBEVENT_LIBRARIES}")

  install(FILES ${LIBEVENT_LIBRARIES} DESTINATION lib)
  install(DIRECTORY ${LIBEVENT_INCLUDE_DIR} DESTINATION include)

  set(THRIFT_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(THRIFT_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(THRIFT_PREFIX "${PROJECT_BINARY_DIR}/external/thrift")
  set(THRIFT_HOME "${THRIFT_PREFIX}")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  set(THRIFT_CMAKE_ARGS "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_CXX_FLAGS=${THRIFT_CXX_FLAGS}"
          "-DCMAKE_C_FLAGS=${THRIFT_C_FLAGS}"
          "-DCMAKE_INSTALL_PREFIX=${THRIFT_PREFIX}"
          "-DCMAKE_INSTALL_RPATH=${THRIFT_PREFIX}/lib"
          "-DBUILD_COMPILER=${GENERATE_THRIFT}"
          "-DBUILD_TESTING=OFF"
          "-DWITH_SHARED_LIB=OFF"
          "-DWITH_QT4=OFF"
          "-DWITH_QT5=OFF"
          "-DWITH_C_GLIB=OFF"
          "-DWITH_HASKELL=OFF"
          "-DWITH_ZLIB=OFF" # For now
          "-DWITH_OPENSSL=OFF" # For now
          "-DWITH_LIBEVENT=ON"
          "-DWITH_JAVA=OFF"
          "-DWITH_PYTHON=OFF"
          "-DWITH_CPP=ON"
          "-DWITH_STDTHREADS=OFF"
          "-DWITH_BOOSTTHREADS=OFF"
          "-DWITH_STATIC_LIB=ON"
          "-DCMAKE_PREFIX_PATH=${LIBEVENT_PREFIX}")

  set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
  endif ()
  set(THRIFT_LIBRARIES "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(THRIFTNB_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thriftnb")
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(THRIFTNB_STATIC_LIB_NAME "${THRIFTNB_STATIC_LIB_NAME}d")
  endif ()
  set(THRIFTNB_LIBRARIES "${THRIFT_PREFIX}/lib/${THRIFTNB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  if (GENERATE_THRIFT)
    set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  endif ()
  ExternalProject_Add(thrift
          URL "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
          CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  add_dependencies(thrift libevent)

  include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
  message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
  message(STATUS "Thrift static libraries: ${THRIFT_LIBRARIES}")
  message(STATUS "Thrift non-blocking libraries: ${THRIFTNB_LIBRARIES}")

  if (GENERATE_THRIFT)
    message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
    add_executable(thriftcompiler IMPORTED GLOBAL)
    set_target_properties(thriftcompiler PROPERTIES IMPORTED_LOCATION ${THRIFT_COMPILER})
    add_dependencies(thriftcompiler thrift)
  endif ()

  install(FILES ${THRIFT_LIBRARIES} DESTINATION lib)
  install(DIRECTORY ${THRIFT_INCLUDE_DIR}/thrift DESTINATION include)
endif ()

if (NOT USE_SYSTEM_LIBCUCKOO)
  set(LIBCUCKOO_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(LIBCUCKOO_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(LIBCUCKOO_PREFIX "${PROJECT_BINARY_DIR}/external/libcuckoo")
  set(LIBCUCKOO_HOME "${LIBCUCKOO_PREFIX}")
  set(LIBCUCKOO_INCLUDE_DIR "${LIBCUCKOO_PREFIX}/include")
  set(LIBCUCKOO_CMAKE_ARGS "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_CXX_FLAGS=${LIBCUCKOO_CXX_FLAGS}"
          "-DCMAKE_INSTALL_PREFIX=${LIBCUCKOO_PREFIX}"
          "-DBUILD_EXAMPLES=OFF"
          "-DBUILD_TESTS=OFF"
          "-DBUILD_STRESS_TESTS=OFF"
          "-DBUILD_UNIVERSAL_BENCHMARK=OFF")

  ExternalProject_Add(libcuckoo
          URL "https://github.com/efficient/libcuckoo/archive/v0.2.tar.gz"
          CMAKE_ARGS ${LIBCUCKOO_CMAKE_ARGS}
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${LIBCUCKOO_INCLUDE_DIR})
  message(STATUS "libcuckoo include dir: ${LIBCUCKOO_INCLUDE_DIR}")

  install(DIRECTORY ${LIBCUCKOO_INCLUDE_DIR}/libcuckoo DESTINATION include)
endif ()

if (NOT USE_SYSTEM_JEMALLOC)
  set(JEMALLOC_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(JEMALLOC_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(JEMALLOC_LD_FLAGS "-Wl,--no-as-needed")
  set(JEMALLOC_PREFIX "${PROJECT_BINARY_DIR}/external/jemalloc")
  set(JEMALLOC_HOME "${JEMALLOC_PREFIX}")
  set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
  set(JEMALLOC_CMAKE_ARGS "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_CXX_FLAGS=${JEMALLOC_CXX_FLAGS}"
          "-DCMAKE_INSTALL_PREFIX=${JEMALLOC_PREFIX}")
  set(JEMALLOC_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc")
  set(JEMALLOC_LIBRARIES "${JEMALLOC_PREFIX}/lib/${JEMALLOC_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(jemalloc
          URL https://github.com/jemalloc/jemalloc/releases/download/5.0.1/jemalloc-5.0.1.tar.bz2
          PREFIX ${JEMALLOC_PREFIX}
          BUILD_BYPRODUCTS ${JEMALLOC_LIBRARIES}
          CONFIGURE_COMMAND ${JEMALLOC_PREFIX}/src/jemalloc/configure --prefix=${JEMALLOC_PREFIX} --enable-autogen --enable-prof-libunwind CFLAGS=${JEMALLOC_C_FLAGS} CXXFLAGS=${JEMALLOC_CXX_FLAGS}
          INSTALL_COMMAND make install_lib
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
  message(STATUS "Jemalloc library: ${JEMALLOC_LIBRARIES}")
  install(FILES ${JEMALLOC_LIBRARIES} DESTINATION lib)
endif ()

# Catch2 Test framework
if (BUILD_TESTS AND NOT USE_SYSTEM_CATCH)
  ExternalProject_Add(catch
          PREFIX ${CMAKE_BINARY_DIR}/catch
          URL https://github.com/catchorg/Catch2/archive/v2.2.1.tar.gz
          CONFIGURE_COMMAND ""
          BUILD_COMMAND ""
          INSTALL_COMMAND ""
          LOG_DOWNLOAD ON)

  ExternalProject_Get_Property(catch source_dir)
  set(CATCH_INCLUDE_DIR ${source_dir}/single_include CACHE INTERNAL "Path to include folder for Catch")
endif ()