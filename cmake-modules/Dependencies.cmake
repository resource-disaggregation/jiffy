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
set(BOOST_COMPONENTS "program_options")
if (USE_SYSTEM_BOOST)
  set(Boost_USE_STATIC_LIBS ON)
  find_package(Boost ${BOOST_VERSION} COMPONENTS program_options REQUIRED)
  set(BOOST_INCLUDE_DIRS ${Boost_INCLUDE_DIRS})
  set(BOOST_LIBRARIES ${Boost_LIBRARIES})
else ()
  foreach (component ${BOOST_COMPONENTS})
    list(APPEND BOOST_COMPONENTS_FOR_BUILD --with-${component})
  endforeach ()

  string(REGEX REPLACE "\\." "_" BOOST_VERSION_STR ${BOOST_VERSION})
  set(BOOST_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(BOOST_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(BOOST_PREFIX "${PROJECT_BINARY_DIR}/external/boost")
  set(BOOST_INCLUDE_DIRS "${BOOST_PREFIX}/include")
  ExternalProject_Add(boost_ep
          URL https://downloads.sourceforge.net/project/boost/boost/${BOOST_VERSION}/boost_${BOOST_VERSION_STR}.zip
          UPDATE_COMMAND ""
          CONFIGURE_COMMAND ./bootstrap.sh --prefix=${BOOST_PREFIX}
          BUILD_COMMAND ./b2 link=static --prefix=${BOOST_PREFIX} ${BOOST_COMPONENTS_FOR_BUILD} install
          BUILD_IN_SOURCE true
          INSTALL_COMMAND ""
          INSTALL_DIR ${BOOST_PREFIX}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  macro(libraries_to_fullpath out)
    set(${out})
    foreach (comp ${BOOST_COMPONENTS})
      list(APPEND ${out} ${BOOST_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}boost_${comp}${CMAKE_STATIC_LIBRARY_SUFFIX})
    endforeach ()
  endmacro()
  libraries_to_fullpath(BOOST_LIBRARIES)
  set(BOOST_DEPENDENCY boost_ep)
endif ()
include_directories(SYSTEM ${BOOST_INCLUDE_DIRS})
message(STATUS "Boost include dirs: ${BOOST_INCLUDE_DIRS}")
message(STATUS "Boost static libraries: ${BOOST_LIBRARIES}")

if ((NOT USE_SYSTEM_AWSSDK_SDK) OR (NOT USE_SYSTEM_THRIFT))
  set(ZLIB_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(ZLIB_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(ZLIB_PREFIX "${PROJECT_BINARY_DIR}/external/zlib")
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

  include_directories(SYSTEM ${ZLIB_INCLUDE_DIR})
  message(STATUS "ZLib include dir: ${ZLIB_INCLUDE_DIR}")
  message(STATUS "ZLib static library: ${ZLIB_LIBRARY}")

  set(OPENSSL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(OPENSSL_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(OPENSSL_PREFIX "${PROJECT_BINARY_DIR}/external/openssl")
  set(OPENSSL_INCLUDE_DIR "${OPENSSL_PREFIX}/include")
  set(OPENSSL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}ssl")
  set(CRYPTO_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}crypto")
  set(OPENSSL_LIBRARIES "${OPENSSL_PREFIX}/lib/${OPENSSL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${OPENSSL_PREFIX}/lib/${CRYPTO_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(openssl_ep
          DEPENDS zlib_ep
          URL https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz
          BUILD_IN_SOURCE 1
          CONFIGURE_COMMAND ./config --prefix=${OPENSSL_PREFIX} --with-zlib-include=${ZLIB_INCLUDE_DIR} --with-zlib-lib=${ZLIB_PREFIX}/lib no-shared no-tests CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER} CFLAGS=${OPENSSL_C_FLAGS} CXXFLAGS=${OPENSSL_CXX_FLAGS}
          BUILD_COMMAND "$(MAKE)"
          INSTALL_COMMAND "$(MAKE)" install
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
  message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
  message(STATUS "OpenSSL static libraries: ${OPENSSL_LIBRARIES}")
endif ()

if (USE_SYSTEM_AWSSDK_SDK)
  find_package(AWSSDK REQUIRED COMPONENTS s3)
else ()
  set(CURL_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(CURL_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(CURL_PREFIX "${PROJECT_BINARY_DIR}/external/curl")
  set(CURL_HOME "${CURL_PREFIX}")
  set(CURL_INCLUDE_DIR "${CURL_PREFIX}/include")
  set(CURL_PREFIX_PATH "${ZLIB_PREFIX}|${OPENSSL_PREFIX}")
  set(CURL_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${CURL_PREFIX}"
          "-DCMAKE_PREFIX_PATH=${CURL_PREFIX_PATH}"
          "-DCURL_STATICLIB=ON"
          "-DBUILD_CURL_EXE=OFF"
          "-DBUILD_TESTING=OFF"
          "-DENABLE_MANUAL=OFF"
          "-DHTTP_ONLY=ON"
          "-DCURL_CA_PATH=none"
          "-DZLIB_LIBRARY=${ZLIB_LIBRARY}") # Force usage of static library

  set(CURL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}curl")
  set(CURL_LIBRARY "${CURL_PREFIX}/lib/${CURL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  string(REGEX REPLACE "\\." "_" CURL_VERSION_STR ${CURL_VERSION})
  ExternalProject_Add(curl_ep
          DEPENDS zlib_ep openssl_ep
          URL https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_STR}/curl-${CURL_VERSION}.tar.gz
          LIST_SEPARATOR |
          CMAKE_ARGS ${CURL_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  include_directories(SYSTEM ${CURL_INCLUDE_DIR})
  message(STATUS "Curl include dir: ${CURL_INCLUDE_DIR}")
  message(STATUS "Curl static library: ${CURL_LIBRARY}")

  install(FILES ${CURL_LIBRARY} DESTINATION lib)
  install(DIRECTORY ${CURL_INCLUDE_DIR}/curl DESTINATION include)

  set(AWSSDK_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(AWSSDK_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(AWSSDK_PREFIX "${PROJECT_BINARY_DIR}/external/aws")
  set(AWSSDK_HOME "${AWSSDK_PREFIX}")
  set(AWSSDK_BUILD_PROJECTS "s3")
  set(AWSSDK_INCLUDE_DIR "${AWSSDK_PREFIX}/include")
  set(AWSSDK_PREFIX_PATH "${ZLIB_PREFIX}|${OPENSSL_PREFIX}|${CURL_PREFIX}")
  set(AWSSDK_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DBUILD_ONLY=${AWSSDK_BUILD_PROJECTS}"
          "-DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}"
          "-DENABLE_TESTING=OFF"
          "-DBUILD_SHARED_LIBS=OFF"
          "-DCMAKE_PREFIX_PATH=${AWSSDK_PREFIX_PATH}"
          "-DZLIB_LIBRARY=${ZLIB_LIBRARY}") # Force usage of static library

  set(AWSSDK_STATIC_CORE_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-core")
  set(AWSSDK_STATIC_S3_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-s3")
  set(AWSSDK_LINK_LIBRARIES "${AWSSDK_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${AWSSDK_STATIC_S3_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${AWSSDK_PREFIX}/${CMAKE_INSTALL_LIBDIR}/${AWSSDK_STATIC_CORE_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(AWSSDK_LINK_LIBRARIES ${AWSSDK_LINK_LIBRARIES} ${CURL_LIBRARY} ${OPENSSL_LIBRARIES} ${ZLIB_LIBRARY})

  ExternalProject_Add(awssdk_ep
          DEPENDS curl_ep openssl_ep zlib_ep
          GIT_REPOSITORY "https://github.com/awslabs/aws-sdk-cpp.git"
          GIT_TAG "${AWSSDK_VERSION}"
          GIT_SHALLOW 1
          BUILD_IN_SOURCE true
          LIST_SEPARATOR |
          CMAKE_ARGS ${AWSSDK_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
  set(AWSSDK_DEPENDENCY "awssdk_ep")
endif ()

include_directories(SYSTEM ${AWSSDK_INCLUDE_DIR})
message(STATUS "AWS include dir: ${AWSSDK_INCLUDE_DIR}")
message(STATUS "AWS static libraries: ${AWSSDK_LINK_LIBRARIES}")

# Thrift
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
  set(LIBEVENT_LIBRARIES "${LIBEVENT_PREFIX}/lib/${LIBEVENT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(LIBEVENT_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
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
  include_directories(SYSTEM ${LIBEVENT_INCLUDE_DIR})
  message(STATUS "Libevent include dir: ${LIBEVENT_INCLUDE_DIR}")
  message(STATUS "Libevent static libraries: ${LIBEVENT_LIBRARIES}")
  set(LIBEVENT_DEPENDENCY "libevent_ep")

  set(THRIFT_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(THRIFT_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(THRIFT_PREFIX "${PROJECT_BINARY_DIR}/external/thrift")
  set(THRIFT_HOME "${THRIFT_PREFIX}")
  set(THRIFT_INCLUDE_DIR "${THRIFT_PREFIX}/include")
  if (USE_SYSTEM_BOOST)
    set(THRIFT_PREFIX_PATH "${LIBEVENT_PREFIX}|${ZLIB_PREFIX}|${OPENSSL_PREFIX}")
  else ()
    set(THRIFT_PREFIX_PATH "${LIBEVENT_PREFIX}|${ZLIB_PREFIX}|${OPENSSL_PREFIX}|${BOOST_PREFIX}")
  endif ()
  set(THRIFT_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
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
          "-DWITH_LIBEVENT=ON"
          "-DWITH_JAVA=OFF"
          "-DWITH_PYTHON=OFF"
          "-DWITH_CPP=ON"
          "-DWITH_STDTHREADS=OFF"
          "-DWITH_BOOSTTHREADS=OFF"
          "-DWITH_STATIC_LIB=ON"
          "-DCMAKE_PREFIX_PATH=${THRIFT_PREFIX_PATH}"
          "-DZLIB_LIBRARY=${ZLIB_LIBRARY}") # Force usage of static library

  set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
  endif ()
  set(THRIFT_LIBRARIES "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(THRIFTNB_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thriftnb")
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(THRIFTNB_STATIC_LIB_NAME "${THRIFTNB_STATIC_LIB_NAME}d")
  endif ()
  set(THRIFTNB_LIBRARIES "${THRIFT_PREFIX}/lib/${THRIFTNB_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}"
          "${LIBEVENT_LIBRARIES}")
  if (GENERATE_THRIFT)
    set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  endif ()
  ExternalProject_Add(thrift_ep
          DEPENDS libevent_ep openssl_ep zlib_ep
          URL "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
          LIST_SEPARATOR |
          CMAKE_ARGS ${THRIFT_CMAKE_ARGS}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
  set(THRIFT_DEPENDENCY "thrift_ep")
endif ()
set(THRIFT_LIBRARIES ${THRIFT_LIBRARIES} ${THRIFTNB_LIBRARIES})
include_directories(SYSTEM ${THRIFT_INCLUDE_DIR})
message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
message(STATUS "Thrift static libraries: ${THRIFT_LIBRARIES}")

if (GENERATE_THRIFT)
  message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
  add_executable(thriftcompiler IMPORTED GLOBAL)
  set_target_properties(thriftcompiler PROPERTIES IMPORTED_LOCATION ${THRIFT_COMPILER})
  add_dependencies(thriftcompiler thrift)
endif ()

if (USE_SYSTEM_JEMALLOC)
  find_package(Jemalloc REQUIRED)
else ()
  set(JEMALLOC_CXX_FLAGS "${EXTERNAL_CXX_FLAGS}")
  set(JEMALLOC_C_FLAGS "${EXTERNAL_C_FLAGS}")
  set(JEMALLOC_LD_FLAGS "-Wl,--no-as-needed")
  set(JEMALLOC_PREFIX "${PROJECT_BINARY_DIR}/external/jemalloc")
  set(JEMALLOC_HOME "${JEMALLOC_PREFIX}")
  set(JEMALLOC_INCLUDE_DIR "${JEMALLOC_PREFIX}/include")
  set(JEMALLOC_FUNCTION_PREFIX "")
  set(JEMALLOC_CMAKE_ARGS "-Wno-dev"
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_CXX_FLAGS=${JEMALLOC_CXX_FLAGS}"
          "-DCMAKE_INSTALL_PREFIX=${JEMALLOC_PREFIX}")
  set(JEMALLOC_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}jemalloc")
  set(JEMALLOC_LIBRARIES "${JEMALLOC_PREFIX}/lib/${JEMALLOC_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  ExternalProject_Add(jemalloc_ep
          URL https://github.com/jemalloc/jemalloc/releases/download/5.0.1/jemalloc-5.0.1.tar.bz2
          PREFIX ${JEMALLOC_PREFIX}
          BUILD_BYPRODUCTS ${JEMALLOC_LIBRARIES}
          CONFIGURE_COMMAND ${JEMALLOC_PREFIX}/src/jemalloc/configure --with-jemalloc-prefix=${JEMALLOC_FUNCTION_PREFIX} --prefix=${JEMALLOC_PREFIX} --enable-autogen --enable-prof-libunwind CFLAGS=${JEMALLOC_C_FLAGS} CXXFLAGS=${JEMALLOC_CXX_FLAGS}
          INSTALL_COMMAND make install_include install_lib
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
  set(JEMALLOC_DEPENDENCY "jemalloc_ep")
endif ()
include_directories(SYSTEM ${JEMALLOC_INCLUDE_DIR})
message(STATUS "Jemalloc include dir: ${JEMALLOC_INCLUDE_DIR}")
message(STATUS "Jemalloc library: ${JEMALLOC_LIBRARIES}")

# Catch2 Test framework
if (BUILD_TESTS)
  ExternalProject_Add(catch_ep
          PREFIX ${CMAKE_BINARY_DIR}/catch
          URL https://github.com/catchorg/Catch2/archive/v${CATCH_VERSION}.tar.gz
          CONFIGURE_COMMAND ""
          BUILD_COMMAND ""
          INSTALL_COMMAND ""
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  ExternalProject_Get_Property(catch_ep source_dir)
  set(CATCH_INCLUDE_DIR ${source_dir}/single_include CACHE INTERNAL "Path to include folder for Catch")
  include_directories(SYSTEM ${CATCH_INCLUDE_DIR})
  set(CATCH_DEPENDENCY "catch_ep")
endif ()

if (BUILD_DOC)
  find_package(MkDocs REQUIRED)
  find_package(Doxygen ${DOXYGEN_VERSION} REQUIRED)
endif ()
