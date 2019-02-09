# Aws SDK external project
# target:
#  - awssdk_ep
# defines:
#  - AWSSDK_INCLUDE_DIR
#  - AWSSDK_LINK_LIBRARIES

set(AWSSDK_VERSION "1.6.53")

set(AWSSDK_MODULES "s3" "core")

if (USE_SYSTEM_AWSSDK)
  find_package(AWSSDK REQUIRED COMPONENTS "${AWSSDK_MODULES}")
  add_custom_target(awssdk_ep)
else ()
  include(CurlExternal)
  include(ZlibExternal)

  set(AWSSDK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/awssdk_ep")
  set(AWSSDK_HOME "${AWSSDK_PREFIX}")
  set(AWSSDK_INCLUDE_DIR "${AWSSDK_PREFIX}/include")

  foreach (MODULE ${AWSSDK_MODULES})
    message(STATUS "AWS SDK Module to build: ${MODULE}")
    string(TOUPPER ${MODULE} MODULE_UPPERCASE)
    set(AWSSDK_${MODULE_UPPERCASE}_LIBRARY "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-${MODULE}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    list(APPEND AWSSDK_LINK_LIBRARIES ${AWSSDK_${MODULE_UPPERCASE}_LIBRARY})
  endforeach ()

  list(APPEND AWSSDK_LINK_LIBRARIES "${CURL_LIBRARY}"
          "${ZLIB_LIBRARY}"
          "${OPENSSL_SSL_LIBRARY}"
          "${OPENSSL_CRYPTO_LIBRARY}")

  set(AWSSDK_CMAKE_ARGS
          "-DCMAKE_CXX_COMPILER=${CMAKE_CXX_COMPILER}"
          "-DCMAKE_C_COMPILER=${CMAKE_C_COMPILER}"
          "-DCMAKE_BUILD_TYPE=${CMAKE_BUILD_TYPE}"
          "-DCMAKE_INSTALL_PREFIX=${AWSSDK_PREFIX}"
          "-DENABLE_TESTING=OFF"
          "-DMINIMIZE_SIZE=ON"
          "-DBUILD_SHARED_LIBS=OFF"
          "-DFORCE_CURL=ON"
          "-DZLIB_ROOT=${ZLIB_HOME}"
          "-DCMAKE_PREFIX_PATH=${CURL_HOME}|${OPENSSL_HOME}"
          "-DBUILD_ONLY=s3")

  ExternalProject_Add(awssdk_ep
          DEPENDS curl_ep zlib_ep openssl_ep
          URL "https://github.com/aws/aws-sdk-cpp/archive/${AWSSDK_VERSION}.tar.gz"
          BUILD_IN_SOURCE 1
          CMAKE_ARGS ${AWSSDK_CMAKE_ARGS}
          LIST_SEPARATOR |
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${AWSSDK_INCLUDE_DIR})
message(STATUS "AWSSDK include dir: ${AWSSDK_INCLUDE_DIR}")
message(STATUS "AWSSDK static libraries: ${AWSSDK_LINK_LIBRARIES}")
