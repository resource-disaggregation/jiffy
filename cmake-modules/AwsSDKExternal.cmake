# Aws SDK external project
# target:
#  - awssdk_ep
# defines:
#  - AWSSDK_INCLUDE_DIR
#  - AWSSDK_LINK_LIBRARIES

set(AWSSDK_VERSION "1.7.276")
set(AWSSDK_MODULES "s3" "core")
set(AWSSDK_BUILD ON)

if ((DEFINED ENV{AWSDK_ROOT} AND EXISTS $ENV{AWSDK_ROOT}))
  set(AWSDK_ROOT_DIR "$ENV{AWSDK_ROOT}")
  set(USE_SYSTEM_AWSSDK ON)
endif ()

if (USE_SYSTEM_AWSSDK)
  CheckHasModule(AWSSDK)
  if (HAS_MODULE_AWSSDK)
    find_package(AWSSDK REQUIRED COMPONENTS "${AWSSDK_MODULES}")
  endif ()
  if (AWSSDK_FOUND)
    set(AWSSDK_BUILD OFF)
    add_custom_target(awssdk_ep)
  else(AWSSDK_FOUND)
    message(STATUS "${Red}Could not use system aws-sdk-cpp, will download and build${ColorReset}")
  endif(AWSSDK_FOUND)
endif()

if (AWSSDK_BUILD)
  message(STATUS "${Yellow}[Assessing aws-sdk-cpp dependencies]")

  # OpenSSL
  include(OpenSSLExternal)

  # Curl
  include(CurlExternal)

  # Zlib
  include(ZlibExternal)

  message(STATUS "[Finished assessing aws-sdk-cpp dependencies]${ColorReset}")

  set(AWSSDK_PREFIX "${CMAKE_CURRENT_BINARY_DIR}/external/awssdk_ep")
  set(AWSSDK_HOME "${AWSSDK_PREFIX}")
  set(AWSSDK_INCLUDE_DIR "${AWSSDK_PREFIX}/include")

  foreach (MODULE ${AWSSDK_MODULES})
    message(STATUS "AWS SDK Module to build: ${MODULE}")
    string(TOUPPER ${MODULE} MODULE_UPPERCASE)
    set(AWSSDK_${MODULE_UPPERCASE}_LIBRARY "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-cpp-sdk-${MODULE}${CMAKE_STATIC_LIBRARY_SUFFIX}")
    add_library(aws-sdk-cpp-${MODULE} STATIC IMPORTED)
    set_target_properties(aws-sdk-cpp-${MODULE} PROPERTIES IMPORTED_LOCATION ${AWSSDK_${MODULE_UPPERCASE}_LIBRARY})
    list(APPEND AWSSDK_LINK_LIBRARIES "aws-sdk-cpp-${MODULE}")
  endforeach ()
  add_library(curl STATIC IMPORTED)
  set_target_properties(curl PROPERTIES IMPORTED_LOCATION ${CURL_LIBRARY})
  add_library(zlib STATIC IMPORTED)
  set_target_properties(zlib PROPERTIES IMPORTED_LOCATION ${ZLIB_LIBRARY})
  add_library(openssl-ssl STATIC IMPORTED)
  set_target_properties(openssl-ssl PROPERTIES IMPORTED_LOCATION ${OPENSSL_SSL_LIBRARY})
  add_library(openssl-crypto STATIC IMPORTED)
  set_target_properties(openssl-crypto PROPERTIES IMPORTED_LOCATION ${OPENSSL_CRYPTO_LIBRARY})
  add_library(aws-c-common STATIC IMPORTED)
  set_target_properties(aws-c-common PROPERTIES IMPORTED_LOCATION "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-c-common${CMAKE_STATIC_LIBRARY_SUFFIX}")
  add_library(aws-c-event-stream STATIC IMPORTED)
  set_target_properties(aws-c-event-stream PROPERTIES IMPORTED_LOCATION "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-c-event-stream${CMAKE_STATIC_LIBRARY_SUFFIX}")
  add_library(aws-checksums STATIC IMPORTED)
  set_target_properties(aws-checksums PROPERTIES IMPORTED_LOCATION "${AWSSDK_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}aws-checksums${CMAKE_STATIC_LIBRARY_SUFFIX}")
  list(APPEND AWSSDK_LINK_LIBRARIES curl zlib openssl-ssl openssl-crypto aws-c-event-stream aws-checksums aws-c-common)
  if(APPLE)
    find_library(CoreFoundation CoreFoundation)
    list(APPEND AWSSDK_LINK_LIBRARIES ${CoreFoundation})
  endif()

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
