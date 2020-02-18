# OpenSSL external project
# target:
#  - openssl_ep
# defines:
#  - OPENSSL_INCLUDE_DIR
#  - OPENSSL_CRYPTO_LIBRARY
#  - OPENSSL_SSL_LIBRARY
#  - OPENSSL_LIBRARIES

set(OPENSSL_VERSION "1.1.1-pre7")
set(OPENSSL_BUILD ON)

if (DEFINED ENV{OPENSSL_ROOT} AND EXISTS $ENV{OPENSSL_ROOT})
  set(OPENSSL_ROOT_DIR "$ENV{OPENSSL_ROOT}")
  set(USE_SYSTEM_OPENSSL ON)
endif ()

if (USE_SYSTEM_OPENSSL)
  set(OPENSSL_USE_STATIC_LIBS TRUE)
  find_package(OpenSSL)
  if (OPENSSL_FOUND)
    set(OPENSSL_BUILD OFF)
    get_filename_component(OPENSSL_HOME ${OPENSSL_INCLUDE_DIR} DIRECTORY)
    add_custom_target(openssl_ep)
  else (OPENSSL_FOUND)
    message(STATUS "${Red}Could not use system OpenSSL, will download and build${ColorReset}")
  endif (OPENSSL_FOUND)
endif ()

if (OPENSSL_BUILD)
  set(OPENSSL_PREFIX "${PROJECT_BINARY_DIR}/external/openssl_ep")
  set(OPENSSL_HOME ${OPENSSL_PREFIX})
  set(OPENSSL_INCLUDE_DIR "${OPENSSL_PREFIX}/include")
  set(OPENSSL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}ssl")
  set(CRYPTO_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}crypto")
  set(OPENSSL_CRYPTO_LIBRARY "${OPENSSL_PREFIX}/lib/${CRYPTO_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(OPENSSL_SSL_LIBRARY "${OPENSSL_PREFIX}/lib/${OPENSSL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  set(OPENSSL_LIBRARIES "${OPENSSL_CRYPTO_LIBRARY}" "${OPENSSL_SSL_LIBRARY}")
  ExternalProject_Add(openssl_ep
          URL https://www.openssl.org/source/openssl-${OPENSSL_VERSION}.tar.gz
          BUILD_IN_SOURCE 1
          CONFIGURE_COMMAND ./config --prefix=${OPENSSL_PREFIX} no-comp no-shared no-tests CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER}
          BUILD_COMMAND "$(MAKE)"
          INSTALL_COMMAND "$(MAKE)" install
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${OPENSSL_INCLUDE_DIR})
message(STATUS "OpenSSL include dir: ${OPENSSL_INCLUDE_DIR}")
message(STATUS "OpenSSL static libraries: ${OPENSSL_LIBRARIES}")