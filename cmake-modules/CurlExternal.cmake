# Curl external project
# target:
#  - curl_ep
# defines:
#  - CURL_HOME
#  - CURL_INCLUDE_DIR
#  - CURL_LIBRARY

set(CURL_VERSION "7.60.0")
set(CURL_BUILD ON)

if (DEFINED ENV{CURL_ROOT} AND EXISTS $ENV{CURL_ROOT})
  set(CURL_ROOT "$ENV{CURL_ROOT}")
  set(USE_SYSTEM_CURL ON)
endif()

if (USE_SYSTEM_CURL)
  find_package(CURL ${CURL_VERSION})
  if (CURL_FOUND)
    set(CURL_BUILD OFF)
    set(CURL_INCLUDE_DIR ${CURL_INCLUDE_DIRS})
    set(CURL_LIBRARY ${CURL_LIBRARIES})
    get_filename_component(CURL_HOME ${CURL_INCLUDE_DIR} DIRECTORY)
    add_custom_target(curl_ep)
  else (CURL_FOUND)
    message(STATUS "${Red}Could not use system curl, will download and build${ColorReset}")
  endif (CURL_FOUND)
endif ()

if (CURL_BUILD)
  set(CURL_PREFIX "${PROJECT_BINARY_DIR}/external/curl_ep")
  set(CURL_HOME "${CURL_PREFIX}")
  set(CURL_INCLUDE_DIR "${CURL_PREFIX}/include")
  set(CURL_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}curl")
  set(CURL_LIBRARY "${CURL_PREFIX}/lib/${CURL_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  string(REGEX REPLACE "\\." "_" CURL_VERSION_UNDERSCORE ${CURL_VERSION})
  ExternalProject_Add(curl_ep
          URL https://github.com/curl/curl/releases/download/curl-${CURL_VERSION_UNDERSCORE}/curl-${CURL_VERSION}.tar.gz
          BUILD_IN_SOURCE 1
          CONFIGURE_COMMAND ./configure --prefix=${CURL_PREFIX} --with-ssl --without-libidn2 --without-zlib --disable-ldap --enable-shared=no --enable-static=yes CXX=${CMAKE_CXX_COMPILER} CC=${CMAKE_C_COMPILER}
          BUILD_COMMAND "$(MAKE)"
          INSTALL_COMMAND "$(MAKE)" install
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)
endif ()

include_directories(SYSTEM ${CURL_INCLUDE_DIR})
message(STATUS "Curl include dir: ${CURL_INCLUDE_DIR}")
message(STATUS "Curl static library: ${CURL_LIBRARY}")
