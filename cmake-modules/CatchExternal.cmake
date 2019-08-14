# Catch external project
# target:
#  - catch_ep
# defines:
#  - CATCH_INCLUDE_DIR

set(CATCH_VERSION "2.2.1")

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
set(CATCH_INCLUDE_DIR ${source_dir}/single_include)
include_directories(SYSTEM ${CATCH_INCLUDE_DIR})