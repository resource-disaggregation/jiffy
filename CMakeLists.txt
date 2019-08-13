cmake_minimum_required(VERSION 3.12) # 3.12 allows linking object libs
project(Jiffy C CXX)

set(CMAKE_CXX_STANDARD 14)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

set(PROJECT_NAME "Jiffy")
set(PROJECT_VERSION_MAJOR "0")
set(PROJECT_VERSION_MINOR "1")
set(PROJECT_VERSION_PATCH "0")
set(PROJECT_VERSION "${PROJECT_VERSION_MAJOR}.${PROJECT_VERSION_MINOR}.${PROJECT_VERSION_PATCH}")

set(CMAKE_MODULE_PATH ${CMAKE_MODULE_PATH} "${CMAKE_SOURCE_DIR}/cmake-modules")

set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fPIC -O0 -Wall -pedantic -Wextra")
set(CMAKE_CXX_FLAGS_RELEASE "${CMAKE_CXX_FLAGS_RELEASE} -fPIC -Wall -pedantic -Wextra")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "${CMAKE_CXX_FLAGS_RELWITHDEBINFO} -fPIC -O3 -g -Wall -pedantic -Wextra")

set(CMAKE_BUILD_TYPE RelWithDebInfo)

# CMake modules
include(Functions)
include(BuildOptions)
include(Dependencies)

if (GENERATE_THRIFT)
  add_subdirectory(thrift)
endif ()

if (BUILD_TESTS)
  include(CTest)
  enable_testing(true)
endif ()

add_subdirectory(libjiffy)

if (BUILD_DIRECTORY)
  add_subdirectory(directory)
endif ()

if (BUILD_STORAGE)
  add_subdirectory(storage)
endif ()

if (BUILD_PYTHON_CLIENT)
  add_subdirectory(pyjiffy)
endif ()

if (BUILD_JAVA_CLIENT)
  add_subdirectory(jiffy4j)
endif ()

if (BUILD_BENCHMARKS)
  add_subdirectory(benchmark)
endif ()

if (BUILD_DOC)
  add_subdirectory(docs)
endif ()

install(FILES conf/jiffy.conf DESTINATION conf)

# uninstall target
if (NOT TARGET uninstall)
  configure_file(
          "${CMAKE_CURRENT_SOURCE_DIR}/cmake_uninstall.cmake.in"
          "${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake"
          IMMEDIATE @ONLY)

  add_custom_target(uninstall COMMAND ${CMAKE_COMMAND} -P ${CMAKE_CURRENT_BINARY_DIR}/cmake_uninstall.cmake)
endif ()

# CPack


# default package generators
if (APPLE)
  set(PACKAGE_GENERATOR "TGZ;ZIP")
  set(PACKAGE_SOURCE_GENERATOR "TGZ;ZIP")
elseif (UNIX)
  set(PACKAGE_GENERATOR "TGZ;DEB;RPM")
  set(PACKAGE_SOURCE_GENERATOR "TGZ;ZIP")
else ()
  set(PACKAGE_GENERATOR "ZIP")
  set(PACKAGE_SOURCE_GENERATOR "ZIP")
endif ()

# used package generators
set(CPACK_GENERATOR "${PACKAGE_GENERATOR}" CACHE STRING "List of binary package generators (CPack).")
set(CPACK_SOURCE_GENERATOR "${PACKAGE_SOURCE_GENERATOR}" CACHE STRING "List of source package generators (CPack).")
mark_as_advanced(CPACK_GENERATOR CPACK_SOURCE_GENERATOR)

# some package generators (e.g., PackageMaker) do not allow .md extension
configure_file("${CMAKE_CURRENT_LIST_DIR}/README.md" "${CMAKE_CURRENT_BINARY_DIR}/README.txt" COPYONLY)

set(CPACK_PACKAGE_VENDOR "Anurag Khandelwal")
set(CPACK_PACKAGE_CONTACT "anuragk@berkeley.edu")
set(CPACK_PACKAGE_NAME "jiffy")
set(CPACK_PACKAGE_VERSION ${PROJECT_VERSION})
set(CPACK_PACKAGE_VERSION_MAJOR ${PROJECT_VERSION_MAJOR})
set(CPACK_PACKAGE_VERSION_MINOR ${PROJECT_VERSION_MINOR})
set(CPACK_PACKAGE_VERSION_PATCH ${PROJECT_VERSION_PATCH})
set(CPACK_PACKAGE_DESCRIPTION_SUMMARY "Jiffy")
set(CPACK_RESOURCE_FILE_WELCOME "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_RESOURCE_FILE_LICENSE "${CMAKE_CURRENT_SOURCE_DIR}/LICENSE")
set(CPACK_PACKAGE_DESCRIPTION_FILE "${CMAKE_CURRENT_BINARY_DIR}/README.txt")
set(CPACK_INSTALL_PREFIX "${CMAKE_INSTALL_PREFIX}")
set(CPACK_OUTPUT_FILE_PREFIX packages)
set(CPACK_PACKAGE_RELOCATABLE TRUE)
set(CPACK_MONOLITHIC_INSTALL TRUE)

set(CPACK_RPM_PACKAGE_GROUP "Development/Libraries")
set(CPACK_RPM_PACKAGE_LICENSE "BSD")
set(CPACK_RPM_PACKAGE_URL "${PACKAGE_URL}")
set(CPACK_RPM_CHANGELOG_FILE "${CMAKE_CURRENT_LIST_DIR}/ChangeLog.txt")

string(TOLOWER "${CMAKE_SYSTEM_NAME}" CPACK_SYSTEM_NAME)
if (APPLE)
  execute_process(COMMAND uname -m OUTPUT_VARIABLE CPACK_PACKAGE_ARCHITECTURE)
  if (CPACK_PACKAGE_ARCHITECTURE MATCHES "x86_64")
    set(CPACK_PACKAGE_ARCHITECTURE amd64)
  else ()
    set(CPACK_PACKAGE_ARCHITECTURE i386)
  endif ()
else ()
  if (CMAKE_CXX_FLAGS MATCHES "-m32")
    set(CPACK_PACKAGE_ARCHITECTURE i386)
  else ()
    execute_process(
            COMMAND dpkg --print-architecture
            RESULT_VARIABLE RV
            OUTPUT_VARIABLE CPACK_PACKAGE_ARCHITECTURE
    )
    if (RV EQUAL 0)
      string(STRIP "${CPACK_PACKAGE_ARCHITECTURE}" CPACK_PACKAGE_ARCHITECTURE)
    else ()
      execute_process(COMMAND uname -m OUTPUT_VARIABLE CPACK_PACKAGE_ARCHITECTURE)
      if (CPACK_PACKAGE_ARCHITECTURE MATCHES "x86_64")
        set(CPACK_PACKAGE_ARCHITECTURE amd64)
      else ()
        set(CPACK_PACKAGE_ARCHITECTURE i386)
      endif ()
    endif ()
  endif ()
endif ()

set(CPACK_SOURCE_TOPLEVEL_TAG "source")
set(CPACK_SOURCE_IGNORE_FILES "~$;[.]swp$;/[.]svn/;/[.]git/;.gitignore;/build/;tags;cscope.*")
set(CPACK_SOURCE_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")
set(CPACK_SOURCE_IGNORE_FILES "/\\\\.git/;\\\\.swp$;\\\\.#;/#;\\\\.*~;cscope\\\\.*;/[Bb]uild[.+-_a-zA-Z0-9]*/")

set(CPACK_INCLUDE_TOPLEVEL_DIRECTORY TRUE)

if (CPACK_SYSTEM_NAME)
  set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}-${CPACK_SYSTEM_NAME}")
else ()
  set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_NAME}-${CPACK_PACKAGE_VERSION}")
endif ()

if (CPACK_PACKAGE_ARCHITECTURE)
  set(CPACK_PACKAGE_FILE_NAME "${CPACK_PACKAGE_FILE_NAME}-${CPACK_PACKAGE_ARCHITECTURE}")
endif ()

configure_file("${CMAKE_CURRENT_LIST_DIR}/cmake-modules/Package.cmake.in"
        "${PROJECT_BINARY_DIR}/${CPACK_PACKAGE_NAME}-package.cmake" @ONLY)
set(CPACK_PROJECT_CONFIG_FILE "${PROJECT_BINARY_DIR}/${CPACK_PACKAGE_NAME}-package.cmake")

include(CPack)
