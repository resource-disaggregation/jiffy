include(ExternalProject)

set(THRIFT_VERSION "0.11.0")

find_package(Threads REQUIRED)

set(EXTERNAL_CXX_FLAGS "${CMAKE_CXX_FLAGS} -fPIC ${CMAKE_CXX_FLAGS_${UPPERCASE_BUILD_TYPE}}")
set(EXTERNAL_C_FLAGS "${CMAKE_C_FLAGS} -fPIC ${CMAKE_C_FLAGS_${UPPERCASE_BUILD_TYPE}}")

if (USE_SYSTEM_THRIFT)
  find_package(Thrift ${THRIFT_VERSION} REQUIRED)
else ()
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
          "-DWITH_LIBEVENT=OFF" # For now
          "-DWITH_JAVA=OFF"
          "-DWITH_PYTHON=OFF"
          "-DWITH_CPP=ON"
          "-DWITH_STDTHREADS=OFF"
          "-DWITH_BOOSTTHREADS=OFF"
          "-DWITH_STATIC_LIB=ON")

  set(THRIFT_STATIC_LIB_NAME "${CMAKE_STATIC_LIBRARY_PREFIX}thrift")
  if (${CMAKE_BUILD_TYPE} STREQUAL "Debug")
    set(THRIFT_STATIC_LIB_NAME "${THRIFT_STATIC_LIB_NAME}d")
  endif ()
  set(THRIFT_LIBRARIES "${THRIFT_PREFIX}/lib/${THRIFT_STATIC_LIB_NAME}${CMAKE_STATIC_LIBRARY_SUFFIX}")
  if (GENERATE_THRIFT)
    set(THRIFT_COMPILER "${THRIFT_PREFIX}/bin/thrift")
  endif ()
  ExternalProject_Add(thrift
          URL "http://archive.apache.org/dist/thrift/${THRIFT_VERSION}/thrift-${THRIFT_VERSION}.tar.gz"
          CMAKE_ARGS ${THRIFT_CMAKE_ARGS})

  include_directories(SYSTEM ${THRIFT_INCLUDE_DIR} ${THRIFT_INCLUDE_DIR}/thrift)
  message(STATUS "Thrift include dir: ${THRIFT_INCLUDE_DIR}")
  message(STATUS "Thrift static library: ${THRIFT_LIBRARIES}")

  if (GENERATE_THRIFT)
    message(STATUS "Thrift compiler: ${THRIFT_COMPILER}")
    add_executable(thriftcompiler IMPORTED GLOBAL)
    set_target_properties(thriftcompiler PROPERTIES IMPORTED_LOCATION ${THRIFT_COMPILER})
    add_dependencies(thriftcompiler thrift)
  endif ()

  add_library(thriftstatic STATIC IMPORTED GLOBAL)
  set_target_properties(thriftstatic PROPERTIES IMPORTED_LOCATION ${THRIFT_LIBRARIES})
  add_dependencies(thriftstatic thrift)

  install(FILES ${THRIFT_STATIC_LIB} DESTINATION lib)
  install(DIRECTORY ${THRIFT_INCLUDE_DIR}/thrift DESTINATION include)
endif ()

# Google Test framework
if (BUILD_TESTS AND NOT USE_SYSTEM_CATCH)
  find_package(Git REQUIRED)
  ExternalProject_Add(catch
          PREFIX ${CMAKE_BINARY_DIR}/catch
          GIT_REPOSITORY https://github.com/philsquared/Catch.git
          TIMEOUT 10
          UPDATE_COMMAND ${GIT_EXECUTABLE} pull
          CONFIGURE_COMMAND ""
          BUILD_COMMAND ""
          INSTALL_COMMAND ""
          LOG_DOWNLOAD ON)

  ExternalProject_Get_Property(catch source_dir)
  set(CATCH_INCLUDE_DIR ${source_dir}/single_include CACHE INTERNAL "Path to include folder for Catch")
endif ()