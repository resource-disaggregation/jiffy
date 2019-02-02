file(GLOB_RECURSE files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.py *.cfg *.xml *.conf)
create_symlinks(${files})

find_package(PythonInterp REQUIRED)

add_custom_target(pyclient ALL
        COMMAND ${PYTHON_EXECUTABLE} setup.py build --build-base ${CMAKE_CURRENT_BINARY_DIR}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Building Python client")

set(ENVVARS "JIFFY_VERSION=${PROJECT_VERSION}" "THRIFT_VERSION=${THRIFT_VERSION}")
if (BUILD_TESTS)
  set(STORAGE_EXEC "${CMAKE_CURRENT_BINARY_DIR}/../storage/storaged")
  set(DIRECTORY_EXEC "${CMAKE_CURRENT_BINARY_DIR}/../directory/directoryd")
  set(TEST_ENVVARS "${ENVVARS}" "DIRECTORY_SERVER_EXEC=${DIRECTORY_EXEC}" "STORAGE_SERVER_EXEC=${STORAGE_EXEC}")
  add_test(NAME PythonClientTest
          COMMAND "${CMAKE_COMMAND}" -E env ${TEST_ENVVARS} ${PYTHON_EXECUTABLE} setup.py test
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif ()

install(CODE "execute_process(COMMAND ${CMAKE_COMMAND} -E env ${TEST_ENVVARS} ${PYTHON_EXECUTABLE} setup.py install WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})")