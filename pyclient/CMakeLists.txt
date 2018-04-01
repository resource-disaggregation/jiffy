file(GLOB_RECURSE files RELATIVE ${CMAKE_CURRENT_SOURCE_DIR} *.py *.cfg *.xml)
create_symlinks(${files})

add_custom_target(pyclient ALL
        COMMAND ${PYTHON_EXECUTABLE} setup.py build --build-base ${CMAKE_CURRENT_BINARY_DIR}
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Building Python client")

if (BUILD_TESTS)
  add_test(NAME pyclient_test
          COMMAND ${CMAKE_COMMAND} -E ${PYTHON_EXECUTABLE} setup.py test
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif ()

set(INSTALL_CMD "${PYTHON_EXECUTABLE} setup.py install --record ${PROJECT_BINARY_DIR}/pyinstall_manifest.txt")
install(CODE "execute_process(COMMAND ${INSTALL_CMD} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})")