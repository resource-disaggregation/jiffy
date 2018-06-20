file(GLOB_RECURSE PY_SOURCES ${CMAKE_CURRENT_SOURCE_DIR}/mmux/*.py)

add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pyclient_symlinks.stamp
        DEPENDS ${PY_SOURCES}
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/mmux ${CMAKE_CURRENT_BINARY_DIR}/mmux
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/test ${CMAKE_CURRENT_BINARY_DIR}/test
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/setup.cfg ${CMAKE_CURRENT_BINARY_DIR}/setup.cfg
        COMMAND ${CMAKE_COMMAND} -E create_symlink ${CMAKE_CURRENT_SOURCE_DIR}/setup.py ${CMAKE_CURRENT_BINARY_DIR}/setup.py
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Creating symlinks for Python client"
        VERBATIM)
add_custom_target(pyclient_symlinks ALL
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/pyclient_symlinks.stamp)

add_custom_command(
        OUTPUT ${CMAKE_CURRENT_BINARY_DIR}/pyclient.stamp
        DEPENDS ${PY_SOURCES} pyclient_symlinks
        COMMAND ${PYTHON_EXECUTABLE} setup.py build --build-base ${CMAKE_CURRENT_BINARY_DIR}
        COMMAND ${CMAKE_COMMAND} -E touch ${CMAKE_CURRENT_BINARY_DIR}/pyclient.stamp
        WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}
        COMMENT "Building Python client"
        VERBATIM)
add_custom_target(pyclient ALL
        DEPENDS ${CMAKE_CURRENT_BINARY_DIR}/pyclient.stamp)

if (BUILD_TESTS)
  add_test(NAME pyclient_test
          COMMAND ${CMAKE_COMMAND} -E ${PYTHON_EXECUTABLE} setup.py test
          WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif ()

set(INSTALL_CMD "${PYTHON_EXECUTABLE} setup.py install --record ${PROJECT_BINARY_DIR}/pyinstall_manifest.txt")
install(CODE "execute_process(COMMAND ${INSTALL_CMD} WORKING_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})")