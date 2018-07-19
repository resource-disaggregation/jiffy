if (BUILD_TESTS)
  # Environment variables
  set(CGO_CFLAGS "-I${PROJECT_SOURCE_DIR}/cmmux/src")
  set(CGO_LDFLAGS "-L${PROJECT_BINARY_DIR}/cmmux -lcmmux_shared")
  set(LD_LIBRARY_PATH "${PROJECT_BINARY_DIR}/cmmux")
  set(DIRECTORY_SERVER_EXEC "${PROJECT_BINARY_DIR}/directory/directoryd")
  set(STORAGE_SERVER_EXEC "${PROJECT_BINARY_DIR}/storage/storaged")
  set(TEST_RESOURCES_PREFIX "${CMAKE_CURRENT_LIST_DIR}/resources")
  set(GOPATH "${CMAKE_CURRENT_LIST_DIR}")

  set(MMUX_ENV "CGO_CFLAGS=\"${CGO_CFLAGS}\""
          "CGO_LDFLAGS=\"${CGO_LDFLAGS}\""
          "LD_LIBRARY_PATH=\"${LD_LIBRARY_PATH}\""
          "DIRECTORY_SERVER_EXEC=\"${DIRECTORY_SERVER_EXEC}\""
          "STORAGE_SERVER_EXEC=\"${STORAGE_SERVER_EXEC}\""
          "TEST_RESOURCES_PREFIX=\"${TEST_RESOURCES_PREFIX}\""
          "GOPATH=\"${GOPATH}\"")
  string(REPLACE ";" " " MMUX_ENV_STR "${MMUX_ENV}")
  set(MMUX_ENV_SETUP "${CMAKE_COMMAND} -E env ${MMUX_ENV_STR}")

  add_test(NAME gommux_tests
          COMMAND ${MMUX_ENV_SETUP} ${CMAKE_Go_COMPILER} test mmux
          WORKING_DIRECTORY ${CMAKE_CURRENT_LIST_DIR})
endif ()
