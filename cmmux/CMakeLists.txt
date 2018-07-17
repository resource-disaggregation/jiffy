include_directories(../libmmux/src)
add_library(cmmux STATIC
        src/mmux/client/cmmux_client.cpp
        src/mmux/client/cmmux_client.h
        src/mmux/directory/client/directory_client_wrapper.cpp
        src/mmux/directory/client/directory_client_wrapper.h
        src/mmux/storage/client/kv_client_wrapper.cpp
        src/mmux/storage/client/kv_client_wrapper.h
        src/mmux/storage/client/kv_listener_wrapper.cpp
        src/mmux/storage/client/kv_listener_wrapper.h
        src/mmux/utils/connection_utils.h
        src/mmux/utils/connection_utils.cpp
        src/mmux/utils/logging.h
        src/mmux/utils/logging.cpp)

target_link_libraries(cmmux mmux)
set_target_properties(cmmux PROPERTIES LINKER_LANGUAGE CXX)

install(TARGETS cmmux
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)
install(DIRECTORY src/mmux
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

if (BUILD_TESTS)
  include_directories(../libmmux/test)
  include_directories(${CATCH_INCLUDE_DIR})
  add_executable(cmmux_tests test/test_cmmux_client.c test/test.h)
  target_link_libraries(cmmux_tests cmmux)
endif ()