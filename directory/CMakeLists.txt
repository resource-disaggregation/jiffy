include_directories(../libkv/src ../libdirectory/src ../libpersistent/src)
add_executable(directoryd src/directory_server.cpp)
target_link_libraries(directoryd directory kv ${THRIFT_LIBRARIES})