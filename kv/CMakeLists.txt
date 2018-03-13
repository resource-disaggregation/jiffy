include_directories(../libkv/src ../libpersistent/src)
add_executable(kvd src/kv_server.cpp)
target_link_libraries(kvd kv persistent ${THRIFT_LIBRARIES})