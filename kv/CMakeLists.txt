include_directories(../libelasticmem/src)
add_executable(kvd src/kv_server.cpp)
target_link_libraries(kvd elasticmem ${THRIFT_LIBRARIES})