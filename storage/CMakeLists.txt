include_directories(../libelasticmem/src)
add_executable(storaged src/storage_server.cpp)
target_link_libraries(storaged elasticmem ${THRIFT_LIBRARIES})