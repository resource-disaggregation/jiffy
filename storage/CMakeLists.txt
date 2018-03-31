include_directories(../libelasticmem/src)
add_executable(storaged src/storage_server.cpp)
target_link_libraries(storaged elasticmem)

install(TARGETS storaged
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)