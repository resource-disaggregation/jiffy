include_directories(../libelasticmem/src)
add_executable(directoryd src/directory_server.cpp)
target_link_libraries(directoryd elasticmem)

install(TARGETS directoryd
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)