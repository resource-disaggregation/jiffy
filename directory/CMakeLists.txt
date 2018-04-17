include_directories(../libelasticmem/src)
add_executable(directoryd src/directory_server.cpp)

if (NOT USE_SYSTEM_JEMALLOC)
  add_dependencies(directoryd jemalloc)
endif ()

target_link_libraries(directoryd elasticmem ${JEMALLOC_LIBRARIES})

install(TARGETS directoryd
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)