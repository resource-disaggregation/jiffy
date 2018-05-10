if (NOT APPLE AND UNIX)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl")
endif ()

include_directories(../libmmux/src)
add_executable(storaged src/storage_server.cpp)

if (NOT USE_SYSTEM_JEMALLOC)
  add_dependencies(storaged jemalloc)
endif ()

target_link_libraries(storaged mmux ${JEMALLOC_LIBRARIES})

install(TARGETS storaged
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)