if (NOT APPLE AND UNIX)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libjiffy/src
        ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/libcuckoo
        ${Boost_INCLUDE_DIRS})
add_executable(storaged src/storage_server.cpp)

if (NOT USE_SYSTEM_BOOST)
  add_dependencies(directoryd boost)
endif ()

if (NOT USE_SYSTEM_JEMALLOC)
  add_dependencies(storaged jemalloc)
endif ()

target_link_libraries(storaged jiffy ${JEMALLOC_LIBRARIES} ${Boost_LIBRARIES})

install(TARGETS storaged
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)