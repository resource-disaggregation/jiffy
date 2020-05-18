if (NOT APPLE AND UNIX)
    set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libjiffy/src
        ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/libcuckoo
        ${Boost_INCLUDE_DIRS})

add_executable(allocator src/allocator_server.cpp)
add_executable(scheduler src/scheduler_server.cpp)

add_dependencies(allocator boost_ep jemalloc_ep thrift_ep awssdk_ep)
add_dependencies(scheduler boost_ep jemalloc_ep thrift_ep awssdk_ep)

target_link_libraries(allocator jiffy ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})
target_link_libraries(scheduler jiffy ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS allocator
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)

install(TARGETS scheduler
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)