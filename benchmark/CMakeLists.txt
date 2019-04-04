if (NOT APPLE AND UNIX)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libjiffy/src
        ${Boost_INCLUDE_DIRS})
add_executable(hash_table_bench src/hash_table_benchmark.cpp)

add_dependencies(hash_table_bench boost_ep jemalloc_ep)

target_link_libraries(hash_table_bench jiffy_client ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS hash_table_bench
        RUNTIME DESTINATION bin)

add_executable(msg_queue_bench src/msg_queue_benchmark.cpp)

add_dependencies(msg_queue_bench boost_ep jemalloc_ep)

target_link_libraries(msg_queue_bench jiffy_client ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS msg_queue_bench
        RUNTIME DESTINATION bin)

add_executable(btree_bench src/btree_benchmark.cpp)

add_dependencies(btree_bench boost_ep jemalloc_ep)

target_link_libraries(btree_bench jiffy_client ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS btree_bench
        RUNTIME DESTINATION bin)

add_executable(msg_queue_auto_scaling_bench src/msg_queue_auto_scaling_benchmark.cpp)

add_dependencies(msg_queue_auto_scaling_bench boost_ep jemalloc_ep)

target_link_libraries(msg_queue_auto_scaling_bench jiffy_client ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS msg_queue_auto_scaling_bench
        RUNTIME DESTINATION bin)