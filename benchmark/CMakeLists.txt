if (NOT APPLE AND UNIX)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libjiffy/src
        ${CMAKE_CURRENT_SOURCE_DIR}/../benchmark
        ${Boost_INCLUDE_DIRS})

add_executable(hash_table_bench src/hash_table_benchmark.cpp)

add_dependencies(hash_table_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(hash_table_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS hash_table_bench
        RUNTIME DESTINATION bin)

add_executable(file_bench src/file_benchmark.cpp)

add_dependencies(file_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(file_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS file_bench
        RUNTIME DESTINATION bin)

add_executable(shared_log_bench src/shared_log_benchmark.cpp)

add_dependencies(shared_log_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(shared_log_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS shared_log_bench
        RUNTIME DESTINATION bin)

add_executable(fifo_queue_bench src/fifo_queue_benchmark.cpp)

add_dependencies(fifo_queue_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(fifo_queue_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS fifo_queue_bench
        RUNTIME DESTINATION bin)

add_executable(file_auto_scaling_bench src/file_auto_scaling_benchmark.cpp)

add_dependencies(file_auto_scaling_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(file_auto_scaling_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS file_auto_scaling_bench
        RUNTIME DESTINATION bin)

add_executable(fifo_queue_auto_scaling_bench src/fifo_queue_auto_scaling_benchmark.cpp)

add_dependencies(fifo_queue_auto_scaling_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(fifo_queue_auto_scaling_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS fifo_queue_auto_scaling_bench
        RUNTIME DESTINATION bin)

add_executable(hash_table_auto_scaling_bench src/hash_table_auto_scaling_benchmark.cpp src/zipf_generator.h)

add_dependencies(hash_table_auto_scaling_bench boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(hash_table_auto_scaling_bench jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS hash_table_auto_scaling_bench
        RUNTIME DESTINATION bin)

add_executable(hash_table_auto_scaling_get src/hash_table_auto_scaling_get.cpp src/zipf_generator.h)

add_dependencies(hash_table_auto_scaling_get boost_ep ${HEAP_MANAGER_EP})

target_link_libraries(hash_table_auto_scaling_get jiffy_client ${HEAP_MANAGER_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})

install(TARGETS hash_table_auto_scaling_get
        RUNTIME DESTINATION bin)