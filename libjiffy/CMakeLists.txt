include_directories(${THRIFT_INCLUDE_DIR}
        ${CMAKE_CURRENT_SOURCE_DIR}/src
        ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/libcuckoo
        ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/btree)

if (BUILD_STORAGE OR BUILD_DIRECTORY)
  add_library(jiffy OBJECT
          src/jiffy/client/jiffy_client.cc
          src/jiffy/client/jiffy_client.h
          src/jiffy/directory/block/block_registration_client.cpp
          src/jiffy/directory/block/block_registration_client.h
          src/jiffy/directory/block/block_registration_service.cpp
          src/jiffy/directory/block/block_registration_service.h
          src/jiffy/directory/block/block_registration_service.tcc
          src/jiffy/directory/block/block_registration_service_constants.cpp
          src/jiffy/directory/block/block_registration_service_constants.h
          src/jiffy/directory/block/block_registration_service_factory.cpp
          src/jiffy/directory/block/block_registration_service_factory.h
          src/jiffy/directory/block/block_registration_service_handler.cpp
          src/jiffy/directory/block/block_registration_service_handler.h
          src/jiffy/directory/block/block_registration_service_types.cpp
          src/jiffy/directory/block/block_registration_service_types.h
          src/jiffy/directory/block/block_registration_service_types.tcc
          src/jiffy/directory/block/block_registration_server.cpp
          src/jiffy/directory/block/block_registration_server.h
          src/jiffy/directory/block/block_allocator.h
          src/jiffy/directory/block/file_size_tracker.cpp
          src/jiffy/directory/block/file_size_tracker.h
          src/jiffy/directory/block/random_block_allocator.cpp
          src/jiffy/directory/block/random_block_allocator.h
          src/jiffy/directory/client/directory_client.cpp
          src/jiffy/directory/client/directory_client.h
          src/jiffy/directory/fs/directory_server.cpp
          src/jiffy/directory/fs/directory_server.h
          src/jiffy/directory/fs/directory_service.cpp
          src/jiffy/directory/fs/directory_service.h
          src/jiffy/directory/fs/directory_service.tcc
          src/jiffy/directory/fs/directory_service_constants.cpp
          src/jiffy/directory/fs/directory_service_constants.h
          src/jiffy/directory/fs/directory_service_factory.cpp
          src/jiffy/directory/fs/directory_service_factory.h
          src/jiffy/directory/fs/directory_service_handler.cpp
          src/jiffy/directory/fs/directory_service_handler.h
          src/jiffy/directory/fs/directory_service_types.cpp
          src/jiffy/directory/fs/directory_service_types.h
          src/jiffy/directory/fs/directory_service_types.tcc
          src/jiffy/directory/fs/directory_tree.cpp
          src/jiffy/directory/fs/directory_tree.h
          src/jiffy/directory/fs/directory_type_conversions.h
          src/jiffy/directory/fs/sync_worker.cpp
          src/jiffy/directory/fs/sync_worker.h
          src/jiffy/directory/client/lease_client.cpp
          src/jiffy/directory/client/lease_client.h
          src/jiffy/directory/client/lease_renewal_worker.cc
          src/jiffy/directory/client/lease_renewal_worker.h
          src/jiffy/directory/lease/lease_server.cpp
          src/jiffy/directory/lease/lease_server.h
          src/jiffy/directory/lease/lease_service.cpp
          src/jiffy/directory/lease/lease_service.h
          src/jiffy/directory/lease/lease_service.tcc
          src/jiffy/directory/lease/lease_service_constants.cpp
          src/jiffy/directory/lease/lease_service_constants.h
          src/jiffy/directory/lease/lease_service_factory.cpp
          src/jiffy/directory/lease/lease_service_factory.h
          src/jiffy/directory/lease/lease_service_handler.cpp
          src/jiffy/directory/lease/lease_service_handler.h
          src/jiffy/directory/lease/lease_service_types.cpp
          src/jiffy/directory/lease/lease_service_types.h
          src/jiffy/directory/lease/lease_service_types.tcc
          src/jiffy/directory/lease/lease_expiry_worker.cpp
          src/jiffy/directory/lease/lease_expiry_worker.h
          src/jiffy/directory/directory_ops.h
          src/jiffy/directory/directory_ops.cpp
          src/jiffy/storage/partition.h
          src/jiffy/storage/partition.cpp
          src/jiffy/storage/partition_manager.h
          src/jiffy/storage/partition_manager.cpp
          src/jiffy/storage/chain_module.h
          src/jiffy/storage/chain_module.cpp
          src/jiffy/storage/command.h
          src/jiffy/storage/command.cpp
          src/jiffy/storage/serde/serde_all.h
          src/jiffy/storage/hashtable/hash_slot.h
          src/jiffy/storage/hashtable/hash_slot.cpp
          src/jiffy/storage/hashtable/hash_table_defs.h
          src/jiffy/storage/hashtable/hash_table_ops.h
          src/jiffy/storage/hashtable/hash_table_ops.cpp
          src/jiffy/storage/hashtable/hash_table_partition.cpp
          src/jiffy/storage/hashtable/hash_table_partition.h
          src/jiffy/storage/btree/btree_defs.h
          src/jiffy/storage/btree/btree_ops.h
          src/jiffy/storage/btree/btree_ops.cpp
          src/jiffy/storage/btree/btree_partition.h
          src/jiffy/storage/btree/btree_partition.cpp
          src/jiffy/storage/msgqueue/msg_queue_defs.h
          src/jiffy/storage/msgqueue/msg_queue_ops.h
          src/jiffy/storage/msgqueue/msg_queue_ops.cpp
          src/jiffy/storage/msgqueue/msg_queue_partition.h
          src/jiffy/storage/msgqueue/msg_queue_partition.cpp
          src/jiffy/storage/client/replica_chain_client.cpp
          src/jiffy/storage/client/replica_chain_client.h
          src/jiffy/storage/service/block_response_client.cpp
          src/jiffy/storage/service/block_response_client.h
          src/jiffy/storage/service/block_server.cpp
          src/jiffy/storage/service/block_server.h
          src/jiffy/storage/client/block_client.cpp
          src/jiffy/storage/client/block_client.h
          src/jiffy/storage/client/block_listener.cpp
          src/jiffy/storage/client/block_listener.h
          src/jiffy/storage/client/data_structure_client.cpp
          src/jiffy/storage/client/data_structure_client.h
          src/jiffy/storage/client/hash_table_client.cpp
          src/jiffy/storage/client/hash_table_client.h
          src/jiffy/storage/client/btree_client.cpp
          src/jiffy/storage/client/btree_client.h
          src/jiffy/storage/client/msg_queue_client.cpp
          src/jiffy/storage/client/msg_queue_client.h
          src/jiffy/storage/client/data_structure_listener.cpp
          src/jiffy/storage/client/data_structure_listener.h
          src/jiffy/storage/service/block_request_handler.cpp
          src/jiffy/storage/service/block_request_handler.h
          src/jiffy/storage/chain/chain_request_client.cpp
          src/jiffy/storage/chain/chain_request_client.h
          src/jiffy/storage/chain/chain_response_client.cpp
          src/jiffy/storage/chain/chain_response_client.h
          src/jiffy/storage/service/block_response_client_map.cpp
          src/jiffy/storage/service/block_response_client_map.h
          src/jiffy/storage/service/block_request_handler_factory.cpp
          src/jiffy/storage/service/block_request_handler_factory.h
          src/jiffy/storage/service/block_request_service.cpp
          src/jiffy/storage/service/block_request_service.h
          src/jiffy/storage/service/block_request_service.tcc
          src/jiffy/storage/service/block_response_service.cpp
          src/jiffy/storage/service/block_response_service.h
          src/jiffy/storage/service/block_response_service.tcc
          src/jiffy/storage/service/block_service_constants.cpp
          src/jiffy/storage/service/block_service_constants.h
          src/jiffy/storage/service/block_service_types.cpp
          src/jiffy/storage/service/block_service_types.h
          src/jiffy/storage/service/block_service_types.tcc
          src/jiffy/storage/manager/detail/block_id_parser.cpp
          src/jiffy/storage/manager/detail/block_id_parser.h
          src/jiffy/storage/manager/storage_management_client.cpp
          src/jiffy/storage/manager/storage_management_client.h
          src/jiffy/storage/manager/storage_management_server.cpp
          src/jiffy/storage/manager/storage_management_server.h
          src/jiffy/storage/manager/storage_management_service.cpp
          src/jiffy/storage/manager/storage_management_service.h
          src/jiffy/storage/manager/storage_management_service.tcc
          src/jiffy/storage/manager/storage_management_service_constants.cpp
          src/jiffy/storage/manager/storage_management_service_constants.h
          src/jiffy/storage/manager/storage_management_service_factory.cpp
          src/jiffy/storage/manager/storage_management_service_factory.h
          src/jiffy/storage/manager/storage_management_service_handler.cpp
          src/jiffy/storage/manager/storage_management_service_handler.h
          src/jiffy/storage/manager/storage_management_service_types.cpp
          src/jiffy/storage/manager/storage_management_service_types.h
          src/jiffy/storage/manager/storage_management_service_types.tcc
          src/jiffy/storage/manager/storage_manager.cpp
          src/jiffy/storage/manager/storage_manager.h
          src/jiffy/storage/notification/blocking_queue.h
          src/jiffy/storage/notification/notification_handler.cpp
          src/jiffy/storage/notification/notification_handler.h
          src/jiffy/storage/notification/notification_worker.cpp
          src/jiffy/storage/notification/notification_worker.h
          src/jiffy/storage/notification/subscription_map.cpp
          src/jiffy/storage/notification/subscription_map.h
          src/jiffy/storage/storage_management_ops.h
          src/jiffy/persistent/persistent_service.cpp
          src/jiffy/persistent/persistent_service.h
          src/jiffy/persistent/persistent_store.cpp
          src/jiffy/persistent/persistent_store.h
          src/jiffy/utils/client_cache.h
          src/jiffy/utils/cmd_parse.h
          src/jiffy/utils/directory_utils.h
          src/jiffy/utils/event.h
          src/jiffy/utils/logger.h
          src/jiffy/utils/logger.cpp
          src/jiffy/utils/rand_utils.h
          src/jiffy/utils/signal_handling.h
          src/jiffy/utils/time_utils.h
          src/jiffy/utils/retry_utils.h
          src/jiffy/utils/string_utils.h
          src/jiffy/utils/thread_utils.h
          src/jiffy/storage/block.cpp
          src/jiffy/storage/block.h
          src/jiffy/utils/property_map.cpp
          src/jiffy/utils/property_map.h
          src/jiffy/storage/block_memory_manager.h
          src/jiffy/storage/block_memory_manager.cpp
          src/jiffy/storage/block_memory_allocator.h
          src/jiffy/directory/fs/ds_node.cpp
          src/jiffy/directory/fs/ds_node.h
          src/jiffy/directory/fs/ds_file_node.cpp
          src/jiffy/directory/fs/ds_file_node.h
          src/jiffy/directory/fs/ds_dir_node.cpp
          src/jiffy/directory/fs/ds_dir_node.h
          src/jiffy/storage/notification/notification_response_client.cpp
          src/jiffy/storage/notification/notification_response_client.h
          src/jiffy/storage/types/binary.h
          src/jiffy/storage/types/binary.cpp)

  add_dependencies(jiffy thrift_ep awssdk_ep jemalloc_ep)
  target_link_libraries(jiffy ${THRIFT_LIBRARY}
          ${THRIFTNB_LIBRARY}
          ${LIBEVENT_LIBRARY}
          ${AWSSDK_LINK_LIBRARIES}
          ${JEMALLOC_LIBRARY}
          ${CMAKE_DL_LIBS}
          ${CMAKE_THREAD_LIBS_INIT})
  set_target_properties(jiffy PROPERTIES LINKER_LANGUAGE CXX)

  install(TARGETS jiffy
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION lib
          LIBRARY DESTINATION lib)
  install(DIRECTORY src/jiffy
          DESTINATION include
          FILES_MATCHING PATTERN "*.h")

  if (BUILD_TESTS)
    include_directories(${CATCH_INCLUDE_DIR})
    add_executable(jiffy_tests
            test/jiffy_tests.cpp
            test/auto_scaling_test.cpp
            #test/block_allocation_service_test.cpp
            #test/block_allocator_test.cpp
            #test/chain_replication_test.cpp
            #test/directory_lease_test.cpp
            #test/directory_tree_test.cpp
            #test/directory_service_test.cpp
            #test/file_size_tracker_test.cpp
            #test/hash_table_partition_test.cpp
            #test/hash_table_client_test.cpp
            #test/btree_partition_test.cpp
            #test/btree_client_test.cpp
            #test/msg_queue_partition_test.cpp
            #test/msg_queue_client_test.cpp
            #test/lease_expiry_worker_test.cpp
            #test/local_store_test.cpp
            #test/jiffy_client_test.cpp
            #test/notification_test.cpp
            #test/storage_manager_test.cpp
            #test/sync_worker_test.cpp
            #test/test_utils.h
            )
    target_link_libraries(jiffy_tests jiffy)
    add_dependencies(jiffy_tests catch_ep)
    # Register test
    add_test(JiffyTest jiffy_tests)
  endif ()
endif ()

if (BUILD_CPP_CLIENT)
  add_library(jiffy_client
          src/jiffy/client/jiffy_client.cc
          src/jiffy/client/jiffy_client.h
          src/jiffy/directory/client/directory_client.cpp
          src/jiffy/directory/client/directory_client.h
          src/jiffy/directory/fs/directory_service.cpp
          src/jiffy/directory/fs/directory_service.h
          src/jiffy/directory/fs/directory_service.tcc
          src/jiffy/directory/fs/directory_service_constants.cpp
          src/jiffy/directory/fs/directory_service_constants.h
          src/jiffy/directory/fs/directory_service_types.cpp
          src/jiffy/directory/fs/directory_service_types.h
          src/jiffy/directory/fs/directory_service_types.tcc
          src/jiffy/directory/fs/directory_type_conversions.h
          src/jiffy/directory/client/lease_client.cpp
          src/jiffy/directory/client/lease_client.h
          src/jiffy/directory/client/lease_renewal_worker.cc
          src/jiffy/directory/client/lease_renewal_worker.h
          src/jiffy/directory/lease/lease_service.cpp
          src/jiffy/directory/lease/lease_service.h
          src/jiffy/directory/lease/lease_service.tcc
          src/jiffy/directory/lease/lease_service_constants.cpp
          src/jiffy/directory/lease/lease_service_constants.h
          src/jiffy/directory/lease/lease_service_types.cpp
          src/jiffy/directory/lease/lease_service_types.h
          src/jiffy/directory/lease/lease_service_types.tcc
          src/jiffy/directory/directory_ops.h
          src/jiffy/directory/directory_ops.cpp
          src/jiffy/storage/command.h
          src/jiffy/storage/command.cpp
          src/jiffy/storage/hashtable/hash_slot.h
          src/jiffy/storage/hashtable/hash_slot.cpp
          src/jiffy/storage/hashtable/hash_table_ops.h
          src/jiffy/storage/hashtable/hash_table_ops.cpp
          src/jiffy/storage/btree/btree_ops.h
          src/jiffy/storage/btree/btree_ops.cpp
          src/jiffy/storage/msgqueue/msg_queue_ops.h
          src/jiffy/storage/msgqueue/msg_queue_ops.cpp
          src/jiffy/storage/client/replica_chain_client.cpp
          src/jiffy/storage/client/replica_chain_client.h
          src/jiffy/storage/service/block_response_client.cpp
          src/jiffy/storage/service/block_response_client.h
          src/jiffy/storage/client/block_client.cpp
          src/jiffy/storage/client/block_client.h
          src/jiffy/storage/client/block_listener.cpp
          src/jiffy/storage/client/block_listener.h
          src/jiffy/storage/client/data_structure_client.cpp
          src/jiffy/storage/client/data_structure_client.h
          src/jiffy/storage/client/hash_table_client.cpp
          src/jiffy/storage/client/hash_table_client.h
          src/jiffy/storage/client/btree_client.cpp
          src/jiffy/storage/client/btree_client.h
          src/jiffy/storage/client/msg_queue_client.cpp
          src/jiffy/storage/client/msg_queue_client.h
          src/jiffy/storage/client/data_structure_listener.cpp
          src/jiffy/storage/client/data_structure_listener.h
          src/jiffy/storage/service/block_request_service.cpp
          src/jiffy/storage/service/block_request_service.h
          src/jiffy/storage/service/block_request_service.tcc
          src/jiffy/storage/service/block_response_service.cpp
          src/jiffy/storage/service/block_response_service.h
          src/jiffy/storage/service/block_response_service.tcc
          src/jiffy/storage/service/block_service_constants.cpp
          src/jiffy/storage/service/block_service_constants.h
          src/jiffy/storage/service/block_service_types.cpp
          src/jiffy/storage/service/block_service_types.h
          src/jiffy/storage/service/block_service_types.tcc
          src/jiffy/storage/manager/detail/block_id_parser.cpp
          src/jiffy/storage/manager/detail/block_id_parser.h
          src/jiffy/storage/notification/blocking_queue.h
          src/jiffy/storage/notification/notification_handler.cpp
          src/jiffy/storage/notification/notification_handler.h
          src/jiffy/storage/notification/notification_worker.cpp
          src/jiffy/storage/notification/notification_worker.h
          src/jiffy/utils/client_cache.h
          src/jiffy/utils/cmd_parse.h
          src/jiffy/utils/directory_utils.h
          src/jiffy/utils/event.h
          src/jiffy/utils/logger.h
          src/jiffy/utils/logger.cpp
          src/jiffy/utils/rand_utils.h
          src/jiffy/utils/signal_handling.h
          src/jiffy/utils/time_utils.h
          src/jiffy/utils/retry_utils.h
          src/jiffy/utils/string_utils.h
          src/jiffy/utils/property_map.cpp
          src/jiffy/utils/property_map.h
          src/jiffy/utils/thread_utils.h)
  add_dependencies(jiffy_client thrift_ep)
  target_link_libraries(jiffy_client ${THRIFT_LIBRARY}
          ${THRIFTNB_LIBRARY}
          ${LIBEVENT_LIBRARY}
          ${CMAKE_DL_LIBS}
          ${CMAKE_THREAD_LIBS_INIT})
  set_target_properties(jiffy_client PROPERTIES LINKER_LANGUAGE CXX)

  install(TARGETS jiffy_client
          RUNTIME DESTINATION bin
          ARCHIVE DESTINATION lib
          LIBRARY DESTINATION lib)

  if (BUILD_BENCHMARKS)
    add_executable(hash_table_benchmark
            bench/hash_table_benchmark.cpp
            bench/benchmark_utils.h)
    add_executable(hash_table_notification_benchmark
            bench/hash_table_notification_benchmark.cpp
            bench/benchmark_utils.h)
    add_executable(persistence_benchmark
            bench/persistence_benchmark.cpp
            bench/benchmark_utils.h)
    target_link_libraries(hash_table_benchmark jiffy_client)
    target_link_libraries(hash_table_notification_benchmark jiffy_client)
    target_link_libraries(persistence_benchmark jiffy)
  endif ()
endif ()
