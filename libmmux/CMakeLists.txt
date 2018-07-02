include_directories(${THRIFT_INCLUDE_DIR})
add_library(mmux STATIC
        src/mmux/client/mmux_client.cc
        src/mmux/client/mmux_client.h
        src/mmux/directory/block/block_advertisement_client.cpp
        src/mmux/directory/block/block_advertisement_client.h
        src/mmux/directory/block/block_allocation_service.cpp
        src/mmux/directory/block/block_allocation_service.h
        src/mmux/directory/block/block_allocation_service.tcc
        src/mmux/directory/block/block_allocation_service_constants.cpp
        src/mmux/directory/block/block_allocation_service_constants.h
        src/mmux/directory/block/block_allocation_service_factory.cpp
        src/mmux/directory/block/block_allocation_service_factory.h
        src/mmux/directory/block/block_allocation_service_handler.cpp
        src/mmux/directory/block/block_allocation_service_handler.h
        src/mmux/directory/block/block_allocation_service_types.cpp
        src/mmux/directory/block/block_allocation_service_types.h
        src/mmux/directory/block/block_allocation_service_types.tcc
        src/mmux/directory/block/block_allocation_server.cpp
        src/mmux/directory/block/block_allocation_server.h
        src/mmux/directory/block/block_allocator.h
        src/mmux/directory/block/file_size_tracker.cpp
        src/mmux/directory/block/file_size_tracker.h
        src/mmux/directory/block/random_block_allocator.cpp
        src/mmux/directory/block/random_block_allocator.h
        src/mmux/directory/client/directory_client.cpp
        src/mmux/directory/client/directory_client.h
        src/mmux/directory/fs/directory_server.cpp
        src/mmux/directory/fs/directory_server.h
        src/mmux/directory/fs/directory_service.cpp
        src/mmux/directory/fs/directory_service.h
        src/mmux/directory/fs/directory_service.tcc
        src/mmux/directory/fs/directory_service_constants.cpp
        src/mmux/directory/fs/directory_service_constants.h
        src/mmux/directory/fs/directory_service_factory.cpp
        src/mmux/directory/fs/directory_service_factory.h
        src/mmux/directory/fs/directory_service_handler.cpp
        src/mmux/directory/fs/directory_service_handler.h
        src/mmux/directory/fs/directory_service_types.cpp
        src/mmux/directory/fs/directory_service_types.h
        src/mmux/directory/fs/directory_service_types.tcc
        src/mmux/directory/fs/directory_tree.cpp
        src/mmux/directory/fs/directory_tree.h
        src/mmux/directory/fs/directory_type_conversions.h
        src/mmux/directory/fs/sync_worker.cpp
        src/mmux/directory/fs/sync_worker.h
        src/mmux/directory/client/lease_client.cpp
        src/mmux/directory/client/lease_client.h
        src/mmux/directory/client/lease_renewal_worker.cc
        src/mmux/directory/client/lease_renewal_worker.h
        src/mmux/directory/lease/lease_server.cpp
        src/mmux/directory/lease/lease_server.h
        src/mmux/directory/lease/lease_service.cpp
        src/mmux/directory/lease/lease_service.h
        src/mmux/directory/lease/lease_service.tcc
        src/mmux/directory/lease/lease_service_constants.cpp
        src/mmux/directory/lease/lease_service_constants.h
        src/mmux/directory/lease/lease_service_factory.cpp
        src/mmux/directory/lease/lease_service_factory.h
        src/mmux/directory/lease/lease_service_handler.cpp
        src/mmux/directory/lease/lease_service_handler.h
        src/mmux/directory/lease/lease_service_types.cpp
        src/mmux/directory/lease/lease_service_types.h
        src/mmux/directory/lease/lease_service_types.tcc
        src/mmux/directory/lease/lease_expiry_worker.cpp
        src/mmux/directory/lease/lease_expiry_worker.h
        src/mmux/directory/directory_ops.h
        src/mmux/directory/directory_ops.cpp
        src/mmux/storage/kv/serde/csv_serde.cpp
        src/mmux/storage/kv/serde/csv_serde.h
        src/mmux/storage/kv/serde/serde.h
        src/mmux/storage/kv/serde/binary_serde.cpp
        src/mmux/storage/kv/serde/binary_serde.h
        src/mmux/storage/kv/hash_slot.h
        src/mmux/storage/kv/hash_slot.cpp
        src/mmux/storage/kv/kv_block.cpp
        src/mmux/storage/kv/kv_block.h
        src/mmux/storage/kv/kv_hash.h
        src/mmux/storage/block.h
        src/mmux/storage/chain_module.h
        src/mmux/storage/chain_module.cpp
        src/mmux/storage/client/replica_chain_client.cpp
        src/mmux/storage/client/replica_chain_client.h
        src/mmux/storage/service/block_response_client.cpp
        src/mmux/storage/service/block_response_client.h
        src/mmux/storage/service/block_server.cpp
        src/mmux/storage/service/block_server.h
        src/mmux/storage/client/block_client.cpp
        src/mmux/storage/client/block_client.h
        src/mmux/storage/client/block_listener.cpp
        src/mmux/storage/client/block_listener.h
        src/mmux/storage/client/kv_client.cpp
        src/mmux/storage/client/kv_client.h
        src/mmux/storage/client/kv_listener.cpp
        src/mmux/storage/client/kv_listener.h
        src/mmux/storage/service/block_request_handler.cpp
        src/mmux/storage/service/block_request_handler.h
        src/mmux/storage/service/chain_request_handler_factory.cpp
        src/mmux/storage/service/chain_request_handler_factory.h
        src/mmux/storage/service/chain_request_handler.cpp
        src/mmux/storage/service/chain_request_handler.h
        src/mmux/storage/service/chain_request_client.cpp
        src/mmux/storage/service/chain_request_client.h
        src/mmux/storage/service/chain_response_client.cpp
        src/mmux/storage/service/chain_response_client.h
        src/mmux/storage/service/block_response_client_map.cpp
        src/mmux/storage/service/block_response_client_map.h
        src/mmux/storage/service/block_request_handler_factory.cpp
        src/mmux/storage/service/block_request_handler_factory.h
        src/mmux/storage/service/block_request_service.cpp
        src/mmux/storage/service/block_request_service.h
        src/mmux/storage/service/block_request_service.tcc
        src/mmux/storage/service/block_response_service.cpp
        src/mmux/storage/service/block_response_service.h
        src/mmux/storage/service/block_response_service.tcc
        src/mmux/storage/service/block_service_constants.cpp
        src/mmux/storage/service/block_service_constants.h
        src/mmux/storage/service/block_service_types.cpp
        src/mmux/storage/service/block_service_types.h
        src/mmux/storage/service/block_service_types.tcc
        src/mmux/storage/service/chain_request_service.cpp
        src/mmux/storage/service/chain_request_service.h
        src/mmux/storage/service/chain_request_service.tcc
        src/mmux/storage/service/chain_response_service.cpp
        src/mmux/storage/service/chain_response_service.h
        src/mmux/storage/service/chain_response_service.tcc
        src/mmux/storage/service/chain_server.cpp
        src/mmux/storage/service/chain_server.h
        src/mmux/storage/block.cc
        src/mmux/storage/service/server_storage_tracker.cpp
        src/mmux/storage/service/server_storage_tracker.h
        src/mmux/storage/service/buffered_transport_factory.h
        src/mmux/storage/manager/detail/block_name_parser.cpp
        src/mmux/storage/manager/detail/block_name_parser.h
        src/mmux/storage/manager/storage_management_client.cpp
        src/mmux/storage/manager/storage_management_client.h
        src/mmux/storage/manager/storage_management_server.cpp
        src/mmux/storage/manager/storage_management_server.h
        src/mmux/storage/manager/storage_management_service.cpp
        src/mmux/storage/manager/storage_management_service.h
        src/mmux/storage/manager/storage_management_service.tcc
        src/mmux/storage/manager/storage_management_service_constants.cpp
        src/mmux/storage/manager/storage_management_service_constants.h
        src/mmux/storage/manager/storage_management_service_factory.cpp
        src/mmux/storage/manager/storage_management_service_factory.h
        src/mmux/storage/manager/storage_management_service_handler.cpp
        src/mmux/storage/manager/storage_management_service_handler.h
        src/mmux/storage/manager/storage_management_service_types.cpp
        src/mmux/storage/manager/storage_management_service_types.h
        src/mmux/storage/manager/storage_management_service_types.tcc
        src/mmux/storage/manager/storage_manager.cpp
        src/mmux/storage/manager/storage_manager.h
        src/mmux/storage/notification/blocking_queue.h
        src/mmux/storage/notification/notification_handler.cpp
        src/mmux/storage/notification/notification_handler.h
        src/mmux/storage/notification/notification_server.cpp
        src/mmux/storage/notification/notification_server.h
        src/mmux/storage/notification/notification_service.cpp
        src/mmux/storage/notification/notification_service.h
        src/mmux/storage/notification/notification_service.tcc
        src/mmux/storage/notification/notification_service_constants.cpp
        src/mmux/storage/notification/notification_service_constants.h
        src/mmux/storage/notification/notification_service_factory.cpp
        src/mmux/storage/notification/notification_service_factory.h
        src/mmux/storage/notification/notification_service_handler.cpp
        src/mmux/storage/notification/notification_service_handler.h
        src/mmux/storage/notification/notification_service_types.cpp
        src/mmux/storage/notification/notification_service_types.h
        src/mmux/storage/notification/notification_service_types.tcc
        src/mmux/storage/notification/notification_worker.cpp
        src/mmux/storage/notification/notification_worker.h
        src/mmux/storage/notification/subscription_map.cpp
        src/mmux/storage/notification/subscription_map.h
        src/mmux/storage/notification/subscription_service.cpp
        src/mmux/storage/notification/subscription_service.h
        src/mmux/storage/notification/subscription_service.tcc
        src/mmux/storage/notification/subscription_service_constants.cpp
        src/mmux/storage/notification/subscription_service_constants.h
        src/mmux/storage/notification/subscription_service_types.cpp
        src/mmux/storage/notification/subscription_service_types.h
        src/mmux/storage/notification/subscription_service_types.tcc
        src/mmux/storage/storage_management_ops.h
        src/mmux/persistent/local/local_store.cpp
        src/mmux/persistent/local/local_store.h
        src/mmux/persistent/s3/s3_store.cpp
        src/mmux/persistent/s3/s3_store.h
        src/mmux/persistent/persistent_service.h
        src/mmux/persistent/persistent_store.cpp
        src/mmux/persistent/persistent_store.h
        src/mmux/utils/client_cache.h
        src/mmux/utils/cmd_parse.h
        src/mmux/utils/directory_utils.h
        src/mmux/utils/event.h
        src/mmux/utils/logger.h
        src/mmux/utils/logger.cpp
        src/mmux/utils/rand_utils.h
        src/mmux/utils/signal_handling.h
        src/mmux/utils/time_utils.h
        src/mmux/utils/retry_utils.h
        src/mmux/utils/string_utils.h)

if (GENERATE_THRIFT)
  add_dependencies(mmux thriftgen_directory thriftgen_kv)
endif ()

if (NOT USE_SYSTEM_LIBEVENT)
  add_dependencies(mmux libevent)
endif ()

if (NOT USE_SYSTEM_THRIFT)
  add_dependencies(mmux thrift)
endif ()

if (NOT USE_SYSTEM_AWS_SDK)
  add_dependencies(mmux awssdk)
endif ()

if (NOT USE_SYSTEM_LIBCUCKOO)
  add_dependencies(mmux libcuckoo)
endif ()

target_link_libraries(mmux ${THRIFT_LIBRARIES}
        ${THRIFTNB_LIBRARIES}
        ${LIBEVENT_LIBRARIES}
        ${AWS_LIBRARIES}
        ${CURL_LIBRARY}
        ${OPENSSL_LIBRARIES}
        ${ZLIB_LIBRARY}
        ${CMAKE_DL_LIBS}
        ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(mmux PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})
  add_executable(mmux_tests
          test/mmux_tests.cpp
          test/auto_scaling_test.cpp
          test/block_allocation_service_test.cpp
          test/block_allocator_test.cpp
          test/chain_replication_test.cpp
          test/directory_lease_test.cpp
          test/directory_tree_test.cpp
          test/directory_service_test.cpp
          test/file_size_tracker_test.cpp
          test/kv_block_test.cpp
          test/kv_client_test.cc
          test/kv_rpc_test.cpp
          test/lease_expiry_worker_test.cpp
          test/local_store_test.cpp
          test/mmux_client_test.cpp
          test/notification_test.cpp
          test/storage_manager_test.cpp
          test/sync_worker_test.cpp
          test/test_utils.h)
  target_link_libraries(mmux_tests mmux)
  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(mmux_tests catch)
  endif ()
endif ()

if (BUILD_BENCHMARKS)
  add_executable(kv_rpc_benchmark
          bench/kv_rpc_benchmark.cpp
          bench/benchmark_utils.h)
  add_executable(kv_notification_benchmark
          bench/kv_notification_benchmark.cpp
          bench/benchmark_utils.h)
  add_executable(persistence_benchmark
          bench/persistence_benchmark.cpp
          bench/benchmark_utils.h)
  target_link_libraries(kv_rpc_benchmark mmux)
  target_link_libraries(kv_notification_benchmark mmux)
  target_link_libraries(persistence_benchmark mmux)
endif ()