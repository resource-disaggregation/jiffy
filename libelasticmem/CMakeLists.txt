include_directories(${THRIFT_INCLUDE_DIR})
add_library(elasticmem STATIC
        src/directory/block/block_advertisement_client.cpp
        src/directory/block/block_advertisement_client.h
        src/directory/block/block_allocation_service.cpp
        src/directory/block/block_allocation_service.h
        src/directory/block/block_allocation_service.tcc
        src/directory/block/block_allocation_service_constants.cpp
        src/directory/block/block_allocation_service_constants.h
        src/directory/block/block_allocation_service_factory.cpp
        src/directory/block/block_allocation_service_factory.h
        src/directory/block/block_allocation_service_handler.cpp
        src/directory/block/block_allocation_service_handler.h
        src/directory/block/block_allocation_service_types.cpp
        src/directory/block/block_allocation_service_types.h
        src/directory/block/block_allocation_service_types.tcc
        src/directory/block/block_allocation_server.cpp
        src/directory/block/block_allocation_server.h
        src/directory/block/block_allocator.h
        src/directory/block/file_size_tracker.cpp
        src/directory/block/file_size_tracker.h
        src/directory/block/random_block_allocator.cpp
        src/directory/block/random_block_allocator.h
        src/directory/fs/directory_client.cpp
        src/directory/fs/directory_client.h
        src/directory/fs/directory_server.cpp
        src/directory/fs/directory_server.h
        src/directory/fs/directory_service.cpp
        src/directory/fs/directory_service.h
        src/directory/fs/directory_service.tcc
        src/directory/fs/directory_service_constants.cpp
        src/directory/fs/directory_service_constants.h
        src/directory/fs/directory_service_factory.cpp
        src/directory/fs/directory_service_factory.h
        src/directory/fs/directory_service_handler.cpp
        src/directory/fs/directory_service_handler.h
        src/directory/fs/directory_service_types.cpp
        src/directory/fs/directory_service_types.h
        src/directory/fs/directory_service_types.tcc
        src/directory/fs/directory_tree.cpp
        src/directory/fs/directory_tree.h
        src/directory/fs/directory_type_conversions.h
        src/directory/lease/directory_lease_client.cpp
        src/directory/lease/directory_lease_client.h
        src/directory/lease/directory_lease_server.cpp
        src/directory/lease/directory_lease_server.h
        src/directory/lease/directory_lease_service.cpp
        src/directory/lease/directory_lease_service.h
        src/directory/lease/directory_lease_service.tcc
        src/directory/lease/directory_lease_service_constants.cpp
        src/directory/lease/directory_lease_service_constants.h
        src/directory/lease/directory_lease_service_factory.cpp
        src/directory/lease/directory_lease_service_factory.h
        src/directory/lease/directory_lease_service_handler.cpp
        src/directory/lease/directory_lease_service_handler.h
        src/directory/lease/directory_lease_service_types.cpp
        src/directory/lease/directory_lease_service_types.h
        src/directory/lease/directory_lease_service_types.tcc
        src/directory/lease/lease_expiry_worker.cpp
        src/directory/lease/lease_expiry_worker.h
        src/directory/directory_ops.h
        src/directory/directory_ops.cpp
        src/storage/kv/serializer/csv_serde.cpp
        src/storage/kv/serializer/csv_serde.h
        src/storage/kv/serializer/serde.h
        src/storage/kv/serializer/binary_serde.cpp
        src/storage/kv/serializer/binary_serde.h
        src/storage/kv/kv_block.cpp
        src/storage/kv/kv_block.h
        src/storage/kv/kv_hash.h
        src/storage/block.h
        src/storage/chain_module.h
        src/storage/chain_module.cpp
        src/storage/service/block_chain_client.cpp
        src/storage/service/block_chain_client.h
        src/storage/service/block_response_client.cpp
        src/storage/service/block_response_client.h
        src/storage/service/block_server.cpp
        src/storage/service/block_server.h
        src/storage/service/block_client.cpp
        src/storage/service/block_client.h
        src/storage/service/block_request_handler.cpp
        src/storage/service/block_request_handler.h
        src/storage/service/chain_request_handler_factory.cpp
        src/storage/service/chain_request_handler_factory.h
        src/storage/service/chain_request_handler.cpp
        src/storage/service/chain_request_handler.h
        src/storage/service/chain_request_client.cpp
        src/storage/service/chain_request_client.h
        src/storage/service/chain_response_client.cpp
        src/storage/service/chain_response_client.h
        src/storage/service/block_response_client_map.cpp
        src/storage/service/block_response_client_map.h
        src/storage/service/block_request_handler_factory.cpp
        src/storage/service/block_request_handler_factory.h
        src/storage/service/block_request_service.cpp
        src/storage/service/block_request_service.h
        src/storage/service/block_request_service.tcc
        src/storage/service/block_response_service.cpp
        src/storage/service/block_response_service.h
        src/storage/service/block_response_service.tcc
        src/storage/service/block_service_constants.cpp
        src/storage/service/block_service_constants.h
        src/storage/service/block_service_types.cpp
        src/storage/service/block_service_types.h
        src/storage/service/block_service_types.tcc
        src/storage/service/chain_request_service.cpp
        src/storage/service/chain_request_service.h
        src/storage/service/chain_request_service.tcc
        src/storage/service/chain_response_service.cpp
        src/storage/service/chain_response_service.h
        src/storage/service/chain_response_service.tcc
        src/storage/service/chain_server.cpp
        src/storage/service/chain_server.h
        src/storage/manager/detail/block_name_parser.cpp
        src/storage/manager/detail/block_name_parser.h
        src/storage/manager/storage_management_client.cpp
        src/storage/manager/storage_management_client.h
        src/storage/manager/storage_management_server.cpp
        src/storage/manager/storage_management_server.h
        src/storage/manager/storage_management_service.cpp
        src/storage/manager/storage_management_service.h
        src/storage/manager/storage_management_service.tcc
        src/storage/manager/storage_management_service_constants.cpp
        src/storage/manager/storage_management_service_constants.h
        src/storage/manager/storage_management_service_factory.cpp
        src/storage/manager/storage_management_service_factory.h
        src/storage/manager/storage_management_service_handler.cpp
        src/storage/manager/storage_management_service_handler.h
        src/storage/manager/storage_management_service_types.cpp
        src/storage/manager/storage_management_service_types.h
        src/storage/manager/storage_management_service_types.tcc
        src/storage/manager/storage_manager.cpp
        src/storage/manager/storage_manager.h
        src/storage/notification/blocking_queue.h
        src/storage/notification/notification_server.cpp
        src/storage/notification/notification_server.h
        src/storage/notification/notification_service.cpp
        src/storage/notification/notification_service.h
        src/storage/notification/notification_service.tcc
        src/storage/notification/notification_service_constants.cpp
        src/storage/notification/notification_service_constants.h
        src/storage/notification/notification_service_factory.cpp
        src/storage/notification/notification_service_factory.h
        src/storage/notification/notification_service_handler.cpp
        src/storage/notification/notification_service_handler.h
        src/storage/notification/notification_service_types.cpp
        src/storage/notification/notification_service_types.h
        src/storage/notification/notification_service_types.tcc
        src/storage/notification/subscriber.cpp
        src/storage/notification/subscriber.h
        src/storage/notification/subscription_map.cpp
        src/storage/notification/subscription_map.h
        src/storage/notification/subscription_service.cpp
        src/storage/notification/subscription_service.h
        src/storage/notification/subscription_service.tcc
        src/storage/notification/subscription_service_constants.cpp
        src/storage/notification/subscription_service_constants.h
        src/storage/notification/subscription_service_types.cpp
        src/storage/notification/subscription_service_types.h
        src/storage/notification/subscription_service_types.tcc
        src/storage/storage_management_ops.h
        src/persistent/local/local_store.cpp
        src/persistent/local/local_store.h
        src/persistent/s3/s3_store.cpp
        src/persistent/s3/s3_store.h
        src/persistent/persistent_service.h
        src/utils/cmd_parse.h
        src/utils/directory_utils.h
        src/utils/event.h
        src/utils/logger.h
        src/utils/logger.cpp
        src/utils/rand_utils.h
        src/utils/signal_handling.h
        src/utils/time_utils.h
        src/utils/retry_utils.h)
if (GENERATE_THRIFT)
  add_dependencies(elasticmem thriftgen_directory thriftgen_kv)
endif ()

if (NOT USE_SYSTEM_THRIFT)
  add_dependencies(elasticmem thrift)
endif ()

if (NOT USE_SYSTEM_AWS_SDK)
  add_dependencies(elasticmem awssdk)
endif ()

if (NOT USE_SYSTEM_LIBCUCKOO)
  add_dependencies(elasticmem libcuckoo)
endif ()

target_link_libraries(elasticmem ${THRIFT_LIBRARIES} ${AWS_LIBRARIES} ${CMAKE_THREAD_LIBS_INIT})
set_target_properties(elasticmem PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})
  add_executable(elasticmem_tests
          test/elasticmem_tests.cpp
          test/block_allocation_service_test.cpp
          test/block_allocator_test.cpp
          test/chain_replication_test.cpp
          test/directory_lease_test.cpp
          test/directory_tree_test.cpp
          test/directory_service_test.cpp
          test/file_size_tracker_test.cpp
          test/kv_block_test.cpp
          test/kv_rpc_test.cpp
          test/lease_expiry_worker_test.cpp
          test/local_store_test.cpp
          test/notification_test.cpp
          test/storage_manager_test.cpp
          test/test_utils.h)
  target_link_libraries(elasticmem_tests elasticmem)
  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(elasticmem_tests catch)
  endif ()
endif ()

if (BUILD_BENCHMARKS)
  add_executable(kv_rpc_benchmark bench/kv_rpc_benchmark.cpp)
  target_link_libraries(kv_rpc_benchmark elasticmem)
endif ()

install(DIRECTORY src/
        DESTINATION include
        FILES_MATCHING PATTERN "*.h")

install(TARGETS elasticmem
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)