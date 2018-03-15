include_directories(${THRIFT_INCLUDE_DIR})
add_library(elasticmem STATIC
        src/directory/block/block_allocator.h
        src/directory/block/block_allocation_service.cpp
        src/directory/block/block_allocation_service.h
        src/directory/block/block_allocation_service.tcc
        src/directory/block/block_allocation_service_constants.cpp
        src/directory/block/block_allocation_service_constants.h
        src/directory/block/block_allocation_service_types.cpp
        src/directory/block/block_allocation_service_types.h
        src/directory/block/block_allocation_service_types.tcc
        src/directory/block/random_block_allocator.cpp
        src/directory/block/random_block_allocator.h
        src/directory/fs/directory_client.cpp
        src/directory/fs/directory_client.h
        src/directory/fs/directory_rpc_server.cpp
        src/directory/fs/directory_rpc_server.h
        src/directory/fs/directory_rpc_service.cpp
        src/directory/fs/directory_rpc_service.h
        src/directory/fs/directory_rpc_service.tcc
        src/directory/fs/directory_rpc_service_constants.cpp
        src/directory/fs/directory_rpc_service_constants.h
        src/directory/fs/directory_rpc_service_factory.cpp
        src/directory/fs/directory_rpc_service_factory.h
        src/directory/fs/directory_rpc_service_handler.cpp
        src/directory/fs/directory_rpc_service_handler.h
        src/directory/fs/directory_rpc_service_types.cpp
        src/directory/fs/directory_rpc_service_types.h
        src/directory/fs/directory_rpc_service_types.tcc
        src/directory/fs/directory_tree.cpp
        src/directory/fs/directory_tree.h
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
        src/directory/lease/lease_manager.cpp
        src/directory/lease/lease_manager.h
        src/directory/directory_service.h
        src/directory/directory_service.cpp
        src/storage/kv/serializer/csv_serde.cpp
        src/storage/kv/serializer/csv_serde.h
        src/storage/kv/serializer/serde.h
        src/storage/kv/serializer/binary_serde.cpp
        src/storage/kv/serializer/binary_serde.h
        src/storage/kv/kv_block.cpp
        src/storage/kv/kv_block.h
        src/storage/manager/detail/block_name_parser.cpp
        src/storage/manager/detail/block_name_parser.h
        src/storage/manager/storage_management_client.cpp
        src/storage/manager/storage_management_client.h
        src/storage/manager/storage_management_rpc_server.cpp
        src/storage/manager/storage_management_rpc_server.h
        src/storage/manager/storage_management_rpc_service.cpp
        src/storage/manager/storage_management_rpc_service.h
        src/storage/manager/storage_management_rpc_service.tcc
        src/storage/manager/storage_management_rpc_service_constants.cpp
        src/storage/manager/storage_management_rpc_service_constants.h
        src/storage/manager/storage_management_rpc_service_factory.cpp
        src/storage/manager/storage_management_rpc_service_factory.h
        src/storage/manager/storage_management_rpc_service_handler.cpp
        src/storage/manager/storage_management_rpc_service_handler.h
        src/storage/manager/storage_management_rpc_service_types.cpp
        src/storage/manager/storage_management_rpc_service_types.h
        src/storage/manager/storage_management_rpc_service_types.tcc
        src/storage/manager/storage_manager.cpp
        src/storage/manager/storage_manager.h
        src/storage/kv/kv_client.cpp
        src/storage/kv/kv_client.h
        src/storage/kv/kv_rpc_server.cpp
        src/storage/kv/kv_rpc_server.h
        src/storage/kv/kv_rpc_service.cpp
        src/storage/kv/kv_rpc_service.h
        src/storage/kv/kv_rpc_service.tcc
        src/storage/kv/kv_rpc_service_constants.cpp
        src/storage/kv/kv_rpc_service_constants.h
        src/storage/kv/kv_rpc_service_factory.cpp
        src/storage/kv/kv_rpc_service_factory.h
        src/storage/kv/kv_rpc_service_handler.cpp
        src/storage/kv/kv_rpc_service_handler.h
        src/storage/kv/kv_rpc_service_types.cpp
        src/storage/kv/kv_rpc_service_types.h
        src/storage/kv/kv_rpc_service_types.tcc
        src/storage/storage_management_service.h
        src/persistent/local/local_store.cpp
        src/persistent/local/local_store.h
        src/persistent/persistent_service.h
        src/utils/cmd_parse.h
        src/utils/directory_utils.h
        src/utils/logger.h
        src/utils/rand_utils.h
        src/utils/signal_handling.h
        src/utils/time_utils.h src/storage/block_management_ops.h)
if (GENERATE_THRIFT)
  add_dependencies(elasticmem thriftgen_directory thriftgen_kv)
endif ()
target_link_libraries(elasticmem ${THRIFT_LIBRARIES})
set_target_properties(elasticmem PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})

  add_executable(elasticmem_tests
          test/elasticmem_tests.cpp
          test/block_allocator_test.cpp
          test/directory_tree_test.cpp
          test/directory_rpc_service_test.cpp
          test/lease_manager_test.cpp
          test/directory_lease_test.cpp
          test/kv_block_test.cpp
          test/storage_manager_test.cpp
          test/kv_rpc_test.cpp
          test/local_store_test.cpp
          test/test_utils.h)
  target_link_libraries(elasticmem_tests elasticmem)

  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(directory_tests catch)
  endif ()
endif ()