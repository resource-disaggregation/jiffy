include_directories(${THRIFT_INCLUDE_DIR})
add_library(elasticmem STATIC
        src/directory/directory_service.h
        src/directory/directory_service.cpp
        src/directory/tree/directory_tree.h
        src/directory/tree/directory_tree.cpp
        src/directory/lease/lease_manager.cpp
        src/directory/lease/lease_manager.h
        src/directory/rpc/directory_lease_service.cpp
        src/directory/rpc/directory_lease_service.h
        src/directory/rpc/directory_lease_service.tcc
        src/directory/rpc/directory_rpc_service.cpp
        src/directory/rpc/directory_rpc_service.h
        src/directory/rpc/directory_rpc_service.tcc
        src/directory/rpc/directory_service_constants.cpp
        src/directory/rpc/directory_service_constants.h
        src/directory/rpc/directory_service_types.cpp
        src/directory/rpc/directory_service_types.h
        src/directory/rpc/directory_service_types.tcc
        src/directory/rpc/directory_rpc_service_handler.cpp
        src/directory/rpc/directory_rpc_service_handler.h
        src/directory/rpc/directory_rpc_service_factory.cpp
        src/directory/rpc/directory_rpc_service_factory.h
        src/directory/rpc/directory_rpc_server.cpp
        src/directory/rpc/directory_rpc_server.h
        src/directory/rpc/directory_client.cpp
        src/directory/rpc/directory_client.h
        src/directory/rpc/directory_lease_service_handler.cpp
        src/directory/rpc/directory_lease_service_handler.h
        src/directory/rpc/directory_lease_service_factory.cpp
        src/directory/rpc/directory_lease_service_factory.h
        src/directory/rpc/directory_lease_server.cpp
        src/directory/rpc/directory_lease_server.h
        src/directory/tree/block_allocator.h
        src/directory/tree/random_block_allocator.cpp
        src/directory/tree/random_block_allocator.h
        src/directory/rpc/directory_lease_client.cpp
        src/directory/rpc/directory_lease_client.h
        src/kv/kv_management_service.h
        src/kv/block/kv_block.cpp
        src/kv/block/kv_block.h
        src/kv/rpc/kv_management_client.cpp
        src/kv/rpc/kv_management_client.h
        src/kv/rpc/kv_management_rpc_server.cpp
        src/kv/rpc/kv_management_rpc_server.h
        src/kv/rpc/kv_management_rpc_service_handler.cpp
        src/kv/rpc/kv_management_rpc_service_handler.h
        src/kv/rpc/kv_management_rpc_service_factory.cpp
        src/kv/rpc/kv_management_rpc_service_factory.h
        src/kv/rpc/kv_management_rpc_service.cpp
        src/kv/rpc/kv_management_rpc_service.h
        src/kv/rpc/kv_management_rpc_service.tcc
        src/kv/rpc/kv_client.cpp
        src/kv/rpc/kv_client.h
        src/kv/rpc/kv_rpc_server.cpp
        src/kv/rpc/kv_rpc_server.h
        src/kv/rpc/kv_rpc_service_handler.cpp
        src/kv/rpc/kv_rpc_service_handler.h
        src/kv/rpc/kv_rpc_service_factory.cpp
        src/kv/rpc/kv_rpc_service_factory.h
        src/kv/rpc/kv_rpc_service.cpp
        src/kv/rpc/kv_rpc_service.h
        src/kv/rpc/kv_rpc_service.tcc
        src/kv/rpc/kv_service_constants.cpp
        src/kv/rpc/kv_service_constants.h
        src/kv/rpc/kv_service_types.cpp
        src/kv/rpc/kv_service_types.h
        src/kv/rpc/kv_service_types.tcc
        src/kv/serializer/csv_serde.cpp
        src/kv/serializer/csv_serde.h
        src/kv/serializer/serde.h
        src/kv/serializer/binary_serde.cpp
        src/kv/serializer/binary_serde.h
        src/kv/manager/kv_manager.cpp
        src/kv/manager/kv_manager.h
        src/kv/manager/block_name_parser.cpp
        src/kv/manager/block_name_parser.h
        src/persistent/persistent_service.h
        src/persistent/local/local_store.cpp
        src/persistent/local/local_store.h
        src/utils/directory_utils.h
        src/utils/signal_handling.h
        src/utils/cmd_parse.h
        src/utils/logger.h
        src/utils/time_utils.h
        src/utils/rand_utils.h)
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
          test/kv_manager_test.cpp
          test/kv_rpc_test.cpp
          test/local_store_test.cpp
          test/test_utils.h)
  target_link_libraries(elasticmem_tests elasticmem)

  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(directory_tests catch)
  endif ()
endif ()