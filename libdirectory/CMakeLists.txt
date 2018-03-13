include_directories(../libkv/src ../libpersistent/src)

add_library(directory STATIC
        src/directory_service.h
        src/directory_service.cpp
        src/utils/directory_utils.h
        src/tree/directory_tree.h
        src/tree/directory_tree.cpp
        src/lease/lease_manager.cpp
        src/lease/lease_manager.h
        src/rpc/directory_lease_service.cpp
        src/rpc/directory_lease_service.h
        src/rpc/directory_lease_service.tcc
        src/rpc/directory_rpc_service.cpp
        src/rpc/directory_rpc_service.h
        src/rpc/directory_rpc_service.tcc
        src/rpc/directory_service_constants.cpp
        src/rpc/directory_service_constants.h
        src/rpc/directory_service_types.cpp
        src/rpc/directory_service_types.h
        src/rpc/directory_service_types.tcc
        src/rpc/directory_rpc_service_handler.cpp
        src/rpc/directory_rpc_service_handler.h
        src/rpc/directory_rpc_service_factory.cpp
        src/rpc/directory_rpc_service_factory.h
        src/rpc/directory_rpc_server.cpp
        src/rpc/directory_rpc_server.h
        src/rpc/directory_client.cpp
        src/rpc/directory_client.h
        src/rpc/directory_lease_service_handler.cpp
        src/rpc/directory_lease_service_handler.h
        src/rpc/directory_lease_service_factory.cpp
        src/rpc/directory_lease_service_factory.h
        src/rpc/directory_lease_server.cpp
        src/rpc/directory_lease_server.h src/tree/block_allocator.h src/tree/random_block_allocator.cpp src/tree/random_block_allocator.h)
if (GENERATE_THRIFT)
  add_dependencies(directory thriftgen_directory)
endif ()
target_link_libraries(directory kv ${THRIFT_LIBRARIES})
set_target_properties(directory PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${THRIFT_INCLUDE_DIR})
  include_directories(${CATCH_INCLUDE_DIR})

  add_executable(directory_tests
          test/block_allocator_test.cpp
          test/directory_tree_test.cpp
          test/directory_rpc_service_test.cpp
          test/lease_manager_test.cpp
          test/directory_tests.cpp)
  target_link_libraries(directory_tests directory)

  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(directory_tests catch)
  endif ()
endif ()