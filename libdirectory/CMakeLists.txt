add_library(directory STATIC
        src/directory_service.h
        src/directory_service.cpp
        src/utils/directory_utils.h
        src/shard/directory_service_shard.h
        src/shard/directory_service_shard.cpp
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
        src/rpc/directory_lease_service_handler.h src/rpc/directory_lease_service_factory.cpp src/rpc/directory_lease_service_factory.h src/rpc/directory_lease_server.cpp src/rpc/directory_lease_server.h)
if (GENERATE_THRIFT)
  add_dependencies(directory thriftgen_directory)
endif ()
target_link_libraries(directory ${THRIFT_LIBRARIES})
set_target_properties(directory PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${THRIFT_INCLUDE_DIR})
  include_directories(${CATCH_INCLUDE_DIR})
  include_directories(${Boost_INCLUDE_DIRS})

  add_executable(ds_shard_test test/directory_service_shard_test.cpp)
  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(ds_shard_test catch)
  endif ()
  target_link_libraries(ds_shard_test directory)
  set_target_properties(ds_shard_test PROPERTIES LINKER_LANGUAGE CXX)

  add_executable(ds_rpc_test test/directory_rpc_service_handler_test.cpp)
  add_dependencies(ds_rpc_test catch)
  target_link_libraries(ds_rpc_test directory)
endif ()