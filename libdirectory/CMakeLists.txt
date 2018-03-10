add_library(directory STATIC
        src/directory_service.h
        src/directory_service.cpp
        src/utils/directory_utils.h
        src/tree/directory_tree.h
        src/tree/directory_tree.cpp
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
        src/rpc/directory_lease_server.h)
if (GENERATE_THRIFT)
  add_dependencies(directory thriftgen_directory)
endif ()
target_link_libraries(directory ${THRIFT_LIBRARIES})
set_target_properties(directory PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${THRIFT_INCLUDE_DIR})
  include_directories(${CATCH_INCLUDE_DIR})
  include_directories(${Boost_INCLUDE_DIRS})

  add_executable(ds_tree_test test/directory_tree_test.cpp)
  target_link_libraries(ds_tree_test directory)

  add_executable(ds_rpc_test test/directory_rpc_service_test.cpp)
  target_link_libraries(ds_rpc_test directory)

  add_executable(ds_lease_test test/directory_lease_service_test.cpp)
  target_link_libraries(ds_lease_test directory)

  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(ds_tree_test catch)
    add_dependencies(ds_rpc_test catch)
    add_dependencies(ds_lease_test catch)
  endif ()
endif ()