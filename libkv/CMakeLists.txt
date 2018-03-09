add_library(kv
        src/kv_service.h
        src/shard/kv_service_shard.cpp
        src/shard/kv_service_shard.h
        src/rpc/kv_management_rpc_service.cpp
        src/rpc/kv_management_rpc_service.h
        src/rpc/kv_management_rpc_service.tcc
        src/rpc/kv_rpc_service.cpp
        src/rpc/kv_rpc_service.h
        src/rpc/kv_rpc_service.tcc
        src/rpc/kv_service_constants.cpp
        src/rpc/kv_service_constants.h
        src/rpc/kv_service_types.cpp
        src/rpc/kv_service_types.h
        src/rpc/kv_service_types.tcc)
if (GENERATE_THRIFT)
  add_dependencies(kv thriftgen_kv)
endif ()
set_target_properties(kv PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})

  add_executable(kv_shard_test test/kv_service_shard_test.cpp)
  add_dependencies(kv_shard_test catch)
  target_link_libraries(kv_shard_test kv)

  add_executable(kv_rpc_test test/kv_rpc_service_handler_test.cpp)
  add_dependencies(kv_rpc_test catch)
  target_link_libraries(kv_rpc_test kv thriftstatic)

  add_executable(kv_rpc_management_test test/kv_rpc_management_service_handler_test.cpp)
  add_dependencies(kv_rpc_management_test catch)
  target_link_libraries(kv_rpc_management_test kv thriftstatic)
endif ()