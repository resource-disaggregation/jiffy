include_directories(../libpersistent/src)
add_library(kv
        src/kv_management_service.h
        src/block/kv_block.cpp
        src/block/kv_block.h
        src/rpc/kv_management_client.cpp
        src/rpc/kv_management_client.h
        src/rpc/kv_management_rpc_server.cpp
        src/rpc/kv_management_rpc_server.h
        src/rpc/kv_management_rpc_service_handler.cpp
        src/rpc/kv_management_rpc_service_handler.h
        src/rpc/kv_management_rpc_service_factory.cpp
        src/rpc/kv_management_rpc_service_factory.h
        src/rpc/kv_management_rpc_service.cpp
        src/rpc/kv_management_rpc_service.h
        src/rpc/kv_management_rpc_service.tcc
        src/rpc/kv_client.cpp
        src/rpc/kv_client.h
        src/rpc/kv_rpc_server.cpp
        src/rpc/kv_rpc_server.h
        src/rpc/kv_rpc_service_handler.cpp
        src/rpc/kv_rpc_service_handler.h
        src/rpc/kv_rpc_service_factory.cpp
        src/rpc/kv_rpc_service_factory.h
        src/rpc/kv_rpc_service.cpp
        src/rpc/kv_rpc_service.h
        src/rpc/kv_rpc_service.tcc
        src/rpc/kv_service_constants.cpp
        src/rpc/kv_service_constants.h
        src/rpc/kv_service_types.cpp
        src/rpc/kv_service_types.h
        src/rpc/kv_service_types.tcc
        src/serializer/csv_serde.cpp
        src/serializer/csv_serde.h
        src/serializer/serde.h
        src/serializer/binary_serde.cpp
        src/serializer/binary_serde.h
        src/manager/kv_manager.cpp
        src/manager/kv_manager.h
        src/manager/block_name_parser.cpp
        src/manager/block_name_parser.h)
if (GENERATE_THRIFT)
  add_dependencies(kv thriftgen_kv)
endif ()
target_link_libraries(kv persistent ${THRIFT_LIBRARIES})
set_target_properties(kv PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})

  add_executable(kv_tests
          test/kv_block_test.cpp
          test/kv_manager_test.cpp
          test/kv_rpc_test.cpp
          test/kv_tests.cpp)
  target_link_libraries(kv_tests kv)
  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(kv_tests catch)
  endif()
endif ()