include_directories(../libpersistent/src)
add_library(kv
        src/kv_service.h
        src/block/kv_block.cpp
        src/block/kv_block.h
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
        src/rpc/kv_service_types.tcc
        src/rpc/kv_rpc_service_handler.cpp
        src/rpc/kv_rpc_service_handler.h
        src/serializer/csv_serde.cpp
        src/serializer/csv_serde.h
        src/serializer/serde.h
        src/serializer/binary_serde.cpp
        src/serializer/binary_serde.h)
if (GENERATE_THRIFT)
  add_dependencies(kv thriftgen_kv)
endif ()
target_link_libraries(kv persistent ${THRIFT_LIBRARIES})
set_target_properties(kv PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})

  add_executable(kv_block_test test/kv_block_test.cpp)
  add_dependencies(kv_block_test catch)
  target_link_libraries(kv_block_test kv)

  add_executable(kv_rpc_test test/kv_rpc_service_handler_test.cpp)
  add_dependencies(kv_rpc_test catch)
  target_link_libraries(kv_rpc_test kv)

  add_executable(kv_rpc_management_test test/kv_rpc_management_service_handler_test.cpp)
  add_dependencies(kv_rpc_management_test catch)
  target_link_libraries(kv_rpc_management_test kv)
endif ()