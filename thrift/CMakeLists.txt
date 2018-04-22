set(THRIFTGEN_CXX_OPTS "templates,pure_enums,no_skeleton,include_prefix")
set(THRIFTGEN_PY_OPTS "no_utf8strings,slots")
set(THRIFTGEN_JAVA_OPTS "fullcamel,handle_runtime_exceptions,generated_annotations=undated")

set(DIRECTORY_INPUT_FILE directory_service.thrift)
set(DIRECTORY_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/libdirectory/src/rpc)
set(DIRECTORY_OUTPUT_SOURCES
        ${DIRECTORY_OUTPUT_PATH}/directory_rpc_service.cpp
        ${DIRECTORY_OUTPUT_PATH}/directory_rpc_service.h
        ${DIRECTORY_OUTPUT_PATH}/directory_rpc_service.tcc
        ${DIRECTORY_OUTPUT_PATH}/directory_service_constants.cpp
        ${DIRECTORY_OUTPUT_PATH}/directory_service_constants.h
        ${DIRECTORY_OUTPUT_PATH}/directory_service_types.cpp
        ${DIRECTORY_OUTPUT_PATH}/directory_service_types.h
        ${DIRECTORY_OUTPUT_PATH}/directory_service_types.tcc)
add_custom_command(
        OUTPUT ${DIRECTORY_OUTPUT_SOURCES}
        DEPENDS thriftcompiler ${DIRECTORY_INPUT_FILE}
        COMMAND thriftcompiler --gen cpp:${THRIFTGEN_CXX_OPTS} ${DIRECTORY_INPUT_FILE}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Generating Thrift files for libdirectory"
        VERBATIM
)
add_custom_target(thriftgen_directory ALL
        DEPENDS ${DIRECTORY_OUTPUT_SOURCES})

set(KV_INPUT_FILE kv_service.thrift)
set(KV_OUTPUT_PATH ${CMAKE_SOURCE_DIR}/libkv/src/rpc)
set(KV_OUTPUT_SOURCES
        ${KV_OUTPUT_PATH}/kv_management_rpc_service.cpp
        ${KV_OUTPUT_PATH}/kv_management_rpc_service.h
        ${KV_OUTPUT_PATH}/kv_management_rpc_service.tcc
        ${KV_OUTPUT_PATH}/kv_rpc_service.cpp
        ${KV_OUTPUT_PATH}/kv_rpc_service.h
        ${KV_OUTPUT_PATH}/kv_rpc_service.tcc
        ${KV_OUTPUT_PATH}/kv_service_constants.cpp
        ${KV_OUTPUT_PATH}/kv_service_constants.h
        ${KV_OUTPUT_PATH}/kv_service_types.cpp
        ${KV_OUTPUT_PATH}/kv_service_types.h
        ${KV_OUTPUT_PATH}/kv_service_types.tcc)
add_custom_command(
        OUTPUT ${KV_OUTPUT_SOURCES}
        DEPENDS thriftcompiler ${KV_INPUT_FILE}
        COMMAND thriftcompiler --gen cpp:${THRIFTGEN_CXX_OPTS} ${KV_INPUT_FILE}
        WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
        COMMENT "Generating Thrift files for libkv"
        VERBATIM
)
add_custom_target(thriftgen_kv ALL
        DEPENDS ${KV_OUTPUT_SOURCES})