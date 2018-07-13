function(thrift_gen lang thrift_file output_dir)
  set(thrift_file_abs "${CMAKE_CURRENT_SOURCE_DIR}/${thrift_file}")
  set(output_dir_abs "${CMAKE_CURRENT_SOURCE_DIR}/${output_dir}")
  if (EXISTS ${thrift_file_abs})
    if (NOT EXISTS ${output_dir_abs})
      message(FATAL_ERROR "Error: Output directory ${output_dir_abs} does not exist for thrift generated files")
    endif ()
    message(STATUS "Generating sources using ${thrift_file_abs} to ${output_dir_abs}")
    execute_process(
            COMMAND ${THRIFT_COMPILER} -out "${output_dir_abs}" --gen ${lang} ${thrift_file_abs}
            OUTPUT_VARIABLE __thrift_OUT
            RESULT_VARIABLE THRIFT_RETURN)
  else ()
    message(FATAL_ERROR "Error: Cannot generate files from ${thrift_file_abs} since it does not exist")
  endif ()
endfunction()

set(THRIFT_GEN_CXX "cpp:templates,pure_enums,no_skeleton,include_prefix")
set(THRIFT_GEN_PY "py:no_utf8strings,slots")
set(THRIFT_GEN_JAVA "java:fullcamel,handle_runtime_exceptions,generated_annotations=undated")

# Generate cpp sources
thrift_gen(${THRIFT_GEN_CXX} src/block_allocation_service.thrift ../libmmux/src/mmux/directory/block/)
thrift_gen(${THRIFT_GEN_CXX} src/block_service.thrift ../libmmux/src/mmux/storage/service/)
thrift_gen(${THRIFT_GEN_CXX} src/directory_service.thrift ../libmmux/src/mmux/directory/fs/)
thrift_gen(${THRIFT_GEN_CXX} src/heartbeat_service.thrift ../libmmux/src/mmux/directory/heartbeat/)
thrift_gen(${THRIFT_GEN_CXX} src/lease_service.thrift ../libmmux/src/mmux/directory/lease/)
thrift_gen(${THRIFT_GEN_CXX} src/notification_service.thrift ../libmmux/src/mmux/storage/notification/)
thrift_gen(${THRIFT_GEN_CXX} src/storage_management_service.thrift ../libmmux/src/mmux/storage/manager/)
thrift_gen(${THRIFT_GEN_CXX} src/subscription_service.thrift ../libmmux/src/mmux/storage/notification/)

# Generate py sources
thrift_gen(${THRIFT_GEN_PY} src/block_service.thrift ../pyclient/)
thrift_gen(${THRIFT_GEN_PY} src/directory_service.thrift ../pyclient/)
thrift_gen(${THRIFT_GEN_PY} src/lease_service.thrift ../pyclient/)
thrift_gen(${THRIFT_GEN_PY} src/notification_service.thrift ../pyclient/)
thrift_gen(${THRIFT_GEN_PY} src/subscription_service.thrift ../pyclient/)
# Remove unwanted generated files
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/__init__.py)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/kv/chain_request_service.py)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/kv/chain_response_service.py)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/directory/directory_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/kv/block_request_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/kv/block_response_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/kv/chain_request_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/kv/chain_response_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/lease/lease_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/notification/notification_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyclient/mmux/subscription/subscription_service-remote)

# Generate java sources
thrift_gen(${THRIFT_GEN_JAVA} src/block_service.thrift ../javaclient/core/src/main/java/)
thrift_gen(${THRIFT_GEN_JAVA} src/directory_service.thrift ../javaclient/core/src/main/java/)
thrift_gen(${THRIFT_GEN_JAVA} src/lease_service.thrift ../javaclient/core/src/main/java/)
thrift_gen(${THRIFT_GEN_JAVA} src/notification_service.thrift ../javaclient/core/src/main/java/)
thrift_gen(${THRIFT_GEN_JAVA} src/subscription_service.thrift ../javaclient/core/src/main/java/)