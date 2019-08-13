function(thrift_gen lang thrift_file output_dir)
  set(thrift_file_abs "${CMAKE_CURRENT_SOURCE_DIR}/${thrift_file}")
  set(output_dir_abs "${CMAKE_CURRENT_SOURCE_DIR}/${output_dir}")
  if (EXISTS ${thrift_file_abs})
    if (NOT EXISTS ${output_dir_abs})
      message(FATAL_ERROR "Error: Output directory ${output_dir_abs} does not exist for thrift generated files")
    endif ()
    message(STATUS "Generating sources using ${thrift_file_abs} to ${output_dir_abs}")
    exec_program(${THRIFT_COMPILER}
            ARGS -out "${output_dir_abs}" --gen ${lang} ${thrift_file_abs}
            OUTPUT_VARIABLE __thrift_OUT
            RETURN_VALUE THRIFT_RETURN)
  else ()
    message(FATAL_ERROR "Error: Cannot generate files from ${thrift_file_abs} since it does not exist")
  endif ()
endfunction()

set(THRIFT_GEN_CXX "cpp:templates,pure_enums,no_skeleton,include_prefix")
set(THRIFT_GEN_PY "py:no_utf8strings,slots")
set(THRIFT_GEN_JAVA "java:fullcamel,reuse-objects,generated_annotations=undated,unsafe_binaries")

# Generate cpp sources
thrift_gen(${THRIFT_GEN_CXX} src/block_registration_service.thrift ../libjiffy/src/jiffy/directory/block/)
thrift_gen(${THRIFT_GEN_CXX} src/block_service.thrift ../libjiffy/src/jiffy/storage/service/)
thrift_gen(${THRIFT_GEN_CXX} src/directory_service.thrift ../libjiffy/src/jiffy/directory/fs/)
thrift_gen(${THRIFT_GEN_CXX} src/lease_service.thrift ../libjiffy/src/jiffy/directory/lease/)
thrift_gen(${THRIFT_GEN_CXX} src/storage_management_service.thrift ../libjiffy/src/jiffy/storage/manager/)
thrift_gen(${THRIFT_GEN_CXX} src/auto_scaling_service.thrift ../libjiffy/src/jiffy/auto_scaling/)


# Generate py sources
thrift_gen(${THRIFT_GEN_PY} src/block_service.thrift ../pyjiffy/)
thrift_gen(${THRIFT_GEN_PY} src/directory_service.thrift ../pyjiffy/)
thrift_gen(${THRIFT_GEN_PY} src/lease_service.thrift ../pyjiffy/)

# Remove unwanted generated files
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/__init__.py)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/storage/chain_request_service.py)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/storage/chain_response_service.py)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/directory/directory_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/storage/block_request_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/storage/block_response_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/storage/chain_request_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/storage/chain_response_service-remote)
file(REMOVE ${CMAKE_CURRENT_SOURCE_DIR}/../pyjiffy/jiffy/lease/lease_service-remote)

# Generate java sources
thrift_gen(${THRIFT_GEN_JAVA} src/block_service.thrift ../jiffy4j/core/src/main/java/)
thrift_gen(${THRIFT_GEN_JAVA} src/directory_service.thrift ../jiffy4j/core/src/main/java/)
thrift_gen(${THRIFT_GEN_JAVA} src/lease_service.thrift ../jiffy4j/core/src/main/java/)