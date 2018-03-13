add_library(persistent STATIC
        src/persistent_service.h
        src/local/local_store.cpp
        src/local/local_store.h )

set_target_properties(persistent PROPERTIES LINKER_LANGUAGE CXX)

if (BUILD_TESTS)
  include_directories(${CATCH_INCLUDE_DIR})

  add_executable(persistent_tests
          test/local_store_test.cpp
          test/persistent_tests.cpp)
  if (NOT USE_SYSTEM_CATCH)
    add_dependencies(persistent_tests catch)
  endif()
  target_link_libraries(persistent_tests persistent)
endif ()