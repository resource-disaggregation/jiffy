add_library(persistent STATIC
        src/persistent_service.h
        src/local/local_store.cpp
        src/local/local_store.h)

set_target_properties(persistent PROPERTIES LINKER_LANGUAGE CXX)