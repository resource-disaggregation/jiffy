if (NOT APPLE AND UNIX)
  set(CMAKE_EXE_LINKER_FLAGS "${CMAKE_EXE_LINKER_FLAGS} -Wl,--no-as-needed -ldl")
endif ()

include_directories(${CMAKE_CURRENT_SOURCE_DIR}/../libjiffy/src
        ${CMAKE_CURRENT_SOURCE_DIR}/../thirdparty/libcuckoo
        ${Boost_INCLUDE_DIRS})
add_executable(directoryd src/directory_server.cpp)

if (BUILD_MEMKIND_SUPPORT)
  add_dependencies(directoryd boost_ep memkind_ep thrift_ep)
  target_link_libraries(directoryd jiffy ${MEMKIND_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})
else()
  add_dependencies(directoryd boost_ep jemalloc_ep thrift_ep)
  target_link_libraries(directoryd jiffy ${JEMALLOC_LIBRARY} ${Boost_PROGRAM_OPTIONS_LIBRARY})
endif()

install(TARGETS directoryd
        RUNTIME DESTINATION bin
        ARCHIVE DESTINATION lib
        LIBRARY DESTINATION lib)