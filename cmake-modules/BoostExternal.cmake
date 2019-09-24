# Boost external project
# target:
#  - boost_ep
# defines:
#  - Boost_INCLUDE_DIR
#  - Boost_<C>_LIBRARY (for each component <C>)
#  - Boost_LIBRARIES

set(Boost_VERSION 1.69.0)
set(Boost_BUILD ON)
set(Boost_COMPONENTS "program_options")

if (DEFINED ENV{Boost_ROOT} AND EXISTS $ENV{Boost_ROOT})
  set(Boost_USE_STATIC_LIBS ON)
  set(Boost_ROOT "$ENV{Boost_ROOT}")
  find_package(Boost ${Boost_VERSION} COMPONENTS ${Boost_COMPONENTS})
  if (Boost_FOUND)
    set(Boost_BUILD OFF)
    set(Boost_INCLUDE_DIR ${Boost_INCLUDE_DIRS})
    get_filename_component(Boost_HOME ${Boost_INCLUDE_DIR} DIRECTORY)
    add_custom_target(boost_ep)
  endif (Boost_FOUND)
endif ()

if (Boost_BUILD)
  string(REGEX REPLACE "\\." "_" Boost_VERSION_STR ${Boost_VERSION})
  set(Boost_PREFIX "${PROJECT_BINARY_DIR}/external/boost_ep")
  set(Boost_HOME ${Boost_PREFIX})
  set(Boost_INCLUDE_DIRS "${Boost_PREFIX}/include")
  set(Boost_INCLUDE_DIR "${Boost_INCLUDE_DIRS}")
  foreach (component ${Boost_COMPONENTS})
    list(APPEND Boost_COMPONENT_FLAGS --with-${component})
  endforeach ()
  ExternalProject_Add(boost_ep
          URL https://downloads.sourceforge.net/project/boost/boost/${Boost_VERSION}/boost_${Boost_VERSION_STR}.zip
          UPDATE_COMMAND ""
          CONFIGURE_COMMAND ./bootstrap.sh --prefix=${Boost_PREFIX}
          BUILD_COMMAND ./b2 link=static variant=release cxxflags=-fPIC cflags=-fPIC --prefix=${Boost_PREFIX} ${Boost_COMPONENT_FLAGS} install
          BUILD_IN_SOURCE 1
          INSTALL_COMMAND ""
          INSTALL_DIR ${Boost_PREFIX}
          LOG_DOWNLOAD ON
          LOG_CONFIGURE ON
          LOG_BUILD ON
          LOG_INSTALL ON)

  macro(libraries_to_fullpath out)
    set(${out})
    foreach (comp ${Boost_COMPONENTS})
      string(TOUPPER ${comp} comp_upper)
      set(Boost_${comp_upper}_LIBRARY ${Boost_PREFIX}/lib/${CMAKE_STATIC_LIBRARY_PREFIX}boost_${comp}${CMAKE_STATIC_LIBRARY_SUFFIX})
      list(APPEND ${out} Boost_${comp_upper}_LIBRARY)
    endforeach ()
  endmacro()
  libraries_to_fullpath(Boost_LIBRARIES)
endif ()

include_directories(SYSTEM ${Boost_INCLUDE_DIR})
message(STATUS "Boost include dirs: ${Boost_INCLUDE_DIR}")
message(STATUS "Boost program options library: ${Boost_PROGRAM_OPTIONS_LIBRARY}")
