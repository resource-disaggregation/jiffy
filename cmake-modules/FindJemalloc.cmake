# Find Jemalloc library
#
# JEMALLOC_INCLUDE_DIR - where to find jemalloc.h, etc.
# JEMALLOC_LIBRARY - Jemalloc library.
# JEMALLOC_FOUND - True if jemalloc found.

find_path(JEMALLOC_INCLUDE_DIR
        NAMES jemalloc/jemalloc.h
        HINTS ${JEMALLOC_ROOT_DIR}/include)

find_library(JEMALLOC_LIBRARY
        NAMES jemalloc
        HINTS ${JEMALLOC_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(jemalloc DEFAULT_MSG JEMALLOC_LIBRARY JEMALLOC_INCLUDE_DIR)

mark_as_advanced(
        JEMALLOC_LIBRARY
        JEMALLOC_INCLUDE_DIR)