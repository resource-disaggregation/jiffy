# Find Memkind library
#
# MEMKIND_INCLUDE_DIR - where to find memkind.h, etc.
# MEMKIND_LIBRARY - Memkind library.
# MEMKIND_FOUND - True if memkind found.

find_path(MEMKIND_INCLUDE_DIR
        NAMES memkind.h
        HINTS ${MEMKIND_ROOT_DIR}/include)

find_library(MEMKIND_LIBRARY
        NAMES memkind
        HINTS ${MEMKIND_ROOT_DIR}/lib)

include(FindPackageHandleStandardArgs)
find_package_handle_standard_args(memkind DEFAULT_MSG MEMKIND_LIBRARY MEMKIND_INCLUDE_DIR)

mark_as_advanced(
        MEMKIND_LIBRARY
        MEMKIND_INCLUDE_DIR)
