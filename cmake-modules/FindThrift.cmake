# - Find Thrift (a cross platform RPC lib/tool)
# This module defines
#  THRIFT_VERSION_STRING, version string of Thrift if found
#  THRIFT_LIBRARY, libraries to link
#  THRIFT_NB_LIBRARY, non-blocking libraries to link
#  THRIFT_INCLUDE_DIR, where to find THRIFT headers
#  THRIFT_COMPILER, thrift compiler executable
#  THRIFT_FOUND, If false, do not try to use Thrift
#
# Initial work was done by Cloudera https://github.com/cloudera/Impala
# 2014 - modified by snikulov

# prefer the thrift version supplied in THRIFT_HOME (cmake -DTHRIFT_HOME then environment)
find_path(THRIFT_INCLUDE_DIR
        NAMES
        thrift/Thrift.h
        HINTS
        ${THRIFT_HOME}
        ENV THRIFT_HOME
        /usr/local/Cellar
        /usr/local
        /opt/local
        /usr
        PATH_SUFFIXES
        include
        )

# prefer the thrift version supplied in THRIFT_HOME
find_library(THRIFTNB_LIBRARY
        NAMES
        thriftnb libthriftnb
        HINTS
        ${THRIFT_HOME}
        ENV THRIFT_HOME
        /usr/local/Cellar
        /usr/local
        /opt/local
        /usr
        PATH_SUFFIXES
        lib lib64
        )

# prefer the thrift version supplied in THRIFT_HOME
find_library(THRIFT_LIBRARY
        NAMES
        thrift libthrift
        HINTS
        ${THRIFT_HOME}
        ENV THRIFT_HOME
        /usr/local/Cellar
        /usr/local
        /opt/local
        /usr
        PATH_SUFFIXES
        lib lib64)

find_library(THRIFTNB_LIBRARY
        NAMES
        thriftnb libthriftnb
        HINTS
        ${THRIFT_HOME}
        ENV THRIFT_HOME
        /usr/local/Cellar
        /usr/local
        /opt/local
        /usr
        PATH_SUFFIXES
        lib lib64)

find_program(THRIFT_COMPILER
        NAMES
        thrift
        HINTS
        ${THRIFT_HOME}
        ENV THRIFT_HOME
        /usr/local/Cellar
        /usr/local
        /opt/local
        /usr
        PATH_SUFFIXES
        bin bin64)

if (THRIFT_COMPILER)
  exec_program(${THRIFT_COMPILER}
          ARGS -version OUTPUT_VARIABLE __thrift_OUT RETURN_VALUE THRIFT_RETURN)
  string(REGEX MATCH "[0-9]+.[0-9]+.[0-9]+-[a-z]+$" THRIFT_VERSION_STRING ${__thrift_OUT})
endif ()


include(FindPackageHandleStandardArgs)
FIND_PACKAGE_HANDLE_STANDARD_ARGS(THRIFT DEFAULT_MSG THRIFT_LIBRARY THRIFTNB_LIBRARY THRIFT_INCLUDE_DIR THRIFT_COMPILER)
mark_as_advanced(THRIFT_LIBRARY THRIFTNB_LIBRARY THRIFT_INCLUDE_DIR THRIFT_COMPILER THRIFT_VERSION_STRING)