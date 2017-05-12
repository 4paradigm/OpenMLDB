cmake_minimum_required (VERSION 2.8)

project(rtidb)

set(CMAKE_BUILD_TYPE Release)
set(RTIDB_VERSION_MAJOR 0)
set(RTIDB_VERSION_MINOR 3)
set(RTIDB_VERSION_BUG 0)
CONFIGURE_FILE (
  "${PROJECT_SOURCE_DIR}/src/version.h.in"
  "${PROJECT_SOURCE_DIR}/src/version.h"
)

add_definitions('-g')
add_definitions('-Wall')
add_definitions('-std=c++11')

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
set(GCC_COVERAGE_LINK_FLAGS    "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free")
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib)
add_subdirectory(src)

