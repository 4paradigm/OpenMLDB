cmake_minimum_required (VERSION 2.8)

project(rtidb)

set(CMAKE_BUILD_TYPE Release)
set(RTIDB_VERSION_MAJOR 1)
set(RTIDB_VERSION_MINOR 3)
set(RTIDB_VERSION_BUG 3)
configure_file (
  "${PROJECT_SOURCE_DIR}/src/version.h.in"
  "${PROJECT_SOURCE_DIR}/src/version.h"
)

add_definitions('-g')
add_definitions('-Wall')
add_definitions('-std=c++11')
option(TCMALLOC_ENABLE "Enable TCMALLOC" ON)
option(STATIC_COMPILE_ENABLE "Enable STATIC COMPILE" OFF) 
if (STATIC_COMPILE_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-static -static-libgcc -static-libstdc++")
endif()

set(CMAKE_ARCHIVE_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_LIBRARY_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/lib)
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
if (TCMALLOC_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free ${CMAKE_EXE_LINKER_FLAGS}")
endif()

option(LOG_FILE_PATH_ENABLE "Enable LOG FILE PATH ENABLE" OFF) 
message("LOG_FILE_PATH_ENABLE option is ${LOG_FILE_PATH_ENABLE}")
if (LOG_FILE_PATH_ENABLE)
    set(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -D__FILE__='\"$(subst ${CMAKE_SOURCE_DIR}/,,$(abspath $<))\"' -Wno-builtin-macro-redefined")
endif()

configure_file(
    "${PROJECT_SOURCE_DIR}/src/config.h.in"
    "${PROJECT_SOURCE_DIR}/src/config.h")
include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib)
add_subdirectory(src)

