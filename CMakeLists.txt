cmake_minimum_required (VERSION 3.5)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake-modules")

project(fesql)
set(FESQL_VERSION_MAJOR 0)
set(FESQL_VERSION_MEDIUM 1)
set(FESQL_VERSION_MINOR 0)
set(FESQL_VERSION_BUG 0)

set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(LLVM_DIR "${PROJECT_SOURCE_DIR}/thirdparty/lib/cmake/llvm")

find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
configure_file (
      "${PROJECT_SOURCE_DIR}/src/version.h.in"
      "${PROJECT_SOURCE_DIR}/src/version.h"
)
option(TESTING_ENABLE "Enable Test" ON)
option(COVERAGE_ENABLE "Enable Coverage" OFF) 
if (TESTING_ENABLE)
    enable_testing()
endif()
if (COVERAGE_ENABLE)
    if(CMAKE_COMPILER_IS_GNUCXX)
        include(CodeCoverage)
        APPEND_COVERAGE_COMPILER_FLAGS()
    endif()
endif()

add_definitions('-g')
add_definitions('-Wall')
add_definitions('-std=c++11')
add_definitions(${LLVM_DEFINITIONS})

include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib64)
add_subdirectory(src)

