cmake_minimum_required (VERSION 3.5)

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake-modules")

project(fesql)

set(FESQL_VERSION_MAJOR 0)
set(FESQL_VERSION_MEDIUM 1)
set(FESQL_VERSION_MINOR 0)
set(FESQL_VERSION_BUG 0)

set(CMAKE_BUILD_TYPE DEBUG)
set(LLVM_DIR "${PROJECT_SOURCE_DIR}/thirdparty/lib/cmake/llvm")
set(COMMON_LIBS glog gflags unwind lzma)
set(FESQL_PLAN_LIBS fesql_planner fesql_analyser fesql_parser fesql_node fesql_proto)
set(FESQL_LIBS fesql_sdk fesql_dbms fesql_tablet fesql_vm fesql_codegen fesql_parser fesql_analyser
    fesql_planner fesql_proto fesql_node fesql_storage)

find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
configure_file (
      "${PROJECT_SOURCE_DIR}/src/cmd/version.h.in"
      "${PROJECT_SOURCE_DIR}/src/cmd/version.h"
)
option(TESTING_ENABLE "Enable Test" ON)
option(COVERAGE_ENABLE "Enable Coverage" ON)
if (TESTING_ENABLE)
    enable_testing()
endif()
# if (COVERAGE_ENABLE)
#     if(CMAKE_COMPILER_IS_GNUCXX)
#         include(CodeCoverage)
#         APPEND_COVERAGE_COMPILER_FLAGS()
#     endif()
# endif()
llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)
add_definitions('-g')
add_definitions('-Wall')
add_definitions('-std=c++11')
add_definitions(${LLVM_DEFINITIONS})

include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib64)
add_subdirectory(src)

# if (COVERAGE_ENABLE)
#     if(CMAKE_COMPILER_IS_GNUCXX)
#         SETUP_TARGET_FOR_COVERAGE_LCOV(NAME coverage  
#             EXECUTABLE ctest -j ${n_cores}
#             DEPENDENCIES ${test_list})
#     endif()
# endif()

