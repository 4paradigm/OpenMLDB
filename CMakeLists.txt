cmake_minimum_required (VERSION 3.8)

if(POLICY CMP0068)
  cmake_policy(SET CMP0068 NEW)
endif()
# Define file(GENERATE) behavior for relative paths.
if(POLICY CMP0070)
  cmake_policy(SET CMP0070 NEW)
endif()
# target_sources: use absolute path for INTERFACE_SOURCES.
if(POLICY CMP0076)
  cmake_policy(SET CMP0076 NEW)
endif()
# SWIG: use standard target name.
if(POLICY CMP0078)
  cmake_policy(SET CMP0078 NEW)
endif()
# SWIG: use SWIG_MODULE_NAME property.
if(POLICY CMP0086)
  cmake_policy(SET CMP0086 NEW)
endif()

list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake-modules")

project(fesql)

set(FESQL_VERSION_MAJOR 0)
set(FESQL_VERSION_MEDIUM 1)
set(FESQL_VERSION_MINOR 0)
set(FESQL_VERSION_BUG 0)

set(CMAKE_BUILD_TYPE RelWithDebInfo)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
#set(CMAKE_BUILD_TYPE DEBUG)
set(LLVM_DIR "${PROJECT_SOURCE_DIR}/thirdparty/lib/cmake/llvm")

set(COMMON_LIBS glog gflags unwind lzma)
set(g_libs glog gflags unwind lzma)

find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
configure_file (
      "${PROJECT_SOURCE_DIR}/src/version.h.in"
      "${PROJECT_SOURCE_DIR}/src/version.h"
)

option(TESTING_ENABLE "Enable Test" ON)
option(COVERAGE_ENABLE "Enable Coverage" ON)
if (TESTING_ENABLE)
    enable_testing()
endif()
if (COVERAGE_ENABLE)
    if(CMAKE_COMPILER_IS_GNUCXX)
        include(CodeCoverage)
        APPEND_COVERAGE_COMPILER_FLAGS()
    endif()
endif()

set(arrow_libs parquet arrow_dataset arrow brotlidec brotlienc 
    brotlicommon
    thrift lz4 z zstd double-conversion boost_filesystem snappy)
llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)
add_definitions('-g')
add_definitions('-Wall')
add_definitions('-std=c++14')
add_definitions(${LLVM_DEFINITIONS})
find_package(SWIG REQUIRED)
include(UseSWIG)


include_directories(${PROJECT_SOURCE_DIR}/thirdparty/include)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib)
link_directories(${PROJECT_SOURCE_DIR}/thirdparty/lib64)

set(Python_ADDITIONAL_VERSIONS "3.8;3.7;3.6;3.5;2.7" CACHE STRING "Python to use for binding")
find_package(PythonInterp REQUIRED)
message(STATUS "Found Python: ${PYTHON_EXECUTABLE} (found version \"${PYTHON_VERSION_STRING}\")")
# Find Python Library
# Force PythonLibs to find the same version than the python interpreter (or nothing).
set(Python_ADDITIONAL_VERSIONS "${PYTHON_VERSION_STRING}")
find_package(PythonLibs REQUIRED)
message(STATUS "Found Python Include: ${PYTHON_INCLUDE_DIRS} (found version \"${PYTHONLIBS_VERSION_STRING}\")")
# Find java

set(JAVA_HOME ${PROJECT_SOURCE_DIR}/thirdparty/jdk1.8.0_141)
find_package(Java COMPONENTS Development REQUIRED)
message(STATUS "Found Java: ${Java_JAVA_EXECUTABLE} (found version \"${Java_VERSION_STRING}\")")

find_package(JNI REQUIRED)
message(STATUS "Found JNI: ${JNI_FOUND}")
# Find maven
find_program(MAVEN_EXECUTABLE mvn)
configure_file(java/pom.xml.in java/pom.xml @ONLY)

add_subdirectory(src)
#TODO add flag
add_subdirectory(experiments)

if (COVERAGE_ENABLE)
    if(CMAKE_COMPILER_IS_GNUCXX)
        SETUP_TARGET_FOR_COVERAGE_LCOV(NAME coverage  
            EXECUTABLE ctest -j ${n_cores}
            DEPENDENCIES ${test_list})
    endif()
endif()
