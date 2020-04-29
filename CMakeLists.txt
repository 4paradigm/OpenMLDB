cmake_minimum_required(VERSION 3.8)

if (POLICY CMP0068)
    cmake_policy(SET CMP0068 NEW)
endif ()
# Define file(GENERATE) behavior for relative paths.
if (POLICY CMP0070)
    cmake_policy(SET CMP0070 NEW)
endif ()
# target_sources: use absolute path for INTERFACE_SOURCES.
if (POLICY CMP0076)
    cmake_policy(SET CMP0076 NEW)
endif ()
# SWIG: use standard target name.
if (POLICY CMP0078)
    cmake_policy(SET CMP0078 NEW)
endif ()
# SWIG: use SWIG_MODULE_NAME property.
if (POLICY CMP0086)
    cmake_policy(SET CMP0086 NEW)
endif ()

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
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(COMMON_LIBS glog gflags unwind lzma)
    set(g_libs glog gflags unwind lzma)
    set(arrow_libs parquet arrow_dataset arrow brotlidec brotlienc
            brotlicommon
            thrift lz4 z zstd double-conversion boost_filesystem snappy)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin") # TODO: Make sure all libraries can be linked
    set(COMMON_LIBS glog gflags)
    set(g_libs glog gflags)
    set(arrow_libs parquet arrow_dataset arrow brotlidec brotlienc
            brotlicommon
            thrift lz4 z zstd double-conversion boost_filesystem snappy)
endif ()
set(BOOST_ROOT "${PROJECT_SOURCE_DIR}/thirdparty/")

find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
llvm_map_components_to_libnames(llvm_libs support core orcjit nativecodegen)

configure_file(
        "${PROJECT_SOURCE_DIR}/src/version.h.in"
        "${PROJECT_SOURCE_DIR}/src/version.h"
)
option(TESTING_ENABLE "Enable Test" ON)
option(COVERAGE_ENABLE "Enable Coverage" ON)
if (TESTING_ENABLE)
    enable_testing()
endif ()
if (COVERAGE_ENABLE)
    if (CMAKE_COMPILER_IS_GNUCXX)
        include(CodeCoverage)
        APPEND_COVERAGE_COMPILER_FLAGS()
    endif ()
endif ()

find_package(Threads)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OS_LIB ${OS_LIB}
            ${CMAKE_THREAD_LIBS_INIT}
            "-framework CoreFoundation"
            "-framework CoreGraphics"
            "-framework CoreData"
            "-framework CoreText"
            "-framework Security"
            "-framework Foundation"
            "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
            "-Wl,-U,_ProfilerStart"
            "-Wl,-U,_ProfilerStop")
endif ()
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    add_definitions('-Wall')
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions(-Wno-inconsistent-missing-override)
    add_definitions(-Wno-deprecated-declarations)
    add_definitions(-Wno-null-dereference)
    add_definitions(-Wno-deprecated-register)
    add_definitions(-Wno-c++17-extensions)
    add_definitions(-Wno-defaulted-function-deleted)
endif ()
set(boost_libs boost_graph)

find_package(yaml-cpp REQUIRED)
set(yaml_libs yaml-cpp)
add_definitions('-g')
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

add_subdirectory(src)
#TODO add flag
add_subdirectory(experiments)

if (COVERAGE_ENABLE)
    if (CMAKE_COMPILER_IS_GNUCXX)
        SETUP_TARGET_FOR_COVERAGE_LCOV(NAME coverage
                EXECUTABLE ctest -j ${n_cores}
                DEPENDENCIES ${test_list})
    endif ()
endif ()
