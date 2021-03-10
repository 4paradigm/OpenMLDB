# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src
        ${PROJECT_SOURCE_DIR}/examples/toydb/src)
# split toydb test and toydb
set(TOYDB_SRC_FILE_LIST)
set(TOYDB_TEST_FILE_LIST)

function(toydb_add_src_and_tests DIR)
    message(STATUS "Add src and testcases from ${DIR}")
    get_property(TMP_SRC_LIST GLOBAL PROPERTY TOYDB_SRC_FILE_LIST)
    get_property(TMP_TEST_SRC_LIST GLOBAL PROPERTY TOYDB_TEST_FILE_LIST)
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    foreach(SRC_FILE ${SRC_FILES})
        if (SRC_FILE MATCHES ".*_test.cc")
            set(TMP_TEST_SRC_LIST "${TMP_TEST_SRC_LIST} ${SRC_FILE}")
        else()
            set(TMP_SRC_LIST "${TMP_SRC_LIST} ${SRC_FILE}")
        endif()
    endforeach()
    set_property(GLOBAL PROPERTY TOYDB_SRC_FILE_LIST "${TMP_SRC_LIST}")
    set_property(GLOBAL PROPERTY TOYDB_TEST_FILE_LIST "${TMP_TEST_SRC_LIST}")
endfunction(toydb_add_src_and_tests)

toydb_add_src_and_tests(storage)
toydb_add_src_and_tests(dbms)
toydb_add_src_and_tests(tablet)
toydb_add_src_and_tests(testing)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(TOYDB_LIBS ${ABSL_LIBS} ${yaml_libs} ${g_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} tcmalloc ${OS_LIB} benchmark gtest)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(TOYDB_LIBS ${ABSL_LIBS} ${yaml_libs} ${g_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS} ${OS_LIB} benchmark gtest)
endif ()
get_property(TOYDB_SRC_FILE_LIST_STR GLOBAL PROPERTY TOYDB_SRC_FILE_LIST)
string(REPLACE " " ";" TOYDB_SRC_FILE_LIST ${TOYDB_SRC_FILE_LIST_STR})

# toydb lib
add_library(fesql_toy_lib STATIC ${TOYDB_SRC_FILE_LIST})
add_library(toy_flags STATIC toy_flags.cc)
set(TOYDB_DEPS_LIBS fesql_core fesql_flags toy_flags
        ${yaml_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${OS_LIB})

target_link_libraries(fesql_toy_lib ${TOYDB_DEPS_LIBS})

# toydb unittests
if (TESTING_ENABLE)
    get_property(TOYDB_TEST_FILE_LIST_STR GLOBAL PROPERTY TOYDB_TEST_FILE_LIST)
    string(REPLACE " " ";" TOYDB_TEST_FILE_LIST ${TOYDB_TEST_FILE_LIST_STR})
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach(TEST_SCRIPT ${TOYDB_TEST_FILE_LIST})
        file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${TEST_SCRIPT})
        get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
        get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
        add_executable(${TEST_TARGET_NAME} ${TEST_SCRIPT})

        add_test(${TEST_TARGET_NAME}
                ${TEST_TARGET_NAME}
                --gtest_output=xml:${CMAKE_BINARY_DIR}/${TEST_TARGET_NAME}.xml)

        target_link_libraries(${TEST_TARGET_NAME}
                fesql_toy_lib fesql_sdk fesql_flags
                gtest benchmark ${yaml_libs} ${BRPC_LIBS} ${OS_LIBS} ${g_libs} sqlite3)
        list(APPEND test_list ${TEST_TARGET_NAME})
    endforeach()
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()

# toy sdk
add_subdirectory(sdk)

# fesql bin
add_executable(fesql cmd/fesql.cc)
target_link_libraries(fesql fesql_core fesql_flags fesql_toy_lib fesql_sdk)
# engine execution
add_executable(fesql_run_engine cmd/fesql_run_engine.cc)
target_link_libraries(fesql_run_engine fesql_toy_lib toy_flags fesql_flags gtest sqlite3)
