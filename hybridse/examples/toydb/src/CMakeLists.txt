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
get_property(TOYDB_SRC_FILE_LIST_STR GLOBAL PROPERTY TOYDB_SRC_FILE_LIST)
string(REPLACE " " ";" TOYDB_SRC_FILE_LIST ${TOYDB_SRC_FILE_LIST_STR})

add_library(toydb_flags toydb_flags.cc)

# toydb lib
set(TOYDB_DEPS_LIBS toydb_flags hybridse_core hybridse_flags sqlite3
    ${yaml_libs} ${Boost_filesystem_LIBRARY} ${VM_LIBS} ${LLVM_LIBS}
    ${OS_LIB} ${GTEST_LIBRARIES} ${BRPC_LIBS})
add_library(toydb_lib ${TOYDB_SRC_FILE_LIST})
target_link_libraries(toydb_lib ${TOYDB_DEPS_LIBS})

# toydb unittests
if (TESTING_ENABLE AND EXAMPLES_TESTING_ENABLE)
    get_property(TOYDB_TEST_FILE_LIST_STR GLOBAL PROPERTY TOYDB_TEST_FILE_LIST)
    string(REPLACE " " ";" TOYDB_TEST_FILE_LIST ${TOYDB_TEST_FILE_LIST_STR})
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach(TEST_SCRIPT ${TOYDB_TEST_FILE_LIST})
        file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${TEST_SCRIPT})
        get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
        get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
        set(TEST_TARGET_NAME "toydb_${TEST_TARGET_NAME}")
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
        add_executable(${TEST_TARGET_NAME} ${TEST_SCRIPT}
                ${PROJECT_SOURCE_DIR}/src/testing/test_base.cc
                ${PROJECT_SOURCE_DIR}/src/testing/engine_test_base.cc)

        add_test(${TEST_TARGET_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
                --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}.xml)
        target_link_libraries(${TEST_TARGET_NAME}
                toydb_lib toydb_sdk hybridse_flags sqlite3
                ${GTEST_LIBRARIES} benchmark ${yaml_libs} ${OS_LIB} ${g_libs})
        if (TESTING_ENABLE_STRIP)
            strip_exe(${TEST_TARGET_NAME})
        endif()
        list(APPEND test_list ${TEST_TARGET_NAME})
    endforeach()
endif()

# toy sdk
add_subdirectory(sdk)
# toy bm
if (BENCHMARK_ENABLE)
add_subdirectory(bm)
endif()

# toydb bin
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

add_executable(toydb cmd/toydb.cc)
target_link_libraries(toydb toydb_sdk toydb_lib hybridse_sdk hybridse_core hybridse_flags hybridse_test_base)
# engine execution
add_executable(toydb_run_engine cmd/toydb_run_engine.cc)
target_link_libraries(toydb_run_engine toydb_lib toydb_flags hybridse_flags hybridse_test_base ${GTEST_LIBRARIES} sqlite3)

set(test_list ${test_list} PARENT_SCOPE)
