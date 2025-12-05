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
set_property(GLOBAL PROPERTY PROP_SRC_FILE_LIST)
set_property(GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST)

function(hybridse_add_src_and_tests DIR)
    message(STATUS "Add src and testcases from ${DIR}")
    get_property(TMP_SRC_LIST GLOBAL PROPERTY PROP_SRC_FILE_LIST)
    get_property(TMP_TEST_SRC_LIST GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST)
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    foreach (SRC_FILE ${SRC_FILES})
        if (SRC_FILE MATCHES ".*_test.cc")
            set(TMP_TEST_SRC_LIST "${TMP_TEST_SRC_LIST} ${SRC_FILE}")
        else ()
            set(TMP_SRC_LIST "${TMP_SRC_LIST} ${SRC_FILE}")
        endif ()
    endforeach ()
    set_property(GLOBAL PROPERTY PROP_SRC_FILE_LIST "${TMP_SRC_LIST}")
    set_property(GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST "${TMP_TEST_SRC_LIST}")
endfunction(hybridse_add_src_and_tests)

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_SOURCE_DIR}/include
        ${PROJECT_BINARY_DIR}/src)

# sub-directory with specific modules
add_subdirectory(proto)
# general sub-directories
hybridse_add_src_and_tests(base)
hybridse_add_src_and_tests(udf)
hybridse_add_src_and_tests(node)
hybridse_add_src_and_tests(plan)
hybridse_add_src_and_tests(planv2)
hybridse_add_src_and_tests(codegen)
hybridse_add_src_and_tests(vm)
hybridse_add_src_and_tests(codec)
hybridse_add_src_and_tests(case)
hybridse_add_src_and_tests(passes)
hybridse_add_src_and_tests(rewriter)

get_property(SRC_FILE_LIST_STR GLOBAL PROPERTY PROP_SRC_FILE_LIST)
string(REPLACE " " ";" SRC_FILE_LIST ${SRC_FILE_LIST_STR})

# compile llvm extension sources without rtti
if (LLVM_EXT_ENABLE)
    add_subdirectory(llvm_ext)
    add_definitions(-DLLVM_EXT_ENABLE)
endif ()

add_library(hybridse_flags ${CMAKE_CURRENT_SOURCE_DIR}/flags.cc)
target_link_libraries(hybridse_flags ${GFLAGS_LIBRARY})

# hybridse core library, enable BUILD_SHARED_LIBS to build shared lib
add_library(hybridse_core ${SRC_FILE_LIST} $<TARGET_OBJECTS:hybridse_proto> case/case_data_mock.cc)
target_link_libraries(hybridse_core
    ${yaml_libs} ${LLVM_LIBS} ${ZETASQL_LIBS} ${OS_LIB} ${COMMON_LIBS} ${g_libs}
    ${LLVM_EXT_LIB} op_contrib::simdjson farmhash hybridse_flags)
set(HYBRIDSE_CORE_LIBS hybridse_core)

add_subdirectory(testing)

set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

# hybridse core unittests
if (TESTING_ENABLE AND HYBRIDSE_TESTING_ENABLE)
    get_property(TEST_FILE_LIST_STR GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST)
    string(REPLACE " " ";" TEST_FILE_LIST ${TEST_FILE_LIST_STR})
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach (TEST_SCRIPT ${TEST_FILE_LIST})
        file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${TEST_SCRIPT})
        get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
        get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)

        add_executable(${TEST_TARGET_NAME} ${TEST_SCRIPT} testing/engine_test_base.cc testing/test_base.cc)
        target_link_libraries(${TEST_TARGET_NAME} ${GTEST_LIBRARIES} ${HYBRIDSE_CORE_LIBS} hybridse_flags
                              ${yaml_libs} ${ZETASQL_LIBS} ${COMMON_LIBS})
        set_target_properties(${TEST_TARGET_NAME} PROPERTIES
            RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
        add_test(${TEST_TARGET_NAME}
                ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
                --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}.xml)

        target_link_options(${TEST_TARGET_NAME} PRIVATE -rdynamic)

        if (TESTING_ENABLE_STRIP)
            strip_exe(${TEST_TARGET_NAME})
        endif()

        list(APPEND test_list ${TEST_TARGET_NAME})
    endforeach ()
endif ()

# hybridse sdk
add_subdirectory(sdk)
# hybridse bm
if (BENCHMARK_ENABLE)
    add_subdirectory(benchmark)
endif ()


# udf exporter
add_executable(export_udf_info cmd/export_udf_info.cc)
target_link_libraries(export_udf_info hybridse_core ${ZETASQL_LIBS})

# simple engine demo
add_executable(simple_engine_demo cmd/simple_engine_demo.cc)
target_link_libraries(simple_engine_demo hybridse_core ${ZETASQL_LIBS} ${GTEST_LIBRARIES})

# test_list is defined in `OpenMLDB/CMakeLists.txt` first, propagate it to parent scope or test_list is only in the this scope
set(test_list ${test_list} PARENT_SCOPE)
