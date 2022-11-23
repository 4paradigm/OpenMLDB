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

set(SDK_DEPS_LIBS hybridse_core hybridse_flags hybridse_sdk
    ${yaml_libs} ${Boost_filesystem_LIBRARY} ${VM_LIBS} ${LLVM_LIBS}
    ${COMMON_LIBS} ${BRPC_LIBRARY} ${OS_LIB})

# split sdk test and bm
set(SDK_SRC_FILE_LIST)
set(SDK_TEST_FILE_LIST)
file(GLOB_RECURSE BM_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)
foreach(SRC_FILE ${BM_FILES})
    if (SRC_FILE MATCHES ".*_test.cc")
        list(APPEND SDK_TEST_FILE_LIST ${SRC_FILE})
    else()
        list(APPEND SDK_SRC_FILE_LIST ${SRC_FILE})
    endif()
endforeach()

# toydb_sdk library
add_library(toydb_sdk STATIC ${SDK_SRC_FILE_LIST})
target_link_libraries(toydb_sdk ${SDK_DEPS_LIBS})

# toydb sdk testcases
if (TESTING_ENABLE AND EXAMPLES_TESTING_ENABLE)
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach(TEST_SCRIPT ${SDK_TEST_FILE_LIST})
        file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${TEST_SCRIPT})
        get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
        get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
        add_executable(${TEST_TARGET_NAME} ${TEST_SCRIPT}
                ${PROJECT_SOURCE_DIR}/src/testing/test_base.cc
                ${PROJECT_SOURCE_DIR}/src/testing/engine_test_base.cc)

        add_test(${TEST_TARGET_NAME}
            ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
            --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}.xml)
        target_link_libraries(${TEST_TARGET_NAME} ${GTEST_LIBRARIES} toydb_sdk toydb_lib zetasql)
        if (TESTING_ENABLE_STRIP)
            strip_exe(${TEST_TARGET_NAME})
        endif()
        list(APPEND test_list ${TEST_TARGET_NAME})
    endforeach()
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
    set(test_list ${test_list} PARENT_SCOPE)
endif()

