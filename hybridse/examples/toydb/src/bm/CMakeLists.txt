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
        ${PROJECT_BINARY_DIR}/src)

# split bm test and bm
set(BM_LIB_FILE_LIST)
set(BM_EXEC_FILE_LIST)
set(BM_TEST_FILE_LIST)
file(GLOB_RECURSE BM_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)
foreach(SRC_FILE ${BM_FILES})
    if (SRC_FILE MATCHES ".*_test.cc")
        list(APPEND BM_TEST_FILE_LIST ${SRC_FILE})
    elseif (SRC_FILE MATCHES ".*_bm.cc")
        list(APPEND BM_EXEC_FILE_LIST ${SRC_FILE})
    else()
        list(APPEND BM_LIB_FILE_LIST ${SRC_FILE})
    endif()
endforeach()

set(BM_LIBS ${ABSL_LIBS} ${yaml_libs} ${g_libs} ${Boost_filesystem_LIBRARY}
    ${VM_LIBS} ${LLVM_LIBS} ${COMMON_LIBS} ${BRPC_LIBRARY} ${OS_LIB} benchmark gtest)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(BM_LIBS ${BM_LIBS} tcmalloc)
endif ()

# bm lib
add_library(toydb_bm_lib STATIC ${BM_LIB_FILE_LIST})
target_link_libraries(toydb_bm_lib toydb_lib toydb_sdk hybridse_sdk hybridse_flags ${BM_LIIBS})
# bm executables
set(BM_TARGET_LIST)
foreach(BM_SCRIPT ${BM_EXEC_FILE_LIST})
    file(RELATIVE_PATH RELATIVE_SRC_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${BM_SCRIPT})
        get_filename_component(BM_TARGET_DIR ${RELATIVE_SRC_PATH} DIRECTORY)
        get_filename_component(BM_TARGET_NAME ${RELATIVE_SRC_PATH} NAME_WE)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${BM_TARGET_DIR})
        add_executable(${BM_TARGET_NAME} ${BM_SCRIPT})

        target_link_libraries(${BM_TARGET_NAME}
            hybridse_sdk hybridse_flags toydb_lib toydb_sdk toydb_bm_lib ${BM_LIBS} benchmark sqlite3)
        list(APPEND BM_TARGET_LIST ${BM_TARGET_NAME})
endforeach()
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_custom_target(toydb_bm DEPENDS ${BM_TARGET_LIST})

# bm unittests
if (TESTING_ENABLE AND EXAMPLES_TESTING_ENABLE)
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach(TEST_SCRIPT ${BM_TEST_FILE_LIST})
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

        target_link_libraries(${TEST_TARGET_NAME}
                toydb_lib toydb_sdk toydb_bm_lib hybridse_sdk hybridse_flags
                ${GTEST_LIBRARIES} benchmark ${yaml_libs} ${COMMON_LIBS} ${BRPC_LIBRARY} ${OS_LIB} ${g_libs} sqlite3)
        list(APPEND test_list ${TEST_TARGET_NAME})
    endforeach()
endif()

