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
set(BENCHMARK_LIB_FILE_LIST)
set(BENCHMARK_EXEC_FILE_LIST)
set(BENCHMARK_TEST_FILE_LIST)
file(GLOB_RECURSE BENCHMARK_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)
foreach(SRC_FILE ${BENCHMARK_FILES})
    if (SRC_FILE MATCHES ".*_test.cc")
        list(APPEND BENCHMARK_TEST_FILE_LIST ${SRC_FILE})
    elseif (SRC_FILE MATCHES ".*_bm.cc")
        list(APPEND BENCHMARK_EXEC_FILE_LIST ${SRC_FILE})
    else()
        list(APPEND BENCHMARK_LIB_FILE_LIST ${SRC_FILE})
    endif()
endforeach()
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(BENCHMARK_LIBS ${ABSL_LIBS} ${yaml_libs} ${g_libs} ${Boost_filesystem_LIBRARY} ${VM_LIBS} ${LLVM_LIBS} ${COMMON_LIBS} tcmalloc ${OS_LIB} benchmark ${GTEST_LIBRARIES})
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(BENCHMARK_LIBS ${ABSL_LIBS} ${yaml_libs} ${g_libs} ${Boost_filesystem_LIBRARY} ${VM_LIBS} ${LLVM_LIBS} ${COMMON_LIBS} ${OS_LIB} benchmark ${GTEST_LIBRARIES})
endif ()
# bm lib
add_library(hybridse_bm_lib STATIC ${BENCHMARK_LIB_FILE_LIST})
target_link_libraries(hybridse_bm_lib hybridse_sdk hybridse_flags ${BENCHMARK_LIIBS})
# bm executables
set(BENCHMARK_TARGET_LIST)
foreach(BENCHMARK_SCRIPT ${BENCHMARK_EXEC_FILE_LIST})
    file(RELATIVE_PATH RELATIVE_SRC_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${BENCHMARK_SCRIPT})
        get_filename_component(BENCHMARK_TARGET_DIR ${RELATIVE_SRC_PATH} DIRECTORY)
        get_filename_component(BENCHMARK_TARGET_NAME ${RELATIVE_SRC_PATH} NAME_WE)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${BENCHMARK_TARGET_DIR})
        add_executable(${BENCHMARK_TARGET_NAME} ${BENCHMARK_SCRIPT})

        target_link_libraries(${BENCHMARK_TARGET_NAME}
            hybridse_sdk hybridse_flags hybridse_bm_lib ${BENCHMARK_LIBS} benchmark)
        list(APPEND BENCHMARK_TARGET_LIST ${BENCHMARK_TARGET_NAME})
endforeach()
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
add_custom_target(hybridse_bm DEPENDS ${BENCHMARK_TARGET_LIST})

# bm unittests
if (TESTING_ENABLE AND HYBRIDSE_TESTING_ENABLE)
    set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
    foreach(TEST_SCRIPT ${BENCHMARK_TEST_FILE_LIST})
        file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${TEST_SCRIPT})
        get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
        get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
        set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
        add_executable(${TEST_TARGET_NAME} ${TEST_SCRIPT})

        add_test(${TEST_TARGET_NAME}
            ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
            --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}.xml)

        target_link_libraries(${TEST_TARGET_NAME}
            hybridse_bm_lib hybridse_sdk hybridse_flags
            ${GTEST_LIBRARIES} benchmark ${yaml_libs} ${COMMON_LIBS} ${OS_LIB} ${g_libs})
        list(APPEND test_list ${TEST_TARGET_NAME})
    endforeach()
    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()

