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

function(fesql_add_src_and_tests DIR)
	message(STATUS "Add src and testcases from ${DIR}")
	get_property(TMP_SRC_LIST GLOBAL PROPERTY PROP_SRC_FILE_LIST)
	get_property(TMP_TEST_SRC_LIST GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST)
	file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
	foreach(SRC_FILE ${SRC_FILES})
	    if (SRC_FILE MATCHES ".*_test.cc")
	    	set(TMP_TEST_SRC_LIST "${TMP_TEST_SRC_LIST} ${SRC_FILE}")
	    else()
	    	set(TMP_SRC_LIST "${TMP_SRC_LIST} ${SRC_FILE}")
	    endif()
	endforeach()
	set_property(GLOBAL PROPERTY PROP_SRC_FILE_LIST "${TMP_SRC_LIST}")
	set_property(GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST "${TMP_TEST_SRC_LIST}")
endfunction(fesql_add_src_and_tests)

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

# sub-directory with specific modules
add_subdirectory(proto)
add_subdirectory(parser)

# general sub-directories
fesql_add_src_and_tests(base)
fesql_add_src_and_tests(udf)
fesql_add_src_and_tests(node)
fesql_add_src_and_tests(plan)
fesql_add_src_and_tests(codegen)
fesql_add_src_and_tests(vm)
fesql_add_src_and_tests(codec)
fesql_add_src_and_tests(case)
fesql_add_src_and_tests(parser)
fesql_add_src_and_tests(passes)

get_property(SRC_FILE_LIST_STR GLOBAL PROPERTY PROP_SRC_FILE_LIST)
string(REPLACE " " ";" SRC_FILE_LIST ${SRC_FILE_LIST_STR})

# compile llvm extension sources without rtti
if (LLVM_EXT_ENABLE)
    add_subdirectory(llvm_ext)
    add_definitions(-DLLVM_EXT_ENABLE)
endif()

add_library(fesql_flags STATIC ${CMAKE_SOURCE_DIR}/src/flags.cc)

# fesql core library
add_library(fesql_core STATIC ${SRC_FILE_LIST} ${PROJECT_SOURCE_DIR}/src/flags.cc  $<TARGET_OBJECTS:fesql_proto> $<TARGET_OBJECTS:fesql_parser> case/case_data_mock.cc vm/engine_test_base.cc vm/test_base.cc)
target_link_libraries(fesql_core
    ${yaml_libs} ${LLVM_LIBS} ${OS_LIB} ${COMMON_LIBS} ${g_libs} ${LLVM_EXT_LIB} fesql_flags)

# fesql core unittests
if (TESTING_ENABLE)
	get_property(TEST_FILE_LIST_STR GLOBAL PROPERTY PROP_TEST_SRC_FILE_LIST)
	string(REPLACE " " ";" TEST_FILE_LIST ${TEST_FILE_LIST_STR})
	set(TEST_CPP_SOURCE_DIR ${CMAKE_CURRENT_SOURCE_DIR})
	foreach(TEST_SCRIPT ${TEST_FILE_LIST}) 
	    file(RELATIVE_PATH RELATIVE_TEST_PATH ${TEST_CPP_SOURCE_DIR} ${TEST_SCRIPT})
	    get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
	    get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
	    set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
		add_executable(${TEST_TARGET_NAME} ${TEST_SCRIPT})

		add_test(${TEST_TARGET_NAME}
			${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
			--gtest_output=xml:${CMAKE_BINARY_DIR}/${TEST_TARGET_NAME}.xml)

            target_link_options(${TEST_TARGET_NAME} PRIVATE -rdynamic)
	    target_link_libraries(${TEST_TARGET_NAME} gtest fesql_core fesql_flags
                ${yaml_libs} ${BRPC_LIBS} sqlite3)
    	list(APPEND test_list ${TEST_TARGET_NAME})
	endforeach()
	set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})
endif()

# fesql sdk
add_subdirectory(sdk)
# fesql bm
if (BENCHMARK_ENABLE)
add_subdirectory(benchmark)
endif()

# udf exporter
add_executable(export_udf_info cmd/export_udf_info.cc)
target_link_libraries(export_udf_info fesql_core)
