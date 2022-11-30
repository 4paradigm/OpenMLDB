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

add_library(hybridse_codegen STATIC
        scope_var.cc
        expr_ir_builder.cc
        fn_ir_builder.cc
        udf_ir_builder.cc
        buf_ir_builder.cc
        ir_base_builder.cc
        window_ir_builder.cc
        block_ir_builder.cc
        fn_let_ir_builder.cc
        aggregate_ir_builder.cc
        cast_expr_ir_builder.cc
        arithmetic_expr_ir_builder.cc
        variable_ir_builder.cc
        timestamp_ir_builder.cc
        date_ir_builder.cc
        string_ir_builder.cc
        struct_ir_builder.cc
        list_ir_builder.cc
        predicate_expr_ir_builder.cc
        cond_select_ir_builder.cc
        null_ir_builder.cc
        native_value.cc
        context.cc memery_ir_builder.h memory_ir_builder.cc)
if (TESTING_ENABLE)
set(vm_deps_libs
        hybridse_case ${Boost_date_time_LIBRARY} ${Boost_filesystem_LIBRARY} ${yaml_libs} ${VM_LIBS} hybridse_flags ${LLVM_LIBS} ${COMMON_LIBS} ${OS_LIB})

add_executable(ir_base_builder_test ir_base_builder_test.cc)
target_link_libraries(ir_base_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(ir_base_builder_test ir_base_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/ir_base_builder_test.xml)
list(APPEND test_list ir_base_builder_test)

add_executable(string_ir_builder_test string_ir_builder_test.cc codegen_base_test.cc)
target_link_libraries(string_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(string_ir_builder_test string_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/string_ir_builder_test.xml)
list(APPEND test_list string_ir_builder_test)

add_executable(timestamp_ir_builder_test timestamp_ir_builder_test.cc codegen_base_test.cc)
target_link_libraries(timestamp_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(timestamp_ir_builder_test timestamp_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/timestamp_ir_builder_test.xml)
list(APPEND test_list timestamp_ir_builder_test)


add_executable(date_ir_builder_test date_ir_builder_test.cc codegen_base_test.cc)
target_link_libraries(date_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(date_ir_builder_test date_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/date_ir_builder_test.xml)
list(APPEND test_list date_ir_builder_test)

add_executable(variable_ir_builder_test variable_ir_builder_test.cc)
target_link_libraries(variable_ir_builder_test  ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(variable_ir_builder_test variable_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/variable_ir_builder_test.xml)
list(APPEND test_list variable_ir_builder_test)

add_executable(cast_expr_ir_builder_test cast_expr_ir_builder_test.cc)
target_link_libraries(cast_expr_ir_builder_test  ${GTEST_LIBRARIES}
    ${vm_deps_libs})
add_test(cast_expr_ir_builder_test cast_expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/cast_expr_ir_builder_test.xml)
list(APPEND test_list cast_expr_ir_builder_test)

add_executable(arithmetic_expr_ir_builder_test arithmetic_expr_ir_builder_test.cc)
target_link_libraries(arithmetic_expr_ir_builder_test  ${GTEST_LIBRARIES}
        hybridse_udf ${vm_deps_libs})
add_test(arithmetic_expr_ir_builder_test arithmetic_expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/arithmetic_expr_ir_builder_test.xml)
list(APPEND test_list arithmetic_expr_ir_builder_test)

add_executable(predicate_expr_ir_builder_test predicate_expr_ir_builder_test.cc)
target_link_libraries(predicate_expr_ir_builder_test ${GTEST_LIBRARIES}
        ${vm_deps_libs})
add_test(predicate_expr_ir_builder_test predicate_expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/predicate_expr_ir_builder_test.xml)
list(APPEND test_list predicate_expr_ir_builder_test)


add_executable(expr_ir_builder_test expr_ir_builder_test.cc)
target_link_libraries(expr_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs} hybridse_vm)
add_test(expr_ir_builder_test expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/expr_ir_builder_test.xml)
list(APPEND test_list expr_ir_builder_test)


add_executable(block_ir_builder_test block_ir_builder_test.cc)
target_link_libraries(block_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs} hybridse_vm)
add_test(block_ir_builder_test block_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/block_ir_builder_test.xml)
list(APPEND test_list block_ir_builder_test)

add_executable(udf_ir_builder_test udf_ir_builder_test.cc codegen_base_test)
target_link_libraries(udf_ir_builder_test ${GTEST_LIBRARIES}
        ${vm_deps_libs})
add_test(udf_ir_builder_test udf_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/udf_ir_builder_test.xml)
list(APPEND test_list udf_ir_builder_test)


add_executable(fn_ir_builder_test fn_ir_builder_test.cc codegen_base_test)
target_link_libraries(fn_ir_builder_test ${GTEST_LIBRARIES}
        ${vm_deps_libs} )
add_test(fn_ir_builder_test fn_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fn_ir_builder_test.xml)
list(APPEND test_list fn_ir_builder_test)

add_executable(aggregate_ir_builder_test aggregate_ir_builder_test.cc codegen_base_test)
target_link_libraries(aggregate_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs} )
add_test(aggregate_ir_builder_test aggregate_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/aggregate_ir_builder_test.xml)
list(APPEND test_list aggregate_ir_builder_test)


add_executable(buf_ir_builder_test buf_ir_builder_test.cc codegen_base_test.cc)
target_link_libraries(buf_ir_builder_test ${GTEST_LIBRARIES}
        ${vm_deps_libs})

add_test(buf_ir_builder_test buf_ir_builder_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/buf_ir_builder_test.xml)
list(APPEND test_list buf_ir_builder_test)

add_executable(list_ir_builder_test list_ir_builder_test.cc codegen_base_test.cc)
target_link_options(list_ir_builder_test PUBLIC -rdynamic)
target_link_libraries(list_ir_builder_test  ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(list_ir_builder_test list_ir_builder_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/list_ir_builder_test.xml)
list(APPEND test_list list_ir_builder_test)

add_executable(fn_let_ir_builder_test fn_let_ir_builder_test.cc codegen_base_test)
target_link_libraries(fn_let_ir_builder_test ${GTEST_LIBRARIES} ${vm_deps_libs})
add_test(fn_let_ir_builder_test fn_let_ir_builder_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/fn_let_ir_builder_test.xml)
list(APPEND test_list fn_let_ir_builder_test)
endif ()
