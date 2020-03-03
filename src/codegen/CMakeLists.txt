include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_codegen STATIC scope_var.cc variable_ir_builder.cc list_ir_builder.cc cast_expr_ir_builder.cc arithmetic_expr_ir_builder.cc predicate_expr_ir_builder.cc
        expr_ir_builder.cc block_ir_builder.cc fn_ir_builder.cc buf_ir_builder.cc ir_base_builder.cc fn_let_ir_builder.cc)
add_dependencies(fesql_codegen fesql_proto fesql_vm)

add_executable(ir_base_builder_test ir_base_builder_test.cc)
target_link_libraries(ir_base_builder_test  fesql_codegen fesql_node fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(ir_base_builder_test ir_base_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/ir_base_builder_test.xml)
list(APPEND test_list ir_base_builder_test)



add_executable(variable_ir_builder_test variable_ir_builder_test.cc)
target_link_libraries(variable_ir_builder_test gflags fesql_codegen fesql_parser
        fesql_node fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(variable_ir_builder_test variable_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/variable_ir_builder_test.xml)
list(APPEND test_list variable_ir_builder_test)

add_executable(cast_expr_ir_builder_test cast_expr_ir_builder_test.cc)
target_link_libraries(cast_expr_ir_builder_test gflags fesql_codegen fesql_parser
        fesql_node fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(cast_expr_ir_builder_test cast_expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/cast_expr_ir_builder_test.xml)
list(APPEND test_list cast_expr_ir_builder_test)

add_executable(arithmetic_expr_ir_builder_test arithmetic_expr_ir_builder_test.cc)
target_link_libraries(arithmetic_expr_ir_builder_test fesql_udf fesql_codegen fesql_parser fesql_storage fesql_vm fesql_proto fesql_node
        protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(arithmetic_expr_ir_builder_test arithmetic_expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/arithmetic_expr_ir_builder_test.xml)
list(APPEND test_list arithmetic_expr_ir_builder_test)

add_executable(predicate_expr_ir_builder_test predicate_expr_ir_builder_test.cc)
target_link_libraries(predicate_expr_ir_builder_test fesql_udf fesql_codegen fesql_parser fesql_storage fesql_vm fesql_proto fesql_node
        protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(predicate_expr_ir_builder_test predicate_expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/predicate_expr_ir_builder_test.xml)
list(APPEND test_list predicate_expr_ir_builder_test)


add_executable(expr_ir_builder_test expr_ir_builder_test.cc)
target_link_libraries(expr_ir_builder_test gflags fesql_codegen fesql_parser
        fesql_node fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(expr_ir_builder_test expr_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/expr_ir_builder_test.xml)
list(APPEND test_list expr_ir_builder_test)


add_executable(block_ir_builder_test block_ir_builder_test.cc)
target_link_libraries(block_ir_builder_test gflags fesql_codegen fesql_parser
        fesql_node fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(block_ir_builder_test block_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/block_ir_builder_test.xml)
list(APPEND test_list block_ir_builder_test)

add_executable(fn_ir_builder_test fn_ir_builder_test.cc)
target_link_libraries(fn_ir_builder_test fesql_udf fesql_codegen fesql_parser fesql_storage fesql_vm fesql_proto fesql_node
        protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(fn_ir_builder_test fn_ir_builder_test --gtest_output=xml:${CMAKE_BINARY_DIR}/fn_ir_builder_test.xml)
list(APPEND test_list fn_ir_builder_test)

add_executable(buf_ir_builder_test buf_ir_builder_test.cc codegen_base_test.cc)
target_link_libraries(buf_ir_builder_test fesql_codegen fesql_storage fesql_vm
        fesql_proto ${COMMON_LIBS} ${llvm_libs} protobuf gtest pthread)
add_test(buf_ir_builder_test buf_ir_builder_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/buf_ir_builder_test.xml)
list(APPEND test_list buf_ir_builder_test)

add_executable(list_ir_builder_test list_ir_builder_test.cc codegen_base_test.cc)
target_link_libraries(list_ir_builder_test fesql_udf fesql_codegen fesql_parser
        fesql_planner fesql_node fesql_storage fesql_vm fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(list_ir_builder_test list_ir_builder_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/list_ir_builder_test.xml)
list(APPEND test_list list_ir_builder_test)

add_executable(fn_let_ir_builder_test fn_let_ir_builder_test.cc)
target_link_libraries(fn_let_ir_builder_test fesql_udf fesql_codegen fesql_parser
        fesql_planner fesql_node fesql_storage fesql_vm fesql_proto protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(fn_let_ir_builder_test fn_let_ir_builder_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/fn_let_ir_builder_test.xml)
list(APPEND test_list fn_let_ir_builder_test)




