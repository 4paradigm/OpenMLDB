include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_vm STATIC jit.cc op_generator.cc sql_compiler.cc engine.cc)
add_dependencies(fesql_vm run_gen_proto fesql_parser)
set(vm_deps_libs fesql_storage fesql_vm fesql_parser fesql_planner fesql_codegen fesql_proto
    fesql_node fesql_udf)

add_executable(jit_test jit_test.cc)
target_link_libraries(jit_test ${vm_deps_libs}  ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(jit_test jit_test --gtest_output=xml:${CMAKE_BINARY_DIR}/jit_test.xml)
list(APPEND test_list jit_test)

add_executable(op_generator_test op_generator_test.cc)
target_link_libraries(op_generator_test 
    ${vm_deps_libs} protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)

add_test(op_generator_test op_generator_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/op_generator_test.xml)
list(APPEND test_list op_generator_test)

add_executable(sql_compiler_test sql_compiler_test.cc)
target_link_libraries(sql_compiler_test  ${vm_deps_libs} 
    protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(sql_compiler_test sql_compiler_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_compiler_test.xml)
list(APPEND test_list sql_compiler_test)

add_executable(engine_test engine_test.cc)
target_link_libraries(engine_test  ${vm_deps_libs}
     protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(engine_test engine_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/engine_test.xml)
list(APPEND test_list engine_test)

add_executable(engine_bm engine_bm.cc)
target_link_libraries(engine_bm  ${vm_deps_libs}
     benchmark protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
