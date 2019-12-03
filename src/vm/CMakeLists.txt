include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_library(fesql_vm STATIC jit.cc op_generator.cc sql_compiler.cc engine.cc)
add_dependencies(fesql_vm fesql_base fesql_udf fesql_node fesql_parser fesql_proto fesql_planner fesql_analyser)
add_executable(jit_test jit_test.cc)
target_link_libraries(jit_test fesql_udf fesql_vm  ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(jit_test jit_test --gtest_output=xml:${CMAKE_BINARY_DIR}/jit_test.xml)
list(APPEND test_list jit_test)

add_executable(op_generator_test op_generator_test.cc)
target_link_libraries(op_generator_test ${FESQL_LIBS} protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(op_generator_test op_generator_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/op_generator_test.xml)
list(APPEND test_list op_generator_test)

add_executable(sql_compiler_test sql_compiler_test.cc)
target_link_libraries(sql_compiler_test ${FESQL_LIBS} protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(sql_compiler_test sql_compiler_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_compiler_test.xml)
list(APPEND test_list sql_compiler_test)

add_executable(engine_test engine_test.cc)
target_link_libraries(engine_test ${FESQL_LIBS} fesql_udf protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(engine_test engine_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/engine_test.xml)
list(APPEND test_list engine_test)

add_executable(engine_bm engine_bm.cc)
target_link_libraries(engine_bm ${FESQL_LIBS} benchmark protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)

