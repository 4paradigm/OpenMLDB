include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_vm STATIC jit.cc op_generator.cc sql_compiler.cc engine.cc transform.cc physical_op.cc)
add_library(fesql_csv_catalog STATIC csv_catalog.cc csv_table_iterator.cc
        csv_window_iterator.cc)

set(vm_deps_libs fesql_tablet fesql_storage fesql_vm fesql_parser fesql_planner fesql_udf fesql_codegen fesql_proto
        fesql_node fesql_base)

add_executable(csv_db csv_db.cc)
target_link_libraries(csv_db fesql_csv_catalog ${vm_deps_libs} ${llvm_libs} ${g_libs} ${arrow_libs} ${COMMON_LIBS} protobuf gtest pthread)


add_executable(physical_op_test physical_op_test.cc)
target_link_libraries(physical_op_test ${COMMON_LIBS} gtest pthread)
add_test(physical_op_test physical_op_test --gtest_output=xml:${CMAKE_BINARY_DIR}/physical_op_test.xml)
list(APPEND physical_op_test physical_op_test)


add_executable(transform_test transform_test.cc)
target_link_libraries(transform_test ${vm_deps_libs} protobuf ${llvm_libs} ${COMMON_LIBS} ${Boost_LIBRARIES} gtest pthread)
add_test(transform_test transform_test --gtest_output=xml:${CMAKE_BINARY_DIR}/transform_test.xml)
list(APPEND transform_test transform_test)

#add_executable(csv_catalog_test csv_catalog_test.cc)
#target_link_libraries(csv_catalog_test  fesql_csv_catalog ${vm_deps_libs} ${llvm_libs} ${g_libs} ${arrow_libs} ${COMMON_LIBS} protobuf gtest pthread)
#add_test(csv_catalog_test csv_catalog_test --gtest_output=xml:${CMAKE_BINARY_DIR}/csv_catalog_test.xml)
#list(APPEND test_list csv_catalog_test)

add_executable(jit_test jit_test.cc)
target_link_libraries(jit_test ${vm_deps_libs} protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(jit_test jit_test --gtest_output=xml:${CMAKE_BINARY_DIR}/jit_test.xml)
list(APPEND test_list jit_test)

add_executable(op_generator_test op_generator_test.cc)
target_link_libraries(op_generator_test
        ${vm_deps_libs} protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)

add_test(op_generator_test op_generator_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/op_generator_test.xml)
list(APPEND test_list op_generator_test)

add_executable(sql_compiler_test sql_compiler_test.cc)
target_link_libraries(sql_compiler_test ${vm_deps_libs}
        protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(sql_compiler_test sql_compiler_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/sql_compiler_test.xml)
list(APPEND test_list sql_compiler_test)

add_executable(engine_test engine_test.cc)
target_link_libraries(engine_test ${vm_deps_libs}
        protobuf ${llvm_libs} ${COMMON_LIBS} gtest pthread)
add_test(engine_test engine_test
        --gtest_output=xml:${CMAKE_BINARY_DIR}/engine_test.xml)
list(APPEND test_list engine_test)

add_executable(engine_bm engine_bm.cc)
target_link_libraries(engine_bm ${vm_deps_libs}
        benchmark protobuf ${llvm_libs} ${COMMON_LIBS} tcmalloc profiler gtest pthread rt)

