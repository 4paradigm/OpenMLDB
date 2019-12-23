include_directories(${INCLUDE_DIRECTORIES} 
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_BINARY_DIR}/src)

add_executable(memsql_bm memsql_bm.cc)
target_link_libraries(memsql_bm benchmark mysqlclient gtest pthread)
