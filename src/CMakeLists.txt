include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)


add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread tcmalloc_minimal)

