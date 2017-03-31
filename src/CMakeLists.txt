include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)


add_executable(skiplist_test base/skiplist_test.cc)
target_link_libraries(skiplist_test gtest pthread tcmalloc_minimal)

add_executable(segment_test  storage/segment.cc storage/segment_test.cc)
target_link_libraries(segment_test gtest pthread common tcmalloc_minimal)

