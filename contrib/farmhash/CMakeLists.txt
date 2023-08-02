# TODO: recommanded compile flag: '-mavx -maes -O3'
add_library(_farmhash ${CMAKE_CURRENT_SOURCE_DIR}/farmhash.cc)
target_include_directories(_farmhash SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}")
target_compile_definitions(_farmhash PUBLIC NAMESPACE_FOR_HASH_FUNCTIONS=farmhash)

add_library(op_contrib::farmhash ALIAS _farmhash)
