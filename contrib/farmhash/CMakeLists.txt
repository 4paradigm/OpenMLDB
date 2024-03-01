
add_library(farmhash ${CMAKE_CURRENT_SOURCE_DIR}/src/farmhash.cc)
target_include_directories(farmhash SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/src/")
set_target_properties(farmhash
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${farmhash_BINARY_DIR}
    ARCHIVE_OUTPUT_DIRECTORY ${farmhash_BINARY_DIR}
)
target_compile_definitions(farmhash
  PUBLIC NAMESPACE_FOR_HASH_FUNCTIONS=farmhash)

add_library(op_contrib::farmhash ALIAS farmhash)