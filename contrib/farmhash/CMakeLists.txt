
FetchContent_Declare(
  farmhash
  GIT_REPOSITORY https://github.com/google/farmhash.git
  GIT_TAG 0d859a811870d10f53a594927d0d0b97573ad06d
)
FetchContent_MakeAvailable(farmhash)

# TODO: recommanded compile flag: '-mavx -maes -O3'
add_library(farmhash ${farmhash_SOURCE_DIR}/src/farmhash.cc)
set_target_properties(farmhash
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${farmhash_BINARY_DIR}
    ARCHIVE_OUTPUT_DIRECTORY ${farmhash_BINARY_DIR}
)
target_compile_definitions(farmhash
  PUBLIC NAMESPACE_FOR_HASH_FUNCTIONS=farmhash)
