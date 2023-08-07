add_library(_simdjson ${CMAKE_CURRENT_SOURCE_DIR}/simdjson.cpp)
target_include_directories(_simdjson SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/")

# simdjson is using its own CPU dispatching and get confused if we enable AVX/AVX2 flags.
if(ARCH_AMD64)
    target_compile_options(_simdjson PRIVATE -mno-avx -mno-avx2)
endif()

add_library(op_contrib::simdjson ALIAS _simdjson)

