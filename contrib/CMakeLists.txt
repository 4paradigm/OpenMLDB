set_property(DIRECTORY PROPERTY EXCLUDE_FROM_ALL 1)

# add_contrib cmake_folder[ base_folder1[, ...base_folderN]]
# or add_contrib base_folder
function(add_contrib cmake_folder)
    if (ARGN)
        set(base_folders ${ARGN})
    else()
        set(base_folders ${cmake_folder})
    endif()

    foreach (base_folder ${base_folders})
        if (NOT IS_DIRECTORY "${CMAKE_CURRENT_SOURCE_DIR}/${base_folder}")
            message(FATAL_ERROR "No such base folder '${base_folder}' (for '${cmake_folder}' cmake folder). Typo in the base folder name?")
        endif()

        file(GLOB contrib_files "${base_folder}/*")
        if (NOT contrib_files)
            message(FATAL_ERROR "submodule ${base_folder} is missing or empty.")
        endif()
    endforeach()

    message(STATUS "Adding contrib module ${base_folders} (configuring with ${cmake_folder})")
    add_subdirectory (${cmake_folder})
endfunction()

add_contrib(simdjson)

# farmhash
add_library(farmhash ${CMAKE_CURRENT_SOURCE_DIR}/farmhash/src/farmhash.cc)
target_include_directories(farmhash SYSTEM PUBLIC "${CMAKE_CURRENT_SOURCE_DIR}/farmhash/src/")
set_target_properties(farmhash
    PROPERTIES
    LIBRARY_OUTPUT_DIRECTORY ${farmhash_BINARY_DIR}
    ARCHIVE_OUTPUT_DIRECTORY ${farmhash_BINARY_DIR}
)
target_compile_definitions(farmhash
  PUBLIC NAMESPACE_FOR_HASH_FUNCTIONS=farmhash)

add_library(op_contrib::farmhash ALIAS farmhash)

# rapidjson
add_contrib(rapidjson)

