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
            message(STATUS "submodule contrib/${base_folder} is missing or empty, update submodule manully ...")
            execute_process(
                COMMAND ${GIT_EXECUTABLE} submodule update --init -- ${CMAKE_SOURCE_DIR}/contrib/${base_folder}
                WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}
            )
        endif()
    endforeach()

    message(STATUS "Adding contrib module ${base_folders} (configuring with ${cmake_folder})")
    add_subdirectory (${cmake_folder})
endfunction()

add_contrib(simdjson)

if (BUILD_SHARED_LIBS)
    add_contrib(gflags-cmake gflags)
    add_contrib(glog-cmake glog)
endif()
