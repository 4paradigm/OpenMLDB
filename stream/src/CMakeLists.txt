cmake_minimum_required(VERSION 3.13)

add_compile_definitions(TABLE_NO_LOCK=ON)

message(STATUS "CMAKE_SYSTEM_NAME = ${CMAKE_SYSTEM_NAME}")

if (NOT DEFINED CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH ${CMAKE_SOURCE_DIR}/.deps/usr)
endif ()

message(STATUS "CMAKE_PREFIX_PATH: ${CMAKE_PREFIX_PATH}")

list(APPEND CMAKE_MODULE_PATH "${PROJECT_SOURCE_DIR}/cmake")
message(STATUS "CMAKE_MODULE_PATH: ${CMAKE_MODULE_PATH}")

set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

function(compile_lib LIB_NAME DIR DEPEND_FILE_LIST DEPEND_LIBS)
    set(FILE_STR_LIST "")
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    message(STATUS "${LIB_NAME} SRC_FILES: ${SRC_FILES}")
    foreach (SRC_FILE ${SRC_FILES})
        if (NOT SRC_FILE MATCHES ".*_test.cc" AND NOT SRC_FILE MATCHES ".*_bm.cc")
            set(FILE_STR_LIST "${FILE_STR_LIST} ${SRC_FILE}")
        endif ()
    endforeach ()
    set(FILE_STR_LIST "${FILE_STR_LIST} ${DEPEND_FILE_LIST}")
    message(STATUS "FILE_STR_LIST: ${FILE_STR_LIST}")
    string(REPLACE " " ";" FILE_LIST ${FILE_STR_LIST})
    add_library(${LIB_NAME} ${FILE_LIST})
    target_link_libraries(${LIB_NAME} ${COMMON_LIBS} ${DEPEND_LIBS})
endfunction(compile_lib)

function(compile_test DIR)
    set(TEST_LIBS ${COMMON_LIBS} ${LIBS})
    if (CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.1")
        # GNU implementation prior to 9.1 requires linking with -lstdc++fs
        list(APPEND TEST_LIBS stdc++fs)
    endif ()
    file(GLOB_RECURSE SRC_FILES ${DIR}/*.cc)
    foreach (SRC_FILE ${SRC_FILES})
        if (SRC_FILE MATCHES ".*_test.cc")
            file(RELATIVE_PATH RELATIVE_TEST_PATH ${CMAKE_CURRENT_SOURCE_DIR} ${SRC_FILE})
            get_filename_component(TEST_TARGET_DIR ${RELATIVE_TEST_PATH} DIRECTORY)
            get_filename_component(TEST_TARGET_NAME ${RELATIVE_TEST_PATH} NAME_WE)
            add_executable(${TEST_TARGET_NAME} ${SRC_FILE})
            list(APPEND test_list ${TEST_TARGET_NAME})
            target_link_libraries(${TEST_TARGET_NAME} ${TEST_LIBS} ${GTEST_LIBRARIES})
            set_target_properties(${TEST_TARGET_NAME}
                    PROPERTIES
                    RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR})
            add_test(${TEST_TARGET_NAME}
                    ${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}
                    --gtest_output=xml:${CMAKE_CURRENT_BINARY_DIR}/${TEST_TARGET_DIR}/${TEST_TARGET_NAME}.xml)
        endif ()
    endforeach ()
    set(test_list ${test_list} PARENT_SCOPE)
endfunction(compile_test)

find_package(absl REQUIRED)
list(
        APPEND
        ABSL_LIBS
        absl::algorithm
        absl::base
        absl::cord
        absl::debugging
        absl::flat_hash_map
        absl::flags
        absl::flags_parse
        absl::memory
        absl::meta
        absl::numeric
        absl::random_random
        absl::strings
        absl::strings_internal
        absl::synchronization
        absl::time
        absl::status)


set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT "${CMAKE_PREFIX_PATH}")

find_package(Boost COMPONENTS filesystem date_time regex REQUIRED)
find_library(GFLAGS_LIBRARY gflags)
find_library(GLOG_LIBRARY glog)
find_package(GTest REQUIRED)

include_directories(
        ${CMAKE_PREFIX_PATH}/include
        ${CMAKE_CURRENT_SOURCE_DIR}
)

link_directories(
        ${CMAKE_PREFIX_PATH}/lib
        ${Boost_LIBRARY_DIRS}
)

set(COMMON_LIBS ${ABSL_LIBS} ${Boost_LIBRARIES} ${GFLAGS_LIBRARY} ${GLOG_LIBRARY} ${GTEST_LIBRARIES})
message(STATUS "COMMON_LIBRARIES: ${ABSL_LIBS} ${Boost_LIBRARIES} ${GFLAGS_LIBRARY} ${GLOG_LIBRARY} ${GTEST_LIBRARIES}")
set(LIBS stream util executor storage openmldb_flags)

compile_lib(util common "" "")
compile_lib(stream stream "" "")
compile_lib(executor executor "" util)

if (TESTING_ENABLE)
    compile_test(stream)
    compile_test(common)
    compile_test(executor)
    compile_test(structure)
endif ()

add_executable(interval_join main.cc)
target_link_libraries(interval_join ${LIBS} ${COMMON_LIBS})
