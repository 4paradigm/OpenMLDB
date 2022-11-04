cmake_minimum_required (VERSION 3.13)
if (POLICY CMP0068)
    cmake_policy(SET CMP0068 NEW)
endif ()
# Define file(GENERATE) behavior for relative paths.
if (POLICY CMP0070)
    cmake_policy(SET CMP0070 NEW)
endif ()
# target_sources: use absolute path for INTERFACE_SOURCES.
if (POLICY CMP0076)
    cmake_policy(SET CMP0076 NEW)
endif ()
# SWIG: use standard target name.
if (POLICY CMP0078)
    cmake_policy(SET CMP0078 NEW)
endif ()
# SWIG: use SWIG_MODULE_NAME property.
if (POLICY CMP0086)
    cmake_policy(SET CMP0086 NEW)
endif ()

list(APPEND CMAKE_MODULE_PATH
    "${CMAKE_SOURCE_DIR}/cmake-modules"
    "${CMAKE_SOURCE_DIR}/cmake")

project(openmldb)

if (CMAKE_BUILD_TYPE STREQUAL "")
    set(CMAKE_BUILD_TYPE RelWithDebInfo)
endif ()

if (NOT DEFINED CMAKE_PREFIX_PATH)
    set(CMAKE_PREFIX_PATH "${CMAKE_SOURCE_DIR}/.deps/usr")
endif()

message (STATUS "CMAKE_BUILD_TYPE: ${CMAKE_BUILD_TYPE}")
set(OPENMLDB_VERSION_MAJOR 0)
set(OPENMLDB_VERSION_MINOR 6)
set(OPENMLDB_VERSION_BUG 5)

function(get_commitid CODE_DIR COMMIT_ID)
    find_package(Git REQUIRED)
    execute_process(
        COMMAND ${GIT_EXECUTABLE} log -1 --pretty=format:%h
        OUTPUT_VARIABLE COMMIT_ID_TMP
        OUTPUT_STRIP_TRAILING_WHITESPACE
        ERROR_QUIET
        WORKING_DIRECTORY
        ${CODE_DIR}
    )
    set("${COMMIT_ID}" ${COMMIT_ID_TMP} PARENT_SCOPE)
endfunction()

# strip the given target
function(strip_exe TARGET_NAME)
    add_custom_command(TARGET ${TARGET_NAME} POST_BUILD
            COMMAND ${CMAKE_STRIP}
            ARGS $<TARGET_FILE:${TARGET_NAME}>)
endfunction()

get_commitid(${PROJECT_SOURCE_DIR} OPENMLDB_COMMIT_ID)
configure_file (
  "${PROJECT_SOURCE_DIR}/src/version.h.in"
  "${PROJECT_SOURCE_DIR}/src/version.h"
)
add_definitions('-g')
add_definitions('-Wall')
if (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    add_definitions(-Wreorder)
    add_definitions(-Wno-unused-private-field)
    add_definitions(-Wno-unused-function)
    add_definitions(-Wno-overloaded-virtual)
    add_definitions(-Wno-inconsistent-missing-override)
    add_definitions(-Wno-deprecated-declarations)
    add_definitions(-Wno-null-dereference)
    add_definitions(-Wno-deprecated-register)
    add_definitions(-Wno-c++17-extensions)
    add_definitions(-Wno-defaulted-function-deleted)
    add_definitions(-Wno-pessimizing-move)
    add_definitions(-Wno-unused-value)
    add_definitions(-Wno-\#pragma-messages)
endif ()
add_definitions(-DHAVE_STDBOOL_H=1)
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)

option(TESTING_ENABLE "Enable test" OFF)
option(TCMALLOC_ENABLE "Enable TCMALLOC" ON)
option(SQL_PYSDK_ENABLE "Enable sql pysdk" OFF)
option(SQL_JAVASDK_ENABLE "Enable sql javasdk" OFF)
option(MAC_TABLET_ENABLE "Enable Table on Mac OS" ON)
option(COVERAGE_ENABLE "Enable Coverage" OFF)
option(SANITIZER_ENABLE "Enable AddressSanitizer in Debug mode" OFF)
# add_library can reply on this variable
# see https://cmake.org/cmake/help/latest/variable/BUILD_SHARED_LIBS.html#variable:BUILD_SHARED_LIBS
option(BUILD_SHARED_LIBS "Enable build shared Libraries instead static" OFF)
option(TESTING_ENABLE_STRIP "Strip test binary that may help reduce storage space" OFF)

message (STATUS "MAC_TABLET_ENABLE: ${MAC_TABLET_ENABLE}")
set(CMAKE_RUNTIME_OUTPUT_DIRECTORY ${CMAKE_BINARY_DIR}/bin)
if (MAC_TABLET_ENABLE)
    add_compile_definitions(__mac_tablet__=1)
endif ()

set(SWIG_DIR ${CMAKE_PREFIX_PATH}/share/swig/4.0.1)
find_package(SWIG REQUIRED)
include(UseSWIG)

find_package(OpenSSL REQUIRED)
find_library(BRPC_LIBRARY brpc)

set(Protobuf_USE_STATIC_LIBS ON)
find_package(Protobuf 3.6 EXACT REQUIRED)
message(STATUS "Found Protobuf Libraries: ${Protobuf_LIBRARIES}")

find_library(GLOG_LIBRARY glog)
find_library(GFLAGS_LIBRARY gflags)
find_library(UNWIND_LIBRARY unwind)
find_library(LEVELDB_LIBRARY leveldb)
find_library(Z_LIBRARY z)
find_library(SNAPPY_LIBRARY snappy)

find_package(RocksDB)
if (RocksDB_FOUND)
    set(RocksDB_LIB RocksDB::rocksdb)
else()
    find_library(RocksDB_LIBRARY rocksdb)
    set(RocksDB_LIB ${RocksDB_LIBRARY})
endif()

# third party sources
include(FetchContent)
set(FETCHCONTENT_QUIET OFF)
include(farmhash)

if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
    set(OS_LIB ${CMAKE_THREAD_LIBS_INIT} rt)
    set(BRPC_LIBS ${BRPC_LIBRARY} ${Protobuf_LIBRARIES} ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} ${UNWIND_LIBRARY} ${OPENSSL_LIBRARIES} ${LEVELDB_LIBRARY} ${Z_LIBRARY} ${SNAPPY_LIBRARY} dl pthread ${OS_LIB})
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
    set(OS_LIB
        ${CMAKE_THREAD_LIBS_INIT}
        "-framework CoreFoundation"
        "-framework CoreGraphics"
        "-framework CoreData"
        "-framework CoreText"
        "-framework Security"
        "-framework Foundation"
        "-Wl,-U,_MallocExtension_ReleaseFreeMemory"
        "-Wl,-U,_ProfilerStart"
        "-Wl,-U,_ProfilerStop")
    set(BRPC_LIBS ${BRPC_LIBRARY} ${Protobuf_LIBRARIES} ${GLOG_LIBRARY} ${GFLAGS_LIBRARY} ${OPENSSL_LIBRARIES} ${LEVELDB_LIBRARY} ${Z_LIBRARY} ${SNAPPY_LIBRARY} dl pthread ${OS_LIB})
endif ()

if (SANITIZER_ENABLE)
    set(CMAKE_CXX_FLAGS_DEBUG "${CMAKE_CXX_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
    set(CMAKE_LINKER_FLAGS_DEBUG "${CMAKE_LINKER_FLAGS_DEBUG} -fno-omit-frame-pointer -fsanitize=address")
    message(STATUS "Enabled AddressSanitizer")
endif()

# add default option to ctest
# - --output-on-failure: print log for tests that fail
list(APPEND CMAKE_CTEST_ARGUMENTS --output-on-failure)

if (TESTING_ENABLE)
    enable_testing()
endif ()

set(test_list "")
if (COVERAGE_ENABLE)
    if (NOT CMAKE_COMPILER_IS_GNUCXX)
        message(FATAL_ERROR "Coverage is only supported on linux")
    endif ()
    find_program(LCOV_EXE NAMES lcov DOC "coverage requires lcov installed" REQUIRED)
    include(CodeCoverage)
    APPEND_COVERAGE_COMPILER_FLAGS()
endif ()

if (DEFINED ENV{CI})
  # suppress useless maven log (e.g download log) on CI environment
  set(MAVEN_FLAGS --batch-mode)
endif()

add_subdirectory(hybridse)

set(Boost_NO_BOOST_CMAKE ON)
set(BOOST_ROOT "${CMAKE_PREFIX_PATH}")
find_package(Boost COMPONENTS filesystem date_time regex REQUIRED)

set(VM_LIBS hybridse_sdk hybridse_core ${Boost_LIBRARIES})
set(LLVM_DIR "${CMAKE_PREFIX_PATH}/lib/cmake/llvm")
find_package(LLVM REQUIRED CONFIG)
message(STATUS "Found LLVM ${LLVM_PACKAGE_VERSION}")
message(STATUS "Using LLVMConfig.cmake in: ${LLVM_DIR}")
llvm_map_components_to_libnames(LLVM_LIBS support core orcjit nativecodegen)
message(STATUS "Using LLVM components: ${LLVM_LIBS}")
add_definitions(${LLVM_DEFINITIONS})

find_package(absl REQUIRED)
# modify absl::time_zone INTERFACE_LINK_LIBRARIES
set_target_properties(absl::time_zone PROPERTIES INTERFACE_LINK_LIBRARIES "\$<\$<PLATFORM_ID:Darwin>:-framework CoreFoundation>")

find_package(GTest REQUIRED)

# TODO(hw): dup with hybridse root cmake, need cleanup
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
        absl::status
        absl::statusor)

find_package(ICU COMPONENTS i18n io uc data)
if (NOT ICU_FOUND)
    message(STATUS "ICU find_package failed, try to use `find_library()` to find")
    find_library(ICU_I18N icui18n)
    find_library(ICU_IO icuio)
    find_library(ICU_UC icuuc)
    find_library(ICU_DATA icudata)
    set(ICU_LIBRARIES ${ICU_I18N} ${ICU_IO} ${ICU_UC} ${ICU_DATA})
endif ()
message(STATUS "Found ICU Libraries: ${ICU_LIBRARIES}")

list(APPEND file_based_test_driver_LIBS
        alternations
        test_case_options
        test_case_outputs
        test_case_mode
        logging
        path
        status
        unified_diff_oss
        ret_check
        )
set(ZETASQL_LIBS
        zetasql
        ${file_based_test_driver_LIBS}
        ${ABSL_LIBS}
        re2
        ${ICU_LIBRARIES}
        date_proto
        timeofday_proto
        latlng_proto
        )

if (TCMALLOC_ENABLE)
    set(CMAKE_EXE_LINKER_FLAGS "-fno-builtin-malloc -fno-builtin-calloc -fno-builtin-realloc -fno-builtin-free ${CMAKE_EXE_LINKER_FLAGS}")
endif()

configure_file(
    "${PROJECT_SOURCE_DIR}/src/config.h.in"
    "${PROJECT_SOURCE_DIR}/src/config.h")

include_directories(
  ${CMAKE_SOURCE_DIR}/include
  ${CMAKE_SOURCE_DIR}/hybridse/include
  ${CMAKE_SOURCE_DIR}/hybridse/src
  ${CMAKE_BINARY_DIR}/hybridse/src
  ${LLVM_INCLUDE_DIRS}
  ${Boost_INCLUDE_DIRS}
  ${CMAKE_PREFIX_PATH}/include)

link_directories(
    ${LLVM_LIBRARY_DIRS}
    ${Boost_LIBRARY_DIRS}
    ${CMAKE_PREFIX_PATH}/lib
    ${CMAKE_PREFIX_PATH}/lib64)

add_subdirectory(src)

if (COVERAGE_ENABLE AND CMAKE_COMPILER_IS_GNUCXX)
  # FIXME(aceforeverd): api_server_test failed to run on debug mode, skip temporarily
  SETUP_TARGET_FOR_COVERAGE_LCOV(NAME coverage
    EXECUTABLE ctest --force-new-ctest-process --output-on-failure -E "api_server_test"
    DEPENDENCIES ${test_list})
endif ()


if(CMAKE_INSTALL_PREFIX_INITIALIZED_TO_DEFAULT)
  # CMAKE_INSTALL_PREFIX default to /usr/local on unix.
  # At current phase we'd like to change it to a customized directory by default
  set(CMAKE_INSTALL_PREFIX "${CMAKE_SOURCE_DIR}/openmldb" CACHE PATH "OpenMLDB install directory" FORCE)
endif()

message(STATUS "CMAKE_INSTALL_PREFIX: ${CMAKE_INSTALL_PREFIX}")

FILE(GLOB OPENMLDB_SCRIPTS "${CMAKE_SOURCE_DIR}/release/bin/*")
FILE(GLOB OPENMLDB_CONFS "${CMAKE_SOURCE_DIR}/release/conf/*")
FILE(GLOB OPENMLDB_TOOLS "${CMAKE_SOURCE_DIR}/tools/*")

install(
  FILES ${OPENMLDB_SCRIPTS}
  TYPE BIN
  PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)
install(FILES ${OPENMLDB_CONFS} DESTINATION conf)
install(
  FILES ${OPENMLDB_TOOLS}
  DESTINATION tools
  PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)
