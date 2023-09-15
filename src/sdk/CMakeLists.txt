# Copyright 2021 4Paradigm
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

if(TESTING_ENABLE)
    set(THIRD_LIBS benchmark_main benchmark ${GTEST_LIBRARIES} yaml-cpp)

    add_library(mini_cluster_bm_common mini_cluster_bm.cc)
    add_dependencies(mini_cluster_bm_common openmldb_proto)

    add_library(base_test sql_sdk_base_test.cc)
    add_dependencies(base_test openmldb_test_base hybridse_core openmldb_proto)

    set(SDK_TEST_DEPS base_test openmldb_test_base)

    add_executable(db_sdk_test db_sdk_test.cc)
    target_link_libraries(db_sdk_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(result_set_sql_test result_set_sql_test.cc)
    target_link_libraries(result_set_sql_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(sql_router_test sql_router_test.cc)
    target_link_libraries(sql_router_test ${SDK_TEST_DEPS} ${BIN_LIBS} benchmark_main benchmark ${GTEST_LIBRARIES})

    add_executable(sql_standalone_sdk_test sql_standalone_sdk_test.cc)
    target_link_libraries(sql_standalone_sdk_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${GTEST_LIBRARIES})

    add_executable(node_adapter_test node_adapter_test.cc)
    target_link_libraries(node_adapter_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${GTEST_LIBRARIES})

    add_executable(sql_sdk_test sql_sdk_test.cc)
    target_link_libraries(sql_sdk_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${GTEST_LIBRARIES} )

    add_executable(sql_sdk_run sql_sdk_run.cc)
    target_link_libraries(sql_sdk_run ${SDK_TEST_DEPS} ${BIN_LIBS})
    set_target_properties(
        sql_sdk_run
        PROPERTIES
        RUNTIME_OUTPUT_DIRECTORY ${CMAKE_CURRENT_BINARY_DIR})

    add_executable(sql_cluster_test sql_cluster_test.cc)
    target_link_libraries(sql_cluster_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${GTEST_LIBRARIES})

    add_executable(sql_request_row_test sql_request_row_test.cc)
    target_link_libraries(sql_request_row_test ${SDK_TEST_DEPS} ${BIN_LIBS} ${ZETASQL_LIBS} ${THIRD_LIBS})

    add_executable(mini_cluster_batch_bm mini_cluster_batch_bm.cc)
    target_link_libraries(mini_cluster_batch_bm mini_cluster_bm_common ${SDK_TEST_DEPS} ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(mini_cluster_request_batch_bm mini_cluster_request_batch_bm.cc)
    target_link_libraries(mini_cluster_request_batch_bm mini_cluster_bm_common ${SDK_TEST_DEPS} ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(mini_cluster_request_bm mini_cluster_request_bm.cc)
    target_link_libraries(mini_cluster_request_bm mini_cluster_bm_common ${SDK_TEST_DEPS} ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(split_test split_test.cc)
    target_link_libraries(split_test ${BIN_LIBS})
endif()

set(SDK_LIBS openmldb_sdk openmldb_catalog client zk_client schema openmldb_flags openmldb_codec openmldb_proto base hybridse_sdk zookeeper_mt)

if(SQL_PYSDK_ENABLE)
    find_package(Python3 COMPONENTS Interpreter Development)
    set_property(SOURCE sql_router_sdk.i PROPERTY CPLUSPLUS ON)
    if (APPLE)
        set_property(SOURCE sql_router_sdk.i PROPERTY COMPILE_OPTIONS -python)
    else ()
        set_property(SOURCE sql_router_sdk.i PROPERTY COMPILE_OPTIONS -py3)
    endif ()
    set(UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
    swig_add_library(sql_router_sdk
            TYPE SHARED
            LANGUAGE python
            OUTPUT_DIR ${CMAKE_BINARY_DIR}/sql_pysdk/openmldb
            SOURCES sql_router_sdk.i)
    target_include_directories(sql_router_sdk PRIVATE ${Python3_INCLUDE_DIRS})
    target_link_libraries(sql_router_sdk ${SDK_LIBS})

    # Python in MacOS does not support load dylib file
    if(APPLE)
      set_target_properties(sql_router_sdk PROPERTIES
               SUFFIX ".so")
      set_property(TARGET sql_router_sdk APPEND PROPERTY
        LINK_FLAGS "-flat_namespace -undefined suppress")
    endif()

    # Find if python module MODULE_NAME is available,
    # if not install it to the Python user install directory.
    function(search_python_module MODULE_NAME)
        execute_process(
            COMMAND ${Python3_EXECUTABLE} -c "import ${MODULE_NAME}; print(${MODULE_NAME}.__version__)"
            RESULT_VARIABLE _RESULT
            OUTPUT_VARIABLE MODULE_VERSION
            ERROR_QUIET
            OUTPUT_STRIP_TRAILING_WHITESPACE
            )
        if(${_RESULT} STREQUAL "0")
            message(STATUS "Found python module: ${MODULE_NAME} (found version \"${MODULE_VERSION}\")")
        else()
            message(WARNING "Can't find python module \"${MODULE_NAME}\", user install it using pip...")
            execute_process(
                COMMAND ${Python3_EXECUTABLE} -m pip install --upgrade --user ${MODULE_NAME}
                OUTPUT_STRIP_TRAILING_WHITESPACE
                )
        endif()
    endfunction()
    # Look for required python modules
    search_python_module(setuptools)
    search_python_module(wheel)

    if(APPLE)
        set(PYTHON_PLATFORM macosx_10_15_x86_64)
        add_custom_target(strip_python_so ALL DEPENDS sql_router_sdk
                COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_router_sdk> ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/openmldb/native/
                COMMAND echo "Do not strip library for MacOS, refer to https://github.com/4paradigm/OpenMLDB/issues/905")
    else()
        set(PYTHON_PLATFORM manylinux1_x86_64)
        add_custom_target(strip_python_so ALL DEPENDS sql_router_sdk
            COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_router_sdk> ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/openmldb/native/
            COMMAND strip ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/openmldb/native/_sql_router_sdk.so)
    endif()

    add_custom_target(cp_python_sdk_so ALL DEPENDS strip_python_so
            COMMAND ${CMAKE_COMMAND} -E copy  ${CMAKE_BINARY_DIR}/sql_pysdk/openmldb/sql_router_sdk.py ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/openmldb/native/
            COMMENT "copy generated native library and sql_router_sdk python file to python project"
            COMMAND cd ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/ && ${Python3_EXECUTABLE} setup.py bdist_wheel --plat-name ${PYTHON_PLATFORM}
            COMMAND cd ${PROJECT_SOURCE_DIR}/python/openmldb_tool/ && ${Python3_EXECUTABLE} setup.py bdist_wheel
            BYPRODUCTS
                ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/build
                ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/dist
                ${PROJECT_SOURCE_DIR}/python/openmldb_sdk/openmldb.egg-info
                ${PROJECT_SOURCE_DIR}/python/openmldb_tool/build
                ${PROJECT_SOURCE_DIR}/python/openmldb_tool/dist
                ${PROJECT_SOURCE_DIR}/python/openmldb_tool/openmldb_tool.egg-info
                )

endif()

if(SQL_JAVASDK_ENABLE)
    set_property(SOURCE sql_router_sdk.i PROPERTY CPLUSPLUS ON)
    find_package(Java COMPONENTS Development REQUIRED)
    message(STATUS "Found Java: ${Java_JAVA_EXECUTABLE} (found version \"${Java_VERSION_STRING}\")")
    find_package(JNI REQUIRED)
    message(STATUS "Found JNI: ${JNI_FOUND}")

    set_property(SOURCE sql_router_sdk.i PROPERTY COMPILE_OPTIONS -package com._4paradigm.openmldb)
    swig_add_library(sql_jsdk
            TYPE SHARED
            LANGUAGE java
            OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/openmldb-native/src/main/java/com/_4paradigm/openmldb/
            SOURCES sql_router_sdk.i)
    target_include_directories(sql_jsdk PRIVATE ${JNI_INCLUDE_DIRS})
    target_compile_options(sql_jsdk PRIVATE -w)

    add_dependencies(sql_jsdk hybridse_jsdk_core_static)
    if(APPLE)
        target_link_libraries(sql_jsdk PRIVATE "-Wl,-force_load $<TARGET_FILE:hybridse_jsdk_core_static>" ${SDK_LIBS} ${Boost_filesystem_LIBRARY})
    else()
        target_link_libraries(sql_jsdk PRIVATE "-Wl,--whole-archive $<TARGET_FILE:hybridse_jsdk_core_static> -Wl,--no-whole-archive" ${SDK_LIBS} ${Boost_filesystem_LIBRARY})
    endif()

    if(APPLE)
      set_target_properties(sql_jsdk PROPERTIES
               SUFFIX ".dylib")
      set_property(TARGET sql_jsdk APPEND PROPERTY
        LINK_FLAGS "-flat_namespace -undefined suppress")
    endif()

    # this target ensure cpp compiled libraries were copied into java project:
    # 1. cp sql_jsdk into openmldb-native
    # 2. cp hybridse jsdk into hybridse-native (inside dependency cp_hybridse_native_so)
    add_custom_target(cp_native_so ALL
        COMMAND ${CMAKE_COMMAND} -E make_directory ${PROJECT_SOURCE_DIR}/java/openmldb-native/src/main/resources
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_jsdk> ${PROJECT_SOURCE_DIR}/java/openmldb-native/src/main/resources/
        COMMENT "copy generated native library to java project")
    add_dependencies(cp_native_so cp_hybridse_native_so)

    add_custom_target(sql_javasdk_package ALL
        COMMAND ./mvnw clean package -DskipTests=true -Dscalatest.skip=true -Dwagon.skip=true -Dmaven.test.skip=true ${MAVEN_FLAGS}
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/java)
    add_dependencies(sql_javasdk_package cp_native_so)

    # Install TaskManager binary and libraries
    FILE(GLOB TASKMANAGER_BIN "${PROJECT_SOURCE_DIR}/java/openmldb-taskmanager/target/openmldb-taskmanager-binary/bin/*")
    FILE(GLOB TASKMANAGER_CONF "${PROJECT_SOURCE_DIR}/java/openmldb-taskmanager/target/openmldb-taskmanager-binary/conf/log4j*")
    FILE(GLOB TASKMANAGER_LIB "${PROJECT_SOURCE_DIR}/java/openmldb-taskmanager/target/openmldb-taskmanager-binary/lib/*")
    install(FILES ${TASKMANAGER_BIN}
            DESTINATION taskmanager/bin/
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ)
    install(FILES ${TASKMANAGER_CONF}
            DESTINATION taskmanager/conf/
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)
    install(FILES ${TASKMANAGER_LIB}
            DESTINATION taskmanager/lib/
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)

    # Install SyncTool binary and libraries
    FILE(GLOB SYNCTOOL_BIN "${PROJECT_SOURCE_DIR}/java/openmldb-synctool/target/openmldb-synctool-binary/bin/*")
    FILE(GLOB SYNCTOOL_CONF "${PROJECT_SOURCE_DIR}/java/openmldb-synctool/target/openmldb-synctool-binary/conf/log4j*")
    FILE(GLOB SYNCTOOL_LIB "${PROJECT_SOURCE_DIR}/java/openmldb-synctool/target/openmldb-synctool-binary/lib/*")
    install(FILES ${SYNCTOOL_BIN}
            DESTINATION synctool/bin/
            PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ)
    install(FILES ${SYNCTOOL_CONF}
            DESTINATION synctool/conf/
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)
    install(FILES ${SYNCTOOL_LIB}
            DESTINATION synctool/lib/
            PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ)
endif()

message(STATUS "build cxx sdk")
# see cicd workflow, we can't build c++ sdk on macos
if (APPLE)
    message(STATUS "skip c++ sdk build on macos")
    return()
endif()

add_library(openmldb_api STATIC openmldb_api.cc)
target_include_directories (
    openmldb_api
    PUBLIC
    ${PROJECT_SOURCE_DIR}/src
    ${PROJECT_SOURCE_DIR}/src/sdk
    ${PROJECT_SOURCE_DIR}/hybridse/src
    ${PROJECT_SOURCE_DIR}/hybridse/include
    ${PROJECT_SOURCE_DIR}/.deps/usr/include
)
# openmldb_sdk is enough for link, but we need to add all libs to one, add lib hint here
target_link_libraries(openmldb_api openmldb_sdk 
    openmldb_codec
    openmldb_catalog
    schema
    client
    zk_client
    base
    openmldb_flags
    hybridse_sdk
    hybridse_core
    hybridse_flags
)

if(TESTING_ENABLE)
    # load cxx sdk library built below is difficult, cuz we need more libs e.g. mini cluster/stdc++fs to build test
    # so we just use lower libs to build test
    # P.S. openmldb_sdk contains a lot, but it's not a good idea to use it to include mini cluster ..., we should reduce ibopenmldbsdk.a
    add_executable(openmldb_api_test openmldb_api_test.cc)
    target_link_libraries(openmldb_api_test ${EXPORTER_LIBS} ${GTEST_LIBRARIES})
endif()

if(APPLE)
    message(STATUS "skip c++ sdk build")
    return()
endif()

# To get the final SDK, we need add built-in libraries and third party libraries 
# Below, we will find and integrate these required libraries

# dfs target here, if needs path, get after find all targets
macro(getAllLinkedLibraries iTarget iReturnValue)
   if(NOT TARGET ${iTarget})
        # message(STATUS "${iTarget} is not a target")
        # ignored
    else()
        if(NOT ${iTarget} IN_LIST ${iReturnValue}) # avoid too much duplicate
            list(APPEND ${iReturnValue} ${iTarget})
        endif()

        get_target_property(linkedLibrairies ${iTarget} INTERFACE_LINK_LIBRARIES)
        if(NOT "${linkedLibrairies}" STREQUAL "linkedLibrairies-NOTFOUND")
            FOREACH(linkedLibrary ${linkedLibrairies})
                getAllLinkedLibraries(${linkedLibrary} ${iReturnValue})
            ENDFOREACH()
        endif()
    endif()
endmacro()

# lib is target or lib name
function(get_lib_path X RET)
    if (NOT TARGET ${X})
        if(EXISTS ${X})
            set(RET_V ${X})
        else()
            find_library(RET_V ${X})
        endif()
    else()
        # if target has no location, handle it before call this function
        get_target_property(type ${X} TYPE)
        if (NOT ${type} STREQUAL "INTERFACE_LIBRARY")
            get_target_property(RET_V ${X} LOCATION)
        endif()
        # message(STATUS "get ${X} path: ${RET_V}")
    endif()
    if("${RET_V}" STREQUAL "RET_V-NOTFOUND")
        # message(STATUS "library ${X} path not found, set empty")
        set(RET_V "")
    endif()
    set(${RET} ${RET_V} PARENT_SCOPE)
endfunction(get_lib_path)

# get linked libraries built in OpenMLDB
get_target_property(CXXSDK_TGTS openmldb_api LINK_LIBRARIES)
list(PREPEND CXXSDK_TGTS openmldb_api)
list(APPEND CXXSDK_TGTS farmhash) # not a standalone lib, build-in, so use target file
list(REMOVE_ITEM CXXSDK_TGTS gcov)
set(CXXSDK_LIBS )
foreach(X IN LISTS CXXSDK_TGTS)
    # build-in libraries are targets without location, add lib path(TARGET_FILE in file can get location)
    list(APPEND CXXSDK_LIBS $<TARGET_FILE:${X}>)
endforeach()

# CXXSDK_THIRDPARTY_LIBS save all required libraries path
set(CXXSDK_THIRDPARTY_LIBS )

# 1. zetasql & it's dependencies use ZETASQL_LIBS(include ABSL_LIBS, ICU_LIBRARIES)
foreach(X IN LISTS ZETASQL_LIBS)
    get_lib_path(${X} Y)
    # message(STATUS "get ${X} path: ${Y}")
    # to simplify, we handle absl implicit link below(some absl lib can't get location by get_target_property), so no recursive get here
    if(NOT "${Y}" STREQUAL "")
        list(APPEND CXXSDK_THIRDPARTY_LIBS ${Y})      
    endif()
endforeach()

# 2. absl llvm libs(some libs needs to be linked explicitly)
set(ABSL_LLVM_LIBS ${ABSL_LIBS} ${LLVM_LIBS})
# set(ABSL_LLVM_LIBS ${LLVM_LIBS})
# handle absl libs, need to recursively look up, target may need more deps
foreach(X IN LISTS ABSL_LLVM_LIBS)
    set(_tgts "")
    getAllLinkedLibraries(${X} _tgts)
    list(APPEND ABSL_LLVM_TGTS ${_tgts})
    # message(STATUS "get ${X} linked libs: ${_tgts}")
endforeach()
list(REMOVE_DUPLICATES ABSL_LLVM_TGTS)

# get absl llvm libs path
foreach(X IN LISTS ABSL_LLVM_TGTS)
    get_lib_path(${X} Y)
    # message(STATUS "get ${X} path: ${Y}")
    list(APPEND CXXSDK_THIRDPARTY_LIBS ${Y})
endforeach()

# 3. extras
# hybrid_core needs yaml_libs
set(EXTRA_LIBS ${BRPC_LIBS} ${YAML_CPP_LIBRARIES} ${zookeeper_mt_LIB} ${Boost_LIBRARIES})
foreach(X IN LISTS EXTRA_LIBS)
    get_lib_path(${X} Y)
    # message(STATUS "get ${X} path: ${Y}")
    list(APPEND CXXSDK_THIRDPARTY_LIBS ${Y})
endforeach()
# brpc includes some so, but we don't need them, so remove them
list(FILTER CXXSDK_THIRDPARTY_LIBS EXCLUDE REGEX ".*\.so$")
# for apple
list(FILTER CXXSDK_THIRDPARTY_LIBS EXCLUDE REGEX ".*\.dylib$")
list(FILTER CXXSDK_THIRDPARTY_LIBS EXCLUDE REGEX ".*\.tbd$")

set(CXXSDK_NAME libopenmldbsdk.a)
list(APPEND TOTAL_LIBS ${CXXSDK_LIBS} ${CXXSDK_THIRDPARTY_LIBS})
# message(STATUS "cxx sdk link libs: ${TOTAL_LIBS}")

# gen mri file
list(JOIN TOTAL_LIBS "\nADDLIB " ADD_LIBS_PART)
string(CONCAT ADD_LIBS "ADDLIB " ${ADD_LIBS_PART})
file(GENERATE OUTPUT cxxsdk.mri CONTENT "CREATE ${CXXSDK_NAME}\n${ADD_LIBS}\nSAVE\nEND")
add_custom_target(gen_mri DEPENDS cxxsdk.mri openmldb_api)

# ~2G, too big lib
add_custom_target(pack_cxxsdk ALL
COMMAND rm -f ${CXXSDK_NAME}
COMMAND ar -TM < cxxsdk.mri
DEPENDS gen_mri
)

if(INSTALL_CXXSDK)
    FILE(GLOB USER_HEADER 
        "${PROJECT_SOURCE_DIR}/src/sdk/openmldb_api.h"
    )
    FILE(GLOB USER_HEADER_SDK
        "${PROJECT_SOURCE_DIR}/hybridse/include/sdk/result_set.h"
        "${PROJECT_SOURCE_DIR}/hybridse/include/sdk/base_schema.h"
    )
    FILE(GLOB USER_LIB
        "${CMAKE_CURRENT_BINARY_DIR}/${CXXSDK_NAME}"
    )

    install(
        FILES ${USER_HEADER}
        DESTINATION include/
        PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ
    )
    install(
        FILES ${USER_HEADER_SDK}
        DESTINATION include/sdk/
        PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ
    )
    install(
        FILES ${USER_LIB}
        DESTINATION lib/
        PERMISSIONS OWNER_WRITE OWNER_READ GROUP_READ WORLD_READ
    )
endif()
