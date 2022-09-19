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
    add_library(mini_cluster_bm_common mini_cluster_bm.cc)
    add_dependencies(mini_cluster_bm_common openmldb_proto)

    add_library(base_test sql_sdk_base_test.cc ../test/base_test.cc)
    add_dependencies(base_test hybridse_core openmldb_proto)

    set(THIRD_LIBS benchmark_main benchmark ${GTEST_LIBRARIES} yaml-cpp)

    add_executable(db_sdk_test db_sdk_test.cc)
    target_link_libraries(db_sdk_test base_test ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(sql_router_test sql_router_test.cc)
    target_link_libraries(sql_router_test base_test ${BIN_LIBS} benchmark_main benchmark ${GTEST_LIBRARIES})

    add_executable(sql_standalone_sdk_test sql_standalone_sdk_test.cc)
    target_link_libraries(sql_standalone_sdk_test base_test ${BIN_LIBS} ${GTEST_LIBRARIES})

    add_executable(node_adapter_test node_adapter_test.cc)
    target_link_libraries(node_adapter_test base_test ${BIN_LIBS} ${GTEST_LIBRARIES})

    add_executable(sql_sdk_test sql_sdk_test.cc)
    target_link_libraries(sql_sdk_test base_test ${BIN_LIBS} ${GTEST_LIBRARIES} )

    add_executable(sql_cluster_test sql_cluster_test.cc)
    target_link_libraries(sql_cluster_test base_test ${BIN_LIBS} ${GTEST_LIBRARIES})

    add_executable(sql_request_row_test sql_request_row_test.cc)
    target_link_libraries(sql_request_row_test base_test ${BIN_LIBS} ${ZETASQL_LIBS} ${THIRD_LIBS})

    add_executable(mini_cluster_batch_bm mini_cluster_batch_bm.cc)
    target_link_libraries(mini_cluster_batch_bm mini_cluster_bm_common base_test ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(mini_cluster_request_batch_bm mini_cluster_request_batch_bm.cc)
    target_link_libraries(mini_cluster_request_batch_bm mini_cluster_bm_common base_test ${BIN_LIBS} ${THIRD_LIBS})

    add_executable(mini_cluster_request_bm mini_cluster_request_bm.cc)
    target_link_libraries(mini_cluster_request_bm mini_cluster_bm_common base_test ${BIN_LIBS} ${THIRD_LIBS})
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

# To get the final SDK, we need some libraries
# Below, we will find and integrate these required libraries

# We need hybridse_flags, but it hasn't been mentioned and needs to be added manually
list(APPEND required_libs ${BIN_LIBS} ${SDK_LIBS} ${LINK_LIBS} hybridse_flags)

# In required_libs, some libraries do not need or require different path lookup methods
# ${ABSL_LIBS} ${LLVM_LIBS} : we need to recursively look up all the libraries they depend on
# ${file_based_test_driver_LIBS} : we don't need the libraries, so we remove them
# unwind : in ${LINK_LIBS}, we can get it by ${UNWIND_LIBRARY} which in ${BRPC_LIBS}
# common tcmalloc_minimal : in ${BIN_LIBS}, we don't need them
# zookeeper_mt : we get it by ${zookeeper_mt_LIBRARY}
# zetasql : we get it by ${zetasql_LIBRARY}
# re2 : we get it by ${re2_LIBRARY}
# date_proto timeofday_proto latlng_proto : we don't need them
# -lpthread : in zookeeper_mt
# dl :we get it by ${dl_LIBRARY}
# pthread : we get it by ${pthread_LIBRARY}
# rt : we get it by ${rt_LIBRARY}
list(REMOVE_ITEM required_libs ${ABSL_LIBS} ${LLVM_LIBS} ${file_based_test_driver_LIBS} unwind    common tcmalloc_minimal zookeeper_mt    zetasql re2 date_proto timeofday_proto latlng_proto    -lpthread dl pthread rt)

# Process libraries in required_libs :
# 1. with path itself
# 2. get path by $<TARGET_FILE:${X}>
# libraries_path sava paths of  the all libraries
foreach(X  IN LISTS required_libs)
    if(EXISTS ${X})
        list(APPEND libraries_path ${X})
        continue()
    else()
    list(APPEND libraries_path $<TARGET_FILE:${X}>)
    endif()
endforeach()

# get path by ${X_LIBRARY}
list(APPEND libraries_path ${zetasql_LIBRARY} ${zookeeper_mt_LIBRARY} ${re2_LIBRARY} ${dl_LIBRARY} ${pthread_LIBRARY} ${rt_LIBRARY} ${m_LIBRARY})

# lib_name is the name of a library
# all_libs save all the libraries name of absl and LLVM which find recursively
function(get_link_libraries lib_name all_libs)
    # Determine whether it is an absl... library or a LLVM... library according to the first four digits of the ${lib_name}
    string(SUBSTRING ${lib_name} 0 4 filter)
    # Get ${lib_name}'s link library
    get_property(_links TARGET ${lib_name} PROPERTY INTERFACE_LINK_LIBRARIES)
    # We should filter out libraries in the form of $<LINK_ONLY:...>, and don't include absl or LLVM, like $<LINK_ONLY:$<$<BOOL:>:"advapi32">>
    list(FILTER _links INCLUDE REGEX ".*${filter}.*")
    foreach(X IN LISTS _links)
        # Some libraries exist in the form similar to $<LINK_ONLY:absl::base>. We need to filter them out through regular expressions
        string(REGEX MATCH "${filter}[^>]+" Y ${X})
        list(FIND ${all_libs} ${Y} _index)
        # If there is no ${Y} in ${all_libs}, we recursively look for B
        if(_index EQUAL -1)
            get_link_libraries(${Y} ${all_libs})
        endif()
    endforeach()
    set(${all_libs} "${${all_libs}};${lib_name}" PARENT_SCOPE)
endfunction(get_link_libraries)

# ${ABSL_LIBS} and ${LLVM_LIBS} need to recursively look up
# We need absl::statusor, but it's not mentioned
list(APPEND ABSL_AND_LLVM ${ABSL_LIBS} absl::statusor ${LLVM_LIBS})
foreach(X IN LISTS ABSL_AND_LLVM)
    get_link_libraries(${X} absl_llvm_libs)
endforeach()
# Remove null values
list(REMOVE_ITEM absl_llvm_libs "")

# Find path of libraries in ${absl_llvm_libs}
foreach(X  IN LISTS absl_llvm_libs)
    get_property(_loc TARGET ${X} PROPERTY LOCATION)
    list(APPEND absl_llvm_path ${_loc})
endforeach()

list(APPEND to_openmldbsdk $<TARGET_FILE:openmldb_api> ${absl_llvm_path} ${libraries_path})

add_custom_command(OUTPUT libopenmldbsdk.a
COMMAND ar crsT libopenmldbsdk.a
    ${to_openmldbsdk}
)
add_custom_target(_openmldbsdk ALL DEPENDS libopenmldbsdk.a)
add_library(openmldbsdk STATIC IMPORTED GLOBAL)

set_target_properties(openmldbsdk PROPERTIES
IMPORTED_LOCATION ${CMAKE_CURRENT_BINARY_DIR}/libopenmldbsdk.a
)

if(TESTING_ENABLE)
    add_executable(openmldb_api_test openmldb_api_test.cc)
    target_link_libraries(openmldb_api_test openmldbsdk base_test ${BIN_LIBS} ${GTEST_LIBRARIES})
endif()

FILE(GLOB USER_HEADER 
    "${PROJECT_SOURCE_DIR}/src/sdk/openmldb_api.h"
)
FILE(GLOB USER_HEADER_SDK
    "${PROJECT_SOURCE_DIR}/src/sdk/result_set.h"
    "${PROJECT_SOURCE_DIR}/src/sdk/base_schema.h"
)
FILE(GLOB USER_LIB
    "${PROJECT_SOURCE_DIR}/build/src/sdk/libopenmldbsdk.a"
)

install(
    FILES ${USER_HEADER}
    DESTINATION include
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)
install(
    FILES ${USER_HEADER_SDK}
    DESTINATION include/sdk
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)
install(
    FILES ${USER_LIB}
    DESTINATION lib
    PERMISSIONS OWNER_EXECUTE OWNER_WRITE OWNER_READ GROUP_EXECUTE GROUP_READ WORLD_EXECUTE WORLD_READ
)