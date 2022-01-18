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

include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)


set(SDK_DEPS_LIBS hybridse_core hybridse_flags
		${yaml_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${ZETASQL_LIBS} ${BRPC_LIBS} ${OS_LIB} )

# split sdk test and bm
set(SDK_SRC_FILE_LIST)
set(SDK_TEST_FILE_LIST)
file(GLOB_RECURSE BM_FILES ${CMAKE_CURRENT_SOURCE_DIR}/*.cc)
foreach(SRC_FILE ${BM_FILES})
	if (SRC_FILE MATCHES ".*_test.cc")
		list(APPEND SDK_TEST_FILE_LIST ${SRC_FILE})
	else()
		list(APPEND SDK_SRC_FILE_LIST ${SRC_FILE})
	endif()
endforeach()

# hybridse sdk library
add_library(hybridse_sdk STATIC ${SDK_SRC_FILE_LIST})
target_link_libraries(hybridse_sdk ${SDK_DEPS_LIBS})

set_property(SOURCE hybridse_interface_core.i PROPERTY CPLUSPLUS ON)

set(sdk_core_deps_libs
		hybridse_core hybridse_flags ${yaml_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${ZETASQL_LIBS} ${COMMON_LIBS} )
set_property(SOURCE hybridse_interface_core.i PROPERTY COMPILE_OPTIONS -package com._4paradigm)

if(SQL_JAVASDK_ENABLE)
  swig_add_library(hybridse_jsdk_core
    TYPE SHARED
    LANGUAGE java
    OUTPUT_DIR ${CMAKE_SOURCE_DIR}/java/hybridse-native/src/main/java/com/_4paradigm/
    SOURCES hybridse_interface_core.i)
  target_include_directories(hybridse_jsdk_core PRIVATE ${JNI_INCLUDE_DIRS})
  target_compile_options(hybridse_jsdk_core PRIVATE -w)
  target_link_libraries(hybridse_jsdk_core PRIVATE glog ${sdk_core_deps_libs})

  if(APPLE)
    set_target_properties(hybridse_jsdk_core PROPERTIES
      SUFFIX ".dylib")
  endif()

  swig_add_library(hybridse_jsdk_core_static
    TYPE STATIC
    LANGUAGE java
    OUTPUT_DIR ${CMAKE_SOURCE_DIR}/java/hybridse-native/src/main/java/com/_4paradigm/
    SOURCES hybridse_interface_core.i)
  target_include_directories(hybridse_jsdk_core_static PRIVATE ${JNI_INCLUDE_DIRS})
  target_compile_options(hybridse_jsdk_core_static PRIVATE -w)
  target_link_libraries(hybridse_jsdk_core_static PRIVATE glog ${sdk_core_deps_libs})

  add_custom_target(
    cp_hybridse_native_so ALL
    COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_SOURCE_DIR}/java/hybridse-native/src/main/resources/
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:hybridse_jsdk_core> ${CMAKE_SOURCE_DIR}/java/hybridse-native/src/main/resources/
    COMMENT "copy hybridse native so")
  add_dependencies(cp_hybridse_native_so hybridse_proto_java)
endif()
