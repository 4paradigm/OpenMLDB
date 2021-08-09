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

if(PYSDK_ENABLE)
set(UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
swig_add_library(hybridse_pysdk
        TYPE SHARED
        LANGUAGE python
        OUTPUT_DIR ${CMAKE_BINARY_DIR}/python/hybridse/
        SOURCES hybridse_interface_core.i)
target_include_directories(hybridse_pysdk PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(hybridse_pysdk ${SDK_DEPS_LIBS})
set_property(SOURCE hybridse_interface_core.i PROPERTY SWIG_FLAGS ${CUR_SWIG_FLAGS})
endif()

if(JAVASDK_ENABLE)
set_property(SOURCE hybridse_interface_core.i PROPERTY COMPILE_OPTIONS -package com._4paradigm)
swig_add_library(hybridse_jsdk_core
        TYPE SHARED
        LANGUAGE java
        OUTPUT_DIR ${PROJECT_SOURCE_DIR}/java/hybridse-native/src/main/java/com/_4paradigm/
        SOURCES hybridse_interface_core.i)
target_include_directories(hybridse_jsdk_core PRIVATE ${JNI_INCLUDE_DIRS})
target_compile_options(hybridse_jsdk_core PRIVATE -w)
set(sdk_core_deps_libs
	hybridse_core hybridse_flags ${yaml_libs} boost_filesystem ${VM_LIBS} ${LLVM_LIBS} ${ZETASQL_LIBS} ${COMMON_LIBS} )
target_link_libraries(hybridse_jsdk_core PRIVATE glog ${sdk_core_deps_libs})

if(APPLE)
	set_target_properties(hybridse_jsdk_core PROPERTIES
           SUFFIX ".dylib")
endif()
endif()

if(PYSDK_ENABLE)
if(APPLE)
  	set_target_properties(hybridse_pysdk PROPERTIES
           SUFFIX ".so")
  	set_property(TARGET hybridse_pysdk APPEND PROPERTY
    			 LINK_FLAGS "-flat_namespace -undefined suppress")
endif()

#######################
## Python Packaging  ##
#######################
# To use a cmake generator expression (aka $<>), it must be processed at build time
# i.e. inside a add_custom_command()
# This command will depend on TARGET(s) in cmake generator expression
add_custom_command(OUTPUT
	python/setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "from setuptools import find_packages, setup" > setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "from setuptools.dist import Distribution" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "class BinaryDistribution(Distribution):" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  def is_pure(self):" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "    return False" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  def has_ext_modules(self):" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "    return True" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "from setuptools.command.install import install" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "class InstallPlatlib(install):" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "    def finalize_options(self):" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "        install.finalize_options(self)" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "        self.install_lib=self.install_platlib" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "setup(" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  name='${PROJECT_NAME}'," >> setup.py
    COMMAND ${CMAKE_COMMAND} -E echo "  version='${HYBRIDSE_VERSION_MAJOR}.${HYBRIDSE_VERSION_MINOR}.${HYBRIDSE_VERSION_BUG}'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  author='4pd'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  url='4paradigm.com'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  distclass=BinaryDistribution," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  cmdclass={'install': InstallPlatlib}," >> setup.py
    COMMAND ${CMAKE_COMMAND} -E echo "  packages=find_packages()," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  package_data={" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'hybridse':['$<TARGET_FILE_NAME:hybridse_pysdk>']," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  }," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  include_package_data=True," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  classifiers=[" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: POSIX :: Linux'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: MacOS :: MacOS X'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Programming Language :: Python'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Programming Language :: C++'" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  ]," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo ")" >> setup.py
	COMMENT "Generate setup.py at build time (to use generator expression)"
    WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/python
	VERBATIM)

# Find if python module MODULE_NAME is available,
# if not install it to the Python user install directory.
function(search_python_module MODULE_NAME)
	execute_process(
		COMMAND ${PYTHON_EXECUTABLE} -c "import ${MODULE_NAME}; print(${MODULE_NAME}.__version__)"
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
			COMMAND ${PYTHON_EXECUTABLE} -m pip install --upgrade --user ${MODULE_NAME}
			OUTPUT_STRIP_TRAILING_WHITESPACE
			)
	endif()
endfunction()
# Look for required python modules
search_python_module(setuptools)
search_python_module(wheel)

add_custom_target(python_package ALL
	DEPENDS python/setup.py
	COMMAND ${CMAKE_COMMAND} -E make_directory hybridse
	COMMAND ${CMAKE_COMMAND} -E remove_directory dist
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/__init__.py.in hybridse/__init__.py
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:hybridse_pysdk> hybridse/
	COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel
	BYPRODUCTS
	  python/${PROJECT_NAME}
	  python/build
	  python/dist
	  python/${PROJECT_NAME}.egg-info
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/python
	)

endif()
if(JAVASDK_ENABLE)
add_custom_command(OUTPUT
    cp_java_native_so
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:hybridse_jsdk_core>
    	${PROJECT_SOURCE_DIR}/java/hybridse-native/src/main/resources/
    )

add_custom_target(java_package ALL
  DEPENDS
    cp_java_native_so
    COMMAND ${MAVEN_EXECUTABLE} package -DskipTests=true -Dscalatest.skip=true
    WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/java
)
add_dependencies(java_package hybridse_proto_java)
endif()
