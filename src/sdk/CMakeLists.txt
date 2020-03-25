include_directories(${INCLUDE_DIRECTORIES}
        ${PROJECT_SOURCE_DIR}/src
        ${PROJECT_BINARY_DIR}/src)

add_library(fesql_sdk STATIC base_impl.cc dbms_sdk.cc tablet_sdk.cc result_set_impl.cc)
target_link_libraries(fesql_sdk fesql_analyser fesql_parser fesql_planner fesql_node fesql_codec fesql_proto brpc ${g_libs} protobuf leveldb z snappy ssl crypto dl pthread)

add_executable(dbms_sdk_test dbms_sdk_test.cc)
target_link_libraries(dbms_sdk_test  
    fesql_dbms
    fesql_tablet 
    fesql_storage fesql_vm fesql_codegen fesql_udf
    fesql_flags
    gtest fesql_sdk  ${COMMON_LIBS} ${llvm_libs})
add_test(dbms_sdk_test dbms_sdk_test --gtest_output=xml:${CMAKE_BINARY_DIR}/dbms_sdk_test.xml)
list(APPEND test_list dbms_sdk_test)

add_executable(tablet_sdk_test tablet_sdk_test.cc)
target_link_libraries(tablet_sdk_test
    fesql_dbms
    fesql_tablet 
    fesql_storage fesql_vm fesql_codegen fesql_udf
    fesql_flags
    gtest fesql_sdk ${COMMON_LIBS} ${llvm_libs})
add_test(tablet_sdk_test tablet_sdk_test
    --gtest_output=xml:${CMAKE_BINARY_DIR}/tablet_sdk_test.xml)
list(APPEND test_list tablet_sdk_test)

add_executable(result_set_impl_test result_set_impl_test.cc)
target_link_libraries(result_set_impl_test
    fesql_dbms
    fesql_tablet 
    fesql_storage fesql_vm fesql_codegen fesql_udf
    fesql_flags 
    gtest fesql_sdk ${COMMON_LIBS} ${llvm_libs})
add_test(result_set_impl_test result_set_impl_test --gtest_output=xml:${CMAKE_BINARY_DIR}/result_set_impl_test.xml)
list(APPEND test_list result_set_impl_test)

set_property(SOURCE fesql.i PROPERTY CPLUSPLUS ON)
set_property(SOURCE fesql.i PROPERTY SWIG_MODULE_NAME fesql)
set_property(SOURCE fesql.i PROPERTY COMPILE_OPTIONS
    -py3
)
swig_add_library(fesql_pysdk
  TYPE SHARED
  LANGUAGE python
  OUTPUT_DIR ${CMAKE_BINARY_DIR}/python_output/
  SOURCES fesql.i)

target_include_directories(fesql_pysdk PRIVATE ${PYTHON_INCLUDE_DIRS})
target_link_libraries(fesql_pysdk fesql_sdk)


#add_library(test_lib test.cc)
#set_property(SOURCE test.i PROPERTY CPLUSPLUS ON)
#set_property(SOURCE test.i PROPERTY SWIG_MODULE_NAME test)
#set_property(SOURCE test.i PROPERTY COMPILE_OPTIONS
#    -py3
#)
#swig_add_library(py_test
#  TYPE SHARED
#  LANGUAGE python
#  OUTPUT_DIR ${CMAKE_BINARY_DIR}/python/
#  SOURCES test.i)
#target_include_directories(py_test PRIVATE ${PYTHON_INCLUDE_DIRS})
#target_link_libraries(py_test test_lib)

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
	COMMAND ${CMAKE_COMMAND} -E echo "  version='0.0.1'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  author='4pd'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  url='4paradigm.com'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  distclass=BinaryDistribution," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  cmdclass={'install': InstallPlatlib}," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  packages=['fesql']," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  package_data={" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'fesql':['$<TARGET_FILE_NAME:fesql_pysdk>']," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  }," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  include_package_data=True," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  classifiers=[" >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Development Status :: 5 - Production/Stable'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Intended Audience :: Developers'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'License :: OSI Approved :: Apache Software License'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: POSIX :: Linux'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: MacOS :: MacOS X'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: Microsoft :: Windows'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Programming Language :: Python'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Programming Language :: C++'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Topic :: Scientific/Engineering'," >> setup.py
	COMMAND ${CMAKE_COMMAND} -E echo "  'Topic :: Software Development :: Libraries :: Python Modules'" >> setup.py
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
	COMMAND ${CMAKE_COMMAND} -E make_directory fesql
	COMMAND ${CMAKE_COMMAND} -E remove_directory dist
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/python_output/fesql.py fesql/fesql.py
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/__init__.py fesql/__init__.py
	COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:fesql_pysdk> fesql/
	COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel
	BYPRODUCTS
	  python/${PROJECT_NAME}
	  python/build
	  python/dist
	  python/${PROJECT_NAME}.egg-info
      WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/python
	)
