
add_library(rtidb_sdk STATIC cluster_sdk.cc sql_cluster_router.cc sql_router.cc result_set_sql.cc)
add_executable(cluster_sdk_test cluster_sdk_test.cc)
target_link_libraries(cluster_sdk_test gtest ${BIN_LIBS})
add_executable(sql_router_test sql_router_test.cc)
target_link_libraries(sql_router_test gtest ${BIN_LIBS})

set(SDK_LIBS rtidb_sdk rtidb_catalog client zk_client rtidb_flags rtidb_proto base fesql_sdk zookeeper_mt ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS})
add_executable(mini_cluster_bm mini_cluster_microbenchmark.cc)
target_link_libraries(mini_cluster_bm benchmark_main benchmark ${BIN_LIBS})

if(SQL_PYSDK_ENABLE)
	find_package(Python3 COMPONENTS Interpreter Development)
	set_property(SOURCE sql_router_sdk.i PROPERTY CPLUSPLUS ON)
	if (CMAKE_SYSTEM_NAME MATCHES "Darwin")
		set_property(SOURCE sql_router_sdk.i PROPERTY COMPILE_OPTIONS -python)
	else ()
		set_property(SOURCE sql_router_sdk.i PROPERTY COMPILE_OPTIONS -py3)
	endif ()
	set(UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
	swig_add_library(sql_pysdk
			TYPE SHARED
			LANGUAGE python
			OUTPUT_DIR ${CMAKE_BINARY_DIR}/sql_pysdk/rtidb_sql
			SOURCES sql_router_sdk.i)
	target_include_directories(sql_pysdk PRIVATE ${Python3_INCLUDE_DIRS})
	target_link_libraries(sql_pysdk ${SDK_LIBS})

	add_custom_command(OUTPUT
		setup.py
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
        COMMAND ${CMAKE_COMMAND} -E echo "  name='${PROJECT_NAME}_sql'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  version='${RTIDB_VERSION_MAJOR}.${RTIDB_VERSION_MEDIUM}.${RTIDB_VERSION_MINOR}.${RTIDB_VERSION_BUG}'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  author='4pd'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  url='4paradigm.com'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  distclass=BinaryDistribution," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  cmdclass={'install': InstallPlatlib}," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  packages=find_packages()," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  package_data={" >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  'rtidb_sql':['$<TARGET_FILE_NAME:sql_pysdk>']," >> setup.py
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
		WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/sql_pysdk
		VERBATIM)

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

	add_custom_target(sql_pysdk_package ALL
		DEPENDS setup.py
		COMMAND ${CMAKE_COMMAND} -E remove_directory dist
		COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/rtidb_sql/__init__.py rtidb_sql/__init__.py
		COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_pysdk> ${CMAKE_BINARY_DIR}/sql_pysdk/rtidb_sql
		COMMAND ${Python3_EXECUTABLE} setup.py bdist_wheel
		BYPRODUCTS
		  python/${PROJECT_NAME}
		  python/build
		  python/dist
		  python/${PROJECT_NAME}_sql.egg-info
		WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/sql_pysdk
		)

endif()

if(SQL_JAVASDK_ENABLE)
	set(JAVA_HOME ${PROJECT_SOURCE_DIR}/thirdparty/jdk1.8.0_141)
	find_package(Java COMPONENTS Development REQUIRED)
	message(STATUS "Found Java: ${Java_JAVA_EXECUTABLE} (found version \"${Java_VERSION_STRING}\")")
	find_package(JNI REQUIRED)
	message(STATUS "Found JNI: ${JNI_FOUND}")
	# Find maven
	find_program(MAVEN_EXECUTABLE mvn)
	set_property(SOURCE sql_router_sdk.i PROPERTY COMPILE_OPTIONS -package com._4paradigm.sql)
	swig_add_library(sql_jsdk
			TYPE SHARED
			LANGUAGE java
            OUTPUT_DIR ${CMAKE_BINARY_DIR}/src/sdk/java/sql-native/src/main/java/com/_4paradigm/sql/
			SOURCES sql_router_sdk.i)
	target_include_directories(sql_jsdk PRIVATE ${JNI_INCLUDE_DIRS})
	target_compile_options(sql_jsdk PRIVATE -w)
    target_link_libraries(sql_jsdk PRIVATE ${SDK_LIBS})
	if(APPLE)
	  set_target_properties(fesql_jsdk PROPERTIES
			   SUFFIX ".dylib")
	  set_target_properties(fesql_pysdk PROPERTIES
			   SUFFIX ".so")
	  set_property(TARGET fesql_pysdk APPEND PROPERTY
		LINK_FLAGS "-flat_namespace -undefined suppress")
	endif()

    add_custom_command(OUTPUT
          cp_sql_resurce
          COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/src/sdk/java/sql-native/src/main/resources
          COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_BINARY_DIR}/src/sdk/java/sql-jdbc
          COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/java/pom.xml  ${CMAKE_BINARY_DIR}/src/sdk/java/
          COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/java/sql-jdbc/pom.xml ${CMAKE_BINARY_DIR}/src/sdk/java/sql-jdbc/
          COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/java/sql-native/pom.xml ${CMAKE_BINARY_DIR}/src/sdk/java/sql-native/
          COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_jsdk> ${CMAKE_BINARY_DIR}/src/sdk/java/sql-native/src/main/resources/)
	add_custom_target(sql_javasdk_package ALL
	  DEPENDS
        sql_jsdk
        cp_sql_resurce
		COMMAND ${MAVEN_EXECUTABLE} package -DskipTests=true -Dscalatest.skip=true
		WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/src/sdk/java
	)
endif()

