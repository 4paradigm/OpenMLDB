add_library(rtidb_sdk STATIC
	cluster_sdk.cc sql_cluster_router.cc sql_router.cc
    result_set_sql.cc batch_request_result_set_sql.cc
    sql_request_row.cc sql_insert_row.cc)
set(FESQL_CASE_LIBS base_test fesql_core yaml-cpp boost_filesystem)

add_executable(cluster_sdk_test cluster_sdk_test.cc)
target_link_libraries(cluster_sdk_test gtest ${BIN_LIBS})

add_executable(sql_router_test sql_router_test.cc)
target_link_libraries(sql_router_test gtest ${BIN_LIBS})

add_executable(sql_sdk_test sql_sdk_test.cc)
target_link_libraries(sql_sdk_test gtest ${BIN_LIBS} ${FESQL_CASE_LIBS})

add_executable(sql_cluster_test sql_cluster_test.cc)
target_link_libraries(sql_cluster_test gtest ${BIN_LIBS} ${FESQL_CASE_LIBS})

add_executable(sql_request_row_test sql_request_row_test.cc)
target_link_libraries(sql_request_row_test gtest ${BIN_LIBS})

set(SDK_LIBS rtidb_sdk rtidb_catalog client zk_client rtidb_flags rtidb_codec rtidb_proto base fesql_sdk zookeeper_mt ${VM_LIBS} ${LLVM_LIBS} ${BRPC_LIBS})
add_executable(mini_cluster_bm mini_cluster_bm.cc mini_cluster_microbenchmark.cc)
target_link_libraries(mini_cluster_bm benchmark_main benchmark gtest ${BIN_LIBS} ${FESQL_CASE_LIBS})

add_executable(mini_cluster_bm_request_batch mini_cluster_bm_request_batch.cc mini_cluster_bm.cc)
target_link_libraries(mini_cluster_bm_request_batch benchmark_main benchmark gtest ${BIN_LIBS} ${FESQL_CASE_LIBS})

add_executable(mini_cluster_bm_request mini_cluster_bm_request.cc mini_cluster_bm.cc)
target_link_libraries(mini_cluster_bm_request benchmark_main benchmark gtest ${BIN_LIBS} ${FESQL_CASE_LIBS})

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
			OUTPUT_DIR ${CMAKE_BINARY_DIR}/sql_pysdk/fedb
			SOURCES sql_router_sdk.i)
	target_include_directories(sql_router_sdk PRIVATE ${Python3_INCLUDE_DIRS})
	target_link_libraries(sql_router_sdk ${SDK_LIBS})
    if(APPLE)
      set_target_properties(sql_router_sdk PROPERTIES
               SUFFIX ".so")
      set_property(TARGET sql_router_sdk APPEND PROPERTY
        LINK_FLAGS "-flat_namespace -undefined suppress")
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
        COMMAND ${CMAKE_COMMAND} -E echo "  name='fedb'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  version='${RTIDB_VERSION_MAJOR}.${RTIDB_VERSION_MEDIUM}.${RTIDB_VERSION_MINOR}.${RTIDB_VERSION_BUG}'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  author='4pd'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  url='4paradigm.com'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  distclass=BinaryDistribution," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  cmdclass={'install': InstallPlatlib}," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  packages=find_packages()," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  package_data={" >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  'fedb':['$<TARGET_FILE_NAME:sql_router_sdk>']," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  }," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  include_package_data=True," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  classifiers=[" >> setup.py
        COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: MacOS'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  'Programming Language :: Python'" >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  ]," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo ")" >> setup.py
		COMMENT "Generate setup.py at build time (to use generator expression)"
		WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/sql_pysdk
		VERBATIM)
    else()
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
        COMMAND ${CMAKE_COMMAND} -E echo "  name='fedb'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  version='${RTIDB_VERSION_MAJOR}.${RTIDB_VERSION_MEDIUM}.${RTIDB_VERSION_MINOR}.${RTIDB_VERSION_BUG}'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  author='4pd'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  url='4paradigm.com'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  distclass=BinaryDistribution," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  cmdclass={'install': InstallPlatlib}," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  packages=find_packages()," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  package_data={" >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  'fedb':['$<TARGET_FILE_NAME:sql_router_sdk>']," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  }," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  include_package_data=True," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  classifiers=[" >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  'Operating System :: POSIX :: Linux'," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  'Programming Language :: Python'" >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo "  ]," >> setup.py
		COMMAND ${CMAKE_COMMAND} -E echo ")" >> setup.py
		COMMENT "Generate setup.py at build time (to use generator expression)"
		WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/sql_pysdk
		VERBATIM)
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

	add_custom_target(sql_pysdk_package ALL
		DEPENDS setup.py
        COMMAND ${CMAKE_COMMAND} -E remove_directory dist
		COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/fedb/__init__.py fedb/__init__.py
		COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/fedb/driver.py fedb/driver.py
		COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_router_sdk> ${CMAKE_BINARY_DIR}/sql_pysdk/fedb
		COMMAND ${Python3_EXECUTABLE} setup.py bdist_wheel
		BYPRODUCTS
		  python/fedb
          python/build
          python/dist
		  python/fedb.egg-info
		WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/sql_pysdk
    )

    add_custom_target(sqlalchemy_fedb DEPENDS sql_router_sdk
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/sqlalchemy-fedb/sqlalchemy_fedb/__init__.py ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/__init__.py
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/sqlalchemy-fedb/sqlalchemy_fedb/safedb.py ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/safedb.py
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/sqlalchemy-fedb/sqlalchemy_fedb/fedbapi/_fedbapi.py ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/fedbapi/_fedbapi.py
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/sqlalchemy-fedb/sqlalchemy_fedb/fedbapi/__init__.py ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/fedbapi/__init__.py
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/fedb/driver.py ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/fedbapi/driver.py
        COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_router_sdk> ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/fedbapi
        COMMAND ${CMAKE_COMMAND} -E copy  ${CMAKE_BINARY_DIR}/sql_pysdk/fedb/sql_router_sdk.py ${CMAKE_BINARY_DIR}/python/sqlalchemy_fedb/fedbapi
        COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/src/sdk/python/sqlalchemy-fedb/setup.py ${CMAKE_BINARY_DIR}/python/
        COMMAND ${CMAKE_COMMAND} -E remove_directory dist
        COMMAND cd ${CMAKE_BINARY_DIR}/python/ && ${Python3_EXECUTABLE} setup.py bdist_wheel
        BYPRODUCTS
          python/sqlalchemy-fedb
          python/build
          python/dist
          python/sqlalchemy-fedb.egg-info
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
            OUTPUT_DIR ${PROJECT_SOURCE_DIR}/src/sdk/java/sql-native/src/main/java/com/_4paradigm/sql/
			SOURCES sql_router_sdk.i)
	target_include_directories(sql_jsdk PRIVATE ${JNI_INCLUDE_DIRS})
	target_compile_options(sql_jsdk PRIVATE -w)
    target_link_libraries(sql_jsdk PRIVATE ${SDK_LIBS} boost_filesystem)

	if(APPLE)
	  set_target_properties(sql_jsdk PROPERTIES
			   SUFFIX ".dylib")
	  set_property(TARGET sql_jsdk APPEND PROPERTY
		LINK_FLAGS "-flat_namespace -undefined suppress")
	endif()

    add_custom_command(OUTPUT
          cp_sql_resource
          COMMAND ${CMAKE_COMMAND} -E make_directory ${PROJECT_SOURCE_DIR}/src/sdk/java/sql-native/src/main/resources
          COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:sql_jsdk> ${PROJECT_SOURCE_DIR}/src/sdk/java/sql-native/src/main/resources/)
	add_custom_target(sql_javasdk_package ALL
	  DEPENDS
        sql_jsdk
        cp_sql_resource
		COMMAND ${MAVEN_EXECUTABLE} package -DskipTests=true -Dscalatest.skip=true
        WORKING_DIRECTORY ${PROJECT_SOURCE_DIR}/src/sdk/java
	)
endif()
