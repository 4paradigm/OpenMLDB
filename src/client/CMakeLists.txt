set(Python_ADDITIONAL_VERSIONS "3.6" CACHE STRING "Python to use for binding")
set(PYTHON_VERSION_MAJOR 3)
set(PYTHON_VERSION_MINOR 6)
find_package(PythonInterp REQUIRED)
message(STATUS "Found Python: ${PYTHON_EXECUTABLE} (found version \"${PYTHON_VERSION_STRING}\")")

# Find Python Library
# Force PythonLibs to find the same version than the python interpreter (or nothing).
set(Python_ADDITIONAL_VERSIONS "${PYTHON_VERSION_STRING}")
find_package(PythonLibs REQUIRED)
message(STATUS "Found Python Include: ${PYTHON_INCLUDE_DIRS} (found version \"${PYTHONLIBS_VERSION_STRING}\")")
include_directories(${INCLUDE_DIRECTORIES} ${PYTHON_INCLUDE_DIRS} ${PROJECT_SOURCE_DIR/src})
message(STATUS "current include directory is ${INCLUDE_DIRECTORIES}")

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


set (UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
cmake_policy(SET CMP0078 NEW)
find_package(SWIG REQUIRED)
include(UseSWIG)
set_property(SOURCE interclient.i PROPERTY CPLUSPLUS ON)
set_property(SOURCE interclient.i PROPERTY SWIG_MODULE_NAME interclient)
set_property(SOURCE interclient.i PROPERTY COMPILE_OPTIONS -py3)
swig_add_library(interclient TYPE SHARED LANGUAGE python OUTPUT_DIR ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME} SOURCES interclient.i client.cc ../flags.cc ../proto/tablet.pb.cc ../proto/common.pb.cc ../proto/type.pb.cc ../proto/name_server.pb.cc ../zk/zk_client.cc ../zk/dist_lock.cc ../client/tablet_client.cc ../client/ns_client.cc ../base/codec.cc)
set_property(TARGET interclient PROPERTY SWIG_USE_TARGET_INCLUDE_DIRECTORIES ON)
target_link_libraries(interclient snappy brpc protobuf gflags rt ssl crypto dl leveldb z common zookeeper_mt)
set_property(TARGET interclient PROPERTY POSITION_INDEPENDENT_CODE ON)

add_custom_target(python_package ALL DEPENDS interclient
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/__init__.py ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/rtidb.py ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy ${CMAKE_BINARY_DIR}/lib/_interclient.so ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/setup.py ${CMAKE_BINARY_DIR}/python
	COMMAND ${CMAKE_COMMAND} -E remove_directory dist
	COMMAND ${PYTHON_EXECUTABLE} setup.py bdist_wheel
	BYPRODUCTS
	  python/${PROJECT_NAME}
	  python/build
	  python/dist
	  python/${PROJECT_NAME}.egg-info
	WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/python
)
