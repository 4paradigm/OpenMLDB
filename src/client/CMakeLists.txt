find_package(Python3 COMPONENTS Interpreter Development)
message(STATUS "Found Python: ${Python3_EXECUTABLE} (found version \"${PYTHON3_VERSION}\") Found include ${Python3_INCLUDE_DIRS}")
include_directories(${INCLUDE_DIRECTORIES} ${Python3_INCLUDE_DIRS})
set (UseSWIG_TARGET_NAME_PREFERENCE STANDARD)
cmake_policy(SET CMP0078 NEW)
find_package(SWIG REQUIRED)
include(UseSWIG)
set_property(SOURCE interclient.i PROPERTY CPLUSPLUS ON)
set_property(SOURCE interclient.i PROPERTY SWIG_MODULE_NAME interclient)
set_property(SOURCE interclient.i PROPERTY COMPILE_OPTIONS -py3)
swig_add_library(interclient TYPE SHARED LANGUAGE python OUTPUT_DIR ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME} SOURCES interclient.i client.cc ../flags.cc ../proto/tablet.pb.cc ../proto/common.pb.cc ../proto/type.pb.cc ../proto/name_server.pb.cc ../zk/zk_client.cc ../zk/dist_lock.cc ../client/tablet_client.cc ../codec/codec.cc ../base/status.cc ../client/bs_client.cc ../client/ns_client.cc ../proto/blob_server.pb.cc ../proto/client.pb.cc ../proto/sql_procedure.pb.cc ../codec/sql_rpc_row_codec.cc)
set_property(TARGET interclient PROPERTY SWIG_USE_TARGET_INCLUDE_DIRECTORIES ON)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
	target_link_libraries(interclient snappy brpc glog protobuf rt ssl crypto dl leveldb z common zookeeper_mt unwind gflags ${VM_LIBS} ${LLVM_LIBS} lzma)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
	target_link_libraries(interclient snappy brpc glog protobuf ssl crypto dl leveldb z common zookeeper_mt gflags ${VM_LIBS} ${LLVM_LIBS})
endif ()
set_property(TARGET interclient PROPERTY POSITION_INDEPENDENT_CODE ON)

set_property(SOURCE interclient_tools.i PROPERTY CPLUSPLUS ON)
set_property(SOURCE interclient_tools.i PROPERTY SWIG_MODULE_NAME interclient_tools)
set_property(SOURCE interclient_tools.i PROPERTY COMPILE_OPTIONS -py3)
swig_add_library(interclient_tools TYPE SHARED LANGUAGE python OUTPUT_DIR ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME} SOURCES interclient_tools.i interclient_tools.cc ../flags.cc ../client/bs_client.cc ../proto/blob_server.pb.cc ../proto/type.pb.cc ../proto/tablet.pb.cc ../proto/common.pb.cc ../proto/sql_procedure.pb.cc)
set_property(TARGET interclient_tools PROPERTY SWIG_USE_TARGET_INCLUDE_DIRECTORIES ON)
if (CMAKE_SYSTEM_NAME STREQUAL "Linux")
	target_link_libraries(interclient_tools brpc protobuf glog gflags ssl crypto common z leveldb unwind lzma)
elseif (CMAKE_SYSTEM_NAME STREQUAL "Darwin")
	target_link_libraries(interclient_tools brpc protobuf glog gflags ssl crypto common z leveldb)
endif ()
set_property(TARGET interclient_tools PROPERTY POSITION_INDEPENDENT_CODE ON)

add_custom_target(python_package ALL DEPENDS interclient interclient_tools
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/__init__.py ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/rtidb.py ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:interclient> ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy $<TARGET_FILE:interclient_tools> ${CMAKE_BINARY_DIR}/python/${PROJECT_NAME}
    COMMAND ${CMAKE_COMMAND} -E copy ${PROJECT_SOURCE_DIR}/python/setup.py ${CMAKE_BINARY_DIR}/python
	COMMAND ${CMAKE_COMMAND} -E remove_directory dist
	COMMAND ${Python3_EXECUTABLE} setup.py bdist_wheel
	BYPRODUCTS
	  python/${PROJECT_NAME}
	  python/build
	  python/dist
	  python/${PROJECT_NAME}.egg-info
	WORKING_DIRECTORY ${CMAKE_BINARY_DIR}/python
)
