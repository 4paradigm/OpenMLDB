set(WITH_GTEST OFF)
set(WITH_GFLAGS ON)
set(WITH_UNWIND ON)
execute_process(
  COMMAND patch -p 1 -N -i ${CMAKE_SOURCE_DIR}/contrib/glog-cmake/find-gflags.patch
  WORKING_DIRECTORY ${CMAKE_SOURCE_DIR}/contrib/glog
  RESULT_VARIABLE ret
  ECHO_OUTPUT_VARIABLE
  ECHO_ERROR_VARIABLE
  COMMAND_ECHO STDOUT
)
if(NOT ret EQUAL "0")
    message(STATUS  "Bad exit status '${ret}', be careful")
endif()
add_subdirectory(${CMAKE_SOURCE_DIR}/contrib/glog ${CMAKE_BINARY_DIR}/contrib/glog)
