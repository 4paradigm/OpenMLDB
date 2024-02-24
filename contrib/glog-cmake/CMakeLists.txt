set(WITH_GTEST OFF)

if (BUILD_SHARED_LIBS)
    # visibility issue building shared glog against static gflags
    # so explicitly skip the gflags dependency
    set(WITH_GFLAGS OFF)
    set(WITH_UNWIND OFF)
endif()

add_subdirectory(${CMAKE_SOURCE_DIR}/contrib/glog ${CMAKE_BINARY_DIR}/contrib/glog)
