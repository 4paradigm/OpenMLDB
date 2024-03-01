# parent project use gflags::gflags alias, so build either shared or static
if (BUILD_SHARED_LIBS)
    set(GFLAGS_BUILD_SHARED_LIBS ON)
    set(GFLAGS_BUILD_STATIC_LIBS OFF)
else()
    set(GFLAGS_BUILD_SHARED_LIBS OFF)
    set(GFLAGS_BUILD_STATIC_LIBS ON)
endif()

set(GFLAGS_NAMESPACE google)

add_subdirectory(${CMAKE_SOURCE_DIR}/contrib/gflags ${CMAKE_BINARY_DIR}/contrib/gflags)
