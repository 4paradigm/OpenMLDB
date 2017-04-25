include_directories(${INCLUDE_DIRECTORIES} ${PROJECT_SOURCE_DIR}/src)

add_library(rtidb_py SHARED rtidb_wrapper.cc)
