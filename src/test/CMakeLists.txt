# Base codes for OpenMLDB tests
# The module depends on library from tablet and nameserver, so
# WARN: do not link in production

add_library(openmldb_test_base base_test.cc util.cc)

set(DEPS openmldb_sdk tablet nameserver)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.1")
    # GNU implementation prior to 9.1 requires linking with -lstdc++fs
    list(APPEND DEPS stdc++fs)
endif()
add_dependencies(openmldb_test_base ${DEPS})
