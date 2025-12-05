# Base codes for OpenMLDB tests
# The module depends on library from tablet and nameserver, so
# WARN: do not link in production

add_library(openmldb_test_base base_test.cc util.cc)

# TODO: Fix me after upgrade Boost or fix nonnull errors in Boost
target_compile_options(openmldb_test_base PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wno-error=nonnull>
    $<$<COMPILE_LANGUAGE:C>:-Wno-error=nonnull>
)
target_compile_options(openmldb_test_base PRIVATE
    $<$<COMPILE_LANGUAGE:CXX>:-Wno-nonnull>
    $<$<COMPILE_LANGUAGE:C>:-Wno-nonnull>
)

add_dependencies(openmldb_test_base openmldb_sdk tablet nameserver)
if(CMAKE_CXX_COMPILER_ID MATCHES "GNU" AND CMAKE_CXX_COMPILER_VERSION VERSION_LESS "9.1")
    # GNU implementation prior to 9.1 requires linking with -lstdc++fs
    target_link_libraries(openmldb_test_base stdc++fs)
endif()
