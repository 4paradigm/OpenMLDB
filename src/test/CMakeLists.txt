# Base codes for OpenMLDB tests
# The module depends on library from tablet and nameserver, so
# WARN: do not link in production

add_library(openmldb_test_base base_test.cc util.cc)
add_dependencies(openmldb_test_base openmldb_sdk tablet nameserver)
