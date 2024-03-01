# library
lib_LTLIBRARIES = libfarmhash.la
libfarmhash_la_SOURCES = farmhash.cc
include_HEADERS = farmhash.h

# test
farmhash_unittest_SOURCES = farm-test.cc
farmhash_unittest_LDADD = libfarmhash.la
TESTS = farmhash_unittest
noinst_PROGRAMS = $(TESTS)
