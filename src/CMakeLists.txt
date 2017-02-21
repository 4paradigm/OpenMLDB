INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src)
SET(util_src util/status.cc util/filter_policy.cc util/coding.cc util/comparator.cc util/arena.cc)

SET(storage_src storage/iterator.cc storage/memtable.cc storage/dbformat.cc)

SET(rtidb_src rtidb_main.cc)

ADD_EXECUTABLE(rtidb ${rtidb_src} ${storage_src} ${util_src})
TARGET_LINK_LIBRARIES(rtidb pthread)
