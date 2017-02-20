INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src)
SET(util_src util/coding.cc util/comparator.cc util/arena.cc)

SET(db_src  db/filter_policy.cc db/memtable.cc db/dbformat.cc)

SET(rtidb_src rtidb.cc)

ADD_EXECUTABLE(rtidb ${rtidb_src} ${db_src} ${util_src})
TARGET_LINK_LIBRARIES(rtidb pthread)
