INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src)
SET(util_src util/coding.cc util/comparator.cc util/arena.cc)
ADD_LIBRARY(util STATIC ${util_src})

SET(db_src db/filter_policy.cc db/memtable.cc db/dbformat.cc)
ADD_LIBRARY(db STATIC ${db_src})

SET(rtidb_src rtidb.cc)

ADD_EXECUTABLE(rtidb ${rtidb_src})
TARGET_LINK_LIBRARIES(rtidb util db pthread)
