INCLUDE_DIRECTORIES(${INCLUDE_DIRECTORIES} 
  ${PROJECT_SOURCE_DIR}/src)
SET(util_src util/coding.cc util/arena.cc)
ADD_LIBRARY(util STATIC ${util_src})

SET(db_src db/memtable.cc)
ADD_LIBRARY(db STATIC ${db_src})
