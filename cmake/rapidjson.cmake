FetchContent_Declare(
  rapidjson
  URL      https://github.com/Tencent/rapidjson/archive/refs/tags/v1.1.0.zip
  URL_HASH MD5=ceb1cf16e693a3170c173dc040a9d2bd
  EXCLUDE_FROM_ALL # don't build this project as part of the overall build
)
# don't build this project, just populate
FetchContent_Populate(rapidjson)
include_directories(${rapidjson_SOURCE_DIR}/include)
