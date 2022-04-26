//
// Created by hezhaozhao on 2022/4/25.
//
#include "base/string_ref.h"


#ifndef OPENMLDB_UDF_JSON_H
#define OPENMLDB_UDF_JSON_H
namespace hybridse {
namespace udf {
using openmldb::base::StringRef;
namespace v1 {
void get_json_object(StringRef *json_string, StringRef *path_string, StringRef *output, bool *is_null_ptr);

}  // namespace v1
}  // namespace udf
}  // namespace hybridse

#endif  // OPENMLDB_UDF_JSON_H
