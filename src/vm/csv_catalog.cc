/*
 * csv_catalog.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/csv_catalog.h"

#include <fstream>
#include "glog/logging.h"
#include "boost/lexical_cast.hpp"
#include "boost/algorithm/string.hpp"
#include "boost/algorithm/predicate.hpp"

namespace fesql {
namespace vm {

bool SchemaParser::Convert(const std::string& type, 
                          type::Type* db_type) {
    if (db_type == nullptr) return false;
    if (type == "bool") {
        *db_type = type::kBool;
    }else if (type == "int16") {
        *db_type = type::kInt16;
    }else if (type == "int32") {
        *db_type = type::kInt32;
    }else if (type == "int64") {
        *db_type = type::kInt64;
    }else if (type == "float") {
        *db_type = type::kFloat;
    }else if (type == "double") {
        *db_type = type::kDouble;
    }else if (type == "varchar") {
        *db_type = type::kVarchar;
    }else if (type == "timestamp") {
        *db_type = type::kTimestamp;
    }else if (type == "date") {
        *db_type = type::kDate;
    }else {
        LOG(WARNING) << type << " is not supported";
        return false;
    }
    return true;
}

bool SchemaParser::Parse(const std::string& path, Schema* schema) {

    if (schema == nullptr) {
        LOG(WARNING) << "schema is nullptr";
        return false;
    }

}

}  // namespace vm
}  // namespace fesql


