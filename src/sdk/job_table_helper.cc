/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "sdk/job_table_helper.h"

#include <unordered_map>
#include <utility>
#include "codec/schema_codec.h"
#include "sdk/result_set_sql.h"
#include "udf/udf.h"

namespace openmldb {
namespace sdk {

static const std::unordered_map<std::string, std::string> STATE_MAP = {
    {"kInited", "Submitted"},
    {"kDoing", "RUNNING"},
    {"kDone", "FINISHED"},
    {"kFailed", "FAILED"},
    {"kCanceled", "STOPPED"},
};

schema::PBSchema JobTableHelper::GetSchema() {
    schema::PBSchema schema;
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "job_id", type::DataType::kInt);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "job_type", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "state", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "start_time", type::DataType::kTimestamp);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "end_time", type::DataType::kTimestamp);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "parameter", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "cluster", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "application_id", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "error", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "db", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "name", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "pid", type::DataType::kInt);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "cur_task", type::DataType::kString);
    codec::SchemaCodec::SetColumnDesc(schema.Add(), "component", type::DataType::kString);
    return schema;
}

bool JobTableHelper::NeedLikeMatch(const std::string& pattern) {
    if (!pattern.empty() &&
            (pattern.find('%') != std::string::npos || pattern.find('_') != std::string::npos)) {
        return true;
    }
    return false;
}

bool JobTableHelper::IsMatch(const std::string& pattern, const std::string& value) {
    bool matched = false;
    bool is_null = false;
    base::StringRef value_ref(value);
    base::StringRef pattern_ref(pattern);
    hybridse::udf::v1::like(&value_ref, &pattern_ref, &matched, &is_null);
    if (is_null || !matched) {
        return false;
    }
    return true;
}

std::shared_ptr<hybridse::sdk::ResultSet> JobTableHelper::MakeResultSet(const PBOpStatus& ops,
        const std::string& like_pattern, ::hybridse::sdk::Status* status) {
    static schema::PBSchema schema = GetSchema();
    std::vector<std::vector<std::string>> records;
    for (const auto& op_status : ops) {
        std::string op_id = std::to_string(op_status.op_id());
        if (!like_pattern.empty() && !IsMatch(like_pattern, op_id)) {
            continue;
        }
        auto iter = STATE_MAP.find(op_status.status());
        if (iter == STATE_MAP.end()) {
            *status = {-1, "unknow status " + op_status.status()};
            return {};
        }
        std::vector<std::string> vec = {
            op_id,
            op_status.op_type(),
            iter->second,
            op_status.start_time() > 0 ? std::to_string(op_status.start_time() * 1000) : "null",
            op_status.end_time() > 0 ? std::to_string(op_status.end_time() * 1000) : "null",
            "null",
            "null",
            "null",
            "null",
            op_status.db(),
            op_status.name(),
            std::to_string(op_status.pid()),
            op_status.task_type(),
            "NameServer"
        };
        records.emplace_back(std::move(vec));
    }
    return ResultSetSQL::MakeResultSet(schema, records, status);
}

std::shared_ptr<hybridse::sdk::ResultSet> JobTableHelper::MakeResultSet(
        const std::shared_ptr<hybridse::sdk::ResultSet>& rs,
        const std::string& like_pattern, ::hybridse::sdk::Status* status) {
    static schema::PBSchema schema = GetSchema();
    std::vector<std::vector<std::string>> records;
    *status = {-1, "decode error"};
    while (rs->Next()) {
        int32_t op_id = 0;
        if (!rs->GetInt32(0, &op_id)) {
            return {};
        }
        std::string op_id_str = std::to_string(op_id);
        if (!like_pattern.empty() && !IsMatch(like_pattern, op_id_str)) {
            continue;
        }
        std::vector<std::string> vec = {op_id_str};
        auto fill_fun = [](const std::shared_ptr<hybridse::sdk::ResultSet>& rs,
                std::vector<std::string>* vec) -> ::hybridse::sdk::Status {
            for (int i = 1; i < rs->GetSchema()->GetColumnCnt(); i++) {
                if (rs->IsNULL(i)) {
                    vec->push_back("null");
                    continue;
                }
                switch (rs->GetSchema()->GetColumnType(i)) {
                    case ::hybridse::sdk::DataType::kTypeInt32: {
                        int32_t val = 0;
                        if (!rs->GetInt32(i, &val)) {
                            return {-1, "decode int error"};
                        }
                        vec->push_back(std::to_string(val));
                        break;
                    }
                    case ::hybridse::sdk::DataType::kTypeString: {
                        std::string val;
                        if (!rs->GetString(i, &val)) {
                            return {-1, "decode int error"};
                        }
                        vec->push_back(std::move(val));
                        break;
                    }
                    case ::hybridse::sdk::DataType::kTypeTimestamp: {
                        int64_t val = 0;
                        if (!rs->GetTime(i, &val)) {
                            return {-1, "decode int error"};
                        }
                        vec->push_back(std::to_string(val));
                        break;
                    }
                    default:
                        return {-1, "invalid type"};
                }
            }
            return {};
        };
        *status = fill_fun(rs, &vec);
        if (!status->IsOK()) {
            return {};
        }
        vec.push_back("null");
        vec.push_back("null");
        vec.push_back("null");
        vec.push_back("null");
        vec.push_back("TaskManager");
        records.emplace_back(std::move(vec));
    }
    return ResultSetSQL::MakeResultSet(schema, records, status);
}

}  // namespace sdk
}  // namespace openmldb
