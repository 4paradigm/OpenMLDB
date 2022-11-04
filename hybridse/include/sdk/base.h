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

#ifndef HYBRIDSE_INCLUDE_SDK_BASE_H_
#define HYBRIDSE_INCLUDE_SDK_BASE_H_

#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <unordered_map>
#include <vector>

#include "absl/strings/str_cat.h"
#include "absl/strings/string_view.h"
#include "sdk/base_schema.h"

namespace hybridse {
namespace sdk {

// TODO(hw): error code ref hybridse::common::StatusCode
struct Status {
    Status() : code(0), msg("ok") {}
    Status(int status_code, absl::string_view msg_str) : code(status_code), msg(msg_str) {}
    Status(int status_code, absl::string_view msg_str, absl::string_view trace)
        : code(status_code), msg(msg_str), trace(trace) {}
    bool IsOK() const { return code == 0; }

    void SetOK() {
        code = 0;
        msg = "ok";
    }
    void SetCode(int c) { code = c; }
    void SetMsg(absl::string_view new_msg) { msg = new_msg; }
    void SetTraces(absl::string_view new_trace) { trace = new_trace; }
    void Prepend(absl::string_view pre) { msg = absl::StrCat(pre, "--", msg); }
    void Append(absl::string_view app) {
        msg.append("--");
        msg.append(app);
    }
    void Append(int other_code) {
        msg.append("--ReturnCode[");
        msg.append(std::to_string(other_code));
        msg.append("]");
    }

    Status CloneAndPrepend(absl::string_view pre_msg) const {
        if (IsOK()) {
            return *this;
        }
        return Status(code, absl::StrCat(pre_msg, "--", msg), trace);
    }

    std::string ToString() const {
        std::string cm("[");
        cm.append(std::to_string(code));
        cm.append("] ");
        cm.append(msg);
        return cm;
    };

    int code;
    // msg use prepend and append, it's better to use absl::Cord, but we may directly use msg
    std::string msg;
    std::string trace;
};

class Table {
 public:
    Table() : empty() {}
    virtual ~Table() {}
    virtual const std::string& GetName() { return empty; }
    virtual const std::string& GetCatalog() { return empty; }
    virtual uint64_t GetCreateTime() { return 0; }
    virtual const std::shared_ptr<Schema> GetSchema() { return std::shared_ptr<Schema>(); }

 private:
    std::string empty;
};

class ColumnTypes {
 public:
    ColumnTypes() : types_() {}
    void AddColumnType(hybridse::sdk::DataType type) { types_.push_back(type); }
    const hybridse::sdk::DataType GetColumnType(size_t idx) const { return types_[idx]; }
    const size_t GetTypeSize() const { return types_.size(); }

 private:
    std::vector<hybridse::sdk::DataType> types_;
};

class TableSet {
 public:
    TableSet() {}
    virtual ~TableSet() {}
    virtual bool Next() { return false; }
    virtual const std::shared_ptr<Table> GetTable() { return std::shared_ptr<Table>(); }
    virtual int32_t Size() { return 0; }
};

enum ProcedureType {
    kUnknow = -1,
    kReqProcedure = 0,
    kReqDeployment,
};

class ProcedureInfo {
 public:
    ProcedureInfo() {}
    virtual ~ProcedureInfo() {}
    virtual const std::string& GetDbName() const = 0;
    virtual const std::string& GetSpName() const = 0;
    virtual const std::string& GetSql() const = 0;
    virtual const hybridse::sdk::Schema& GetInputSchema() const = 0;
    virtual const hybridse::sdk::Schema& GetOutputSchema() const = 0;
    virtual const std::vector<std::string>& GetTables() const = 0;
    virtual const std::vector<std::string>& GetDbs() const = 0;
    virtual const std::string& GetMainTable() const = 0;
    virtual const std::string& GetMainDb() const = 0;
    virtual ProcedureType GetType() const = 0;
    virtual const std::string* GetOption(const std::string& key) const = 0;
    virtual const std::unordered_map<std::string, std::string>* GetOption() const = 0;
};

}  // namespace sdk
}  // namespace hybridse
#endif  // HYBRIDSE_INCLUDE_SDK_BASE_H_
