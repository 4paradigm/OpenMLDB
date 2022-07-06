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
#include <string>
#include <vector>
#include <unordered_map>

namespace hybridse {
namespace sdk {

struct Status {
    Status() : code(0), msg("ok") {}
    Status(int status_code, const std::string& msg_str)
        : code(status_code), msg(msg_str) {}
    bool IsOK() const { return code == 0; }
    int code;
    std::string trace;
    std::string msg;
};

enum DataType {
    kTypeBool = 0,
    kTypeInt16,
    kTypeInt32,
    kTypeInt64,
    kTypeFloat,
    kTypeDouble,
    kTypeString,
    kTypeDate,
    kTypeTimestamp,
    kTypeUnknow
};

inline const std::string DataTypeName(const DataType& type) {
    switch (type) {
        case kTypeBool:
            return "bool";
        case kTypeInt16:
            return "int16";
        case kTypeInt32:
            return "int32";
        case kTypeInt64:
            return "int64";
        case kTypeFloat:
            return "float";
        case kTypeDouble:
            return "double";
        case kTypeString:
            return "string";
        case kTypeTimestamp:
            return "timestamp";
        case kTypeDate:
            return "date";
        default:
            return "unknownType";
    }
}

class Schema {
 public:
    Schema() : empty() {}
    virtual ~Schema() {}
    virtual int32_t GetColumnCnt() const { return 0; }
    virtual const std::string& GetColumnName(uint32_t index) const {
        return empty;
    }
    virtual const DataType GetColumnType(uint32_t index) const {
        return kTypeUnknow;
    }
    virtual const bool IsColumnNotNull(uint32_t index) const { return false; }
    virtual const bool IsConstant(uint32_t index) const { return false; }

 private:
    std::string empty;
};

class Table {
 public:
    Table() : empty() {}
    virtual ~Table() {}
    virtual const std::string& GetName() { return empty; }
    virtual const std::string& GetCatalog() { return empty; }
    virtual uint64_t GetCreateTime() { return 0; }
    virtual const std::shared_ptr<Schema> GetSchema() {
        return std::shared_ptr<Schema>();
    }

 private:
    std::string empty;
};

class ColumnTypes {
 public:
    ColumnTypes() : types_() {}
    void AddColumnType(hybridse::sdk::DataType type) {
        types_.push_back(type);
    }
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
    virtual const std::shared_ptr<Table> GetTable() {
        return std::shared_ptr<Table>();
    }
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
