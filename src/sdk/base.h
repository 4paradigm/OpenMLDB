/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * Author: chenjing
 * Date: 2019/11/21
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_SDK_BASE_H_
#define SRC_SDK_BASE_H_

#include <stdint.h>
#include <memory>
#include <string>

namespace fesql {
namespace sdk {

struct Status {
    Status() : code(0), msg("ok") {}
    Status(int status_code, const std::string& msg_str)
        : code(status_code), msg(msg_str) {}
    int code;
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
        default:
            return "unknownType";
    }
}

class Schema {
 public:
    Schema():empty() {}
    virtual ~Schema() {}
    virtual int32_t GetColumnCnt() const {
        return 0;
    }
    virtual const std::string& GetColumnName(uint32_t index) const {
        return empty;
    }
    virtual const DataType GetColumnType(uint32_t index) const {
        return kTypeUnknow;
    }
    virtual const bool IsColumnNotNull(uint32_t index) const {
        return false;
    }
 private:
    std::string empty;
};

class Table {
 public:
    Table(): empty(){}
    virtual ~Table() {}
    virtual const std::string& GetName()  {
        return empty;
    }
    virtual const std::string& GetCatalog() {
        return empty;
    }
    virtual uint64_t GetCreateTime() {
        return 0;
    }
    virtual const std::shared_ptr<Schema> GetSchema() {
        return std::shared_ptr<Schema>();
    }
 private:
    std::string empty;
};

class TableSet {
 public:
    TableSet() {}
    virtual ~TableSet() {}
    virtual bool Next() {
        return false;
    }
    virtual const std::shared_ptr<Table> GetTable() {
        return std::shared_ptr<Table>();
    }
    virtual int32_t Size() {
        return 0;
    }
};

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_BASE_H_
