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
    Schema() {}
    ~Schema() {}
    virtual int32_t GetColumnCnt() const = 0;
    virtual const std::string& GetColumnName(uint32_t index) const = 0;
    virtual const DataType GetColumnType(uint32_t index) const = 0;
    virtual const bool IsColumnNotNull(uint32_t index) const = 0;

};

class Table {
 public:
    Table() {}
    ~Table() {}
    virtual const std::string& GetName() = 0;
    virtual const std::string& GetCatalog() = 0;
    virtual uint64_t GetCreateTime() = 0;
    virtual const std::unique_ptr<Schema> GetSchema() = 0;
};

class TableSet {
 public:
    TableSet() {}
    ~TableSet() {}
    virtual bool Next() = 0;
    virtual const std::unique_ptr<Table> GetTable() = 0;
    virtual int32_t Size() = 0;
};

}  // namespace sdk
}  // namespace fesql
#endif  // SRC_SDK_BASE_H_
