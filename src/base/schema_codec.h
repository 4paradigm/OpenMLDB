//
// schema_codec.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-23
//

#ifndef RTIDB_SCHEMA_CODEC_H
#define RTIDB_SCHEMA_CODEC_H
#include <vector>
#include <map>
#include <string>
#include <cstring>
#include <iostream>

namespace rtidb {
namespace base {

enum ColType {
    kString = 0,
    kFloat = 1,
    kInt32 = 2,
    kInt64 = 3,
    kDouble = 4,
    kNull = 5,
    kUInt32 = 6,
    kUInt64 = 7
};

struct Column {
    ColType type;
    std::string buffer;
};

struct ColumnDesc {
    uint32_t idx;
    ColType type;
    std::string name;
};

class SchemaCodec {

public:
    void Encode(const std::vector<std::pair<ColType, std::string> >& columns, 
                std::string& buffer) {
        uint32_t byte_size = GetSize(columns);     
        buffer.resize(byte_size);
        char* cbuffer = reinterpret_cast<char*>(&(buffer[0]));
        for (uint32_t i = 0; i < columns.size(); i++) {
            uint8_t type = (uint8_t) columns[i].first;
            const std::string& name = columns[i].second;
            memcpy(cbuffer, static_cast<const void*>(&type), 1);
            cbuffer += 1;
            //TODO limit the name length
            uint8_t name_size = (uint8_t)name.size();
            memcpy(cbuffer, static_cast<const void*>(&name_size), 1);
            cbuffer += 1;
            memcpy(cbuffer, static_cast<const void*>(name.c_str()), name_size);
            cbuffer += name_size;
        }
    }

    void Decode(const std::string& schema, std::vector<std::pair<ColType, std::string>>& columns) {
        const char* buffer = schema.c_str();
        uint32_t read_size = 0;
        while (read_size < schema.size()) {
            if (schema.size() - read_size < 2) {
                return;
            }
            uint8_t type = 0;
            memcpy(static_cast<void*>(&type), buffer, 1);
            buffer += 1;
            uint8_t name_size = 0;
            memcpy(static_cast<void*>(&name_size), buffer, 1);
            buffer += 1;
            uint32_t total_size = 2 + name_size;
            if (schema.size() - read_size < total_size) {
                return;
            }
            std::string name(buffer, name_size);
            buffer += name_size;
            read_size += total_size;
            columns.push_back(std::pair<ColType, std::string>(static_cast<ColType>(type), name));
        }
    }

private:
    uint32_t GetSize(const std::vector<std::pair<ColType, std::string> >& columns) {
        uint32_t byte_size = 0;
        for (uint32_t i = 0; i < columns.size(); i++) {
            byte_size += (1 + 1 + columns[i].second.size());
        }
        return byte_size;
    }

};

}
}

#endif
