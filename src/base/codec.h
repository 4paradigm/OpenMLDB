//
// codec.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef RTIDB_BASE_CODEC_H
#define RTIDB_BASE_CODEC_H

#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <vector>
#include <map>
#include "storage/segment.h"
#include "logging.h"
#include "base/strings.h"
#include "base/endianconv.h"

using ::rtidb::storage::DataBlock;

using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;

namespace rtidb {
namespace base {

static inline void Encode(uint64_t time, const char* data, const size_t size, char* buffer, uint32_t offset) {
    buffer += offset;
    uint32_t total_size = 8 + size;
    PDLOG(DEBUG, "encode size %d", total_size);
    memcpy(buffer, static_cast<const void*>(&total_size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&time), 8);
    memrev64ifbe(buffer);
    buffer += 8;
    memcpy(buffer, static_cast<const void*>(data), size);
}

static inline void Encode(uint64_t time, const DataBlock* data, char* buffer, uint32_t offset) {
    return Encode(time, data->data, data->size, buffer, offset);
}

static inline void Encode(const char* data, const size_t size, char* buffer, uint32_t offset) {
    buffer += offset;
    memcpy(buffer, static_cast<const void*>(&size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(data), size);
}

static inline void Encode(const DataBlock* data, char* buffer, uint32_t offset) {
    return Encode(data->data, data->size, buffer, offset);
}

static inline int32_t EncodeRows(const std::vector<::rtidb::base::Slice>& rows,
                                 uint32_t total_block_size, std::string* body) {
    if (body == NULL) {
        PDLOG(WARNING, "invalid output body");
        return -1;
    }

    uint32_t total_size = rows.size() * 4 + total_block_size;
    if (rows.size() > 0) {
        body->resize(total_size);
    }
    uint32_t offset = 0;
    char* rbuffer = reinterpret_cast<char*>(& ((*body)[0]));
    for (auto lit = rows.begin(); lit != rows.end(); ++lit) {
        ::rtidb::base::Encode(lit->data(), lit->size(), rbuffer, offset);
        offset += (4 + lit->size());
    }
    return total_size;
}

static inline int32_t EncodeRows(const std::vector<std::pair<uint64_t, ::rtidb::base::Slice>>& rows,
                                 uint32_t total_block_size, std::string* pairs) {

    if (pairs == NULL) {
        PDLOG(WARNING, "invalid output pairs");
        return -1;
    }

    uint32_t total_size = rows.size() * (8+4) + total_block_size;
    if (rows.size() > 0) {
        pairs->resize(total_size);
    }

    char* rbuffer = reinterpret_cast<char*>(& ((*pairs)[0]));
    uint32_t offset = 0;
    for (auto lit = rows.begin(); lit != rows.end(); ++lit) {
        const std::pair<uint64_t, ::rtidb::base::Slice>& pair = *lit;
        ::rtidb::base::Encode(pair.first, pair.second.data(), pair.second.size(), rbuffer, offset);
        offset += (4 + 8 + pair.second.size());
    }
    return total_size;
}

// encode pk, ts and value
static inline void EncodeFull(const std::string& pk, uint64_t time, const char* data, const size_t size, char* buffer, uint32_t offset) {
    buffer += offset;
    uint32_t pk_size = pk.length();
    uint32_t total_size = 8 + pk_size + size;
    PDLOG(DEBUG, "encode total size %u pk size %u", total_size, pk_size);
    memcpy(buffer, static_cast<const void*>(&total_size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&pk_size), 4);
    memrev32ifbe(buffer);
    buffer += 4;
    memcpy(buffer, static_cast<const void*>(&time), 8);
    memrev64ifbe(buffer);
    buffer += 8;
    memcpy(buffer, static_cast<const void*>(pk.c_str()), pk_size);
    buffer += pk_size;
    memcpy(buffer, static_cast<const void*>(data), size);
}

static inline void EncodeFull(const std::string& pk, uint64_t time, const DataBlock* data, char* buffer, uint32_t offset) {
    return EncodeFull(pk, time, data->data, data->size, buffer, offset);
}

static inline void Decode(const std::string* str, std::vector<std::pair<uint64_t, std::string*> >& pairs) {
    const char* buffer = str->c_str();
    uint32_t total_size = str->length();
    PDLOG(DEBUG, "total size %d %s", total_size, DebugString(*str).c_str());
    while (total_size > 0) {
        uint32_t size = 0;
        memcpy(static_cast<void*>(&size), buffer, 4);
        memrev32ifbe(static_cast<void*>(&size));
        PDLOG(DEBUG, "decode size %d", size);
        buffer += 4;
        uint64_t time = 0;
        memcpy(static_cast<void*>(&time), buffer, 8);
        memrev64ifbe(static_cast<void*>(&time));
        buffer += 8;
        assert(size >= 8);
        std::string* data = new std::string(size - 8, '0');
        memcpy(reinterpret_cast<char*>(& ((*data)[0])), buffer, size - 8);
        buffer += (size - 8);
        pairs.push_back(std::make_pair(time, data));
        total_size -= (size + 4);
    }
}

static inline void DecodeFull(const std::string* str, std::map<std::string, std::vector<std::pair<uint64_t, std::string*>>>& value_map) {
    const char* buffer = str->c_str();
    uint32_t total_size = str->length();
    PDLOG(DEBUG, "total size %u %s", total_size, DebugString(*str).c_str());
    while (total_size > 0) {
        uint32_t size = 0;
        memcpy(static_cast<void*>(&size), buffer, 4);
        memrev32ifbe(static_cast<void*>(&size));
        PDLOG(DEBUG, "decode size %u", size);
        buffer += 4;
        uint32_t pk_size = 0;
        memcpy(static_cast<void*>(&pk_size), buffer, 4);
        buffer += 4;
        memrev32ifbe(static_cast<void*>(&pk_size));
        PDLOG(DEBUG, "decode size %u", pk_size);
        assert(size > pk_size + 8);
        uint64_t time = 0;
        memcpy(static_cast<void*>(&time), buffer, 8);
        memrev64ifbe(static_cast<void*>(&time));
        buffer += 8;
        std::string pk(buffer, pk_size);
        buffer += pk_size;
        uint32_t value_size = size - 8 - pk_size;
        std::string* data = new std::string(value_size, '0');
        memcpy(reinterpret_cast<char*>(& ((*data)[0])), buffer, value_size);
        buffer += value_size;
        if (value_map.find(pk) == value_map.end()) {
            value_map.insert(std::make_pair(pk, std::vector<std::pair<uint64_t, std::string*>>()));
        }
        value_map[pk].push_back(std::make_pair(time, data));
        total_size -= (size + 8);
    }
}

using Schema = ::google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDef>;
static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
static constexpr uint8_t HEADER_LENGTH = VERSION_LENGTH + SIZE_LENGTH;
static constexpr uint32_t UINT24_MAX = (1 << 24) - 1;

class RowBuilder {
 public:
    explicit RowBuilder(const Schema& schema);
    ~RowBuilder() = default;

    uint32_t CalTotalLength(uint32_t string_length);
    bool SetBuffer(int8_t* buf, uint32_t size);
    bool AppendBool(bool val);
    bool AppendInt32(int32_t val);
    bool AppendInt16(int16_t val);
    bool AppendInt64(int64_t val);
    bool AppendTimestamp(int64_t val);
    bool AppendFloat(float val);
    bool AppendDouble(double val);
    bool AppendString(const char* val, uint32_t length);
    bool AppendNULL();

 private:
    bool Check(::rtidb::common::DataType type);

 private:
    const Schema& schema_;
    int8_t* buf_;
    uint32_t cnt_;
    uint32_t size_;
    uint32_t str_field_cnt_;
    uint32_t str_addr_length_;
    uint32_t str_field_start_offset_;
    uint32_t str_offset_;
    std::vector<uint32_t> offset_vec_;
};

class RowView {
 public:
    RowView(const Schema& schema, const int8_t* row, uint32_t size);
    explicit RowView(const Schema& schema);
    ~RowView() = default;
    bool Reset(const int8_t* row, uint32_t size);
    bool Reset(const int8_t* row);

    int32_t GetBool(uint32_t idx, bool* val);
    int32_t GetInt32(uint32_t idx, int32_t* val);
    int32_t GetInt64(uint32_t idx, int64_t* val);
    int32_t GetTimestamp(uint32_t idx, int64_t* val);
    int32_t GetInt16(uint32_t idx, int16_t* val);
    int32_t GetFloat(uint32_t idx, float* val);
    int32_t GetDouble(uint32_t idx, double* val);
    int32_t GetString(uint32_t idx, char** val, uint32_t* length);
    bool IsNULL(uint32_t idx) { return IsNULL(row_, idx); }
    inline uint32_t GetSize() { return size_; }

    static inline uint32_t GetSize(const int8_t* row) {
        return *(reinterpret_cast<const uint32_t*>(row + VERSION_LENGTH));
    }

    int32_t GetValue(const int8_t* row, uint32_t idx, ::rtidb::common::DataType type,
                     void* val);

    int32_t GetInteger(const int8_t* row, uint32_t idx,
                       ::rtidb::common::DataType type, int64_t* val);
    int32_t GetValue(const int8_t* row, uint32_t idx, char** val,
                     uint32_t* length);

 private:
    bool Init();
    bool CheckValid(uint32_t idx, ::rtidb::common::DataType type);

    inline bool IsNULL(const int8_t* row, uint32_t idx) {
        const int8_t* ptr = row + HEADER_LENGTH + (idx >> 3);
        return *(reinterpret_cast<const uint8_t*>(ptr)) & (1 << (idx & 0x07));
    }

 private:
    uint8_t str_addr_length_;
    bool is_valid_;
    uint32_t string_field_cnt_;
    uint32_t str_field_start_offset_;
    uint32_t size_;
    const int8_t* row_;
    const Schema& schema_;
    std::vector<uint32_t> offset_vec_;
};

namespace v1 {

static constexpr uint8_t VERSION_LENGTH = 2;
static constexpr uint8_t SIZE_LENGTH = 4;
// calc the total row size with primary_size, str field count and str_size
inline uint32_t CalcTotalLength(uint32_t primary_size, uint32_t str_field_cnt,
                                uint32_t str_size, uint32_t* str_addr_space) {
    uint32_t total_size = primary_size + str_size;
    if (total_size + str_field_cnt <= UINT8_MAX) {
        *str_addr_space = 1;
        return total_size + str_field_cnt;
    } else if (total_size + str_field_cnt * 2 <= UINT16_MAX) {
        *str_addr_space = 2;
        return total_size + str_field_cnt * 2;
    } else if (total_size + str_field_cnt * 3 <= 1 << 24) {
        *str_addr_space = 3;
        return total_size + str_field_cnt * 3;
    } else {
        *str_addr_space = 4;
        return total_size + str_field_cnt * 4;
    }
}
inline int32_t AppendInt16(int8_t* buf_ptr, uint32_t buf_size, int16_t val,
                           uint32_t field_offset) {
    if (field_offset + 2 > buf_size) {
        return -1;
    }
    *(reinterpret_cast<int16_t*>(buf_ptr + field_offset)) = val;
    return 4;
}

inline int32_t AppendFloat(int8_t* buf_ptr, uint32_t buf_size, float val,
                           uint32_t field_offset) {
    if (field_offset + 4 > buf_size) {
        return -1;
    }
    *(reinterpret_cast<float*>(buf_ptr + field_offset)) = val;
    return 4;
}

inline int32_t AppendInt32(int8_t* buf_ptr, uint32_t buf_size, int32_t val,
                           uint32_t field_offset) {
    if (field_offset + 4 > buf_size) {
        return -1;
    }
    *(reinterpret_cast<int32_t*>(buf_ptr + field_offset)) = val;
    return 4;
}

inline int32_t AppendInt64(int8_t* buf_ptr, uint32_t buf_size, int64_t val,
                           uint32_t field_offset) {
    if (field_offset + 8 > buf_size) {
        return -1;
    }
    *(reinterpret_cast<int64_t*>(buf_ptr + field_offset)) = val;
    return 8;
}

inline int32_t AppendDouble(int8_t* buf_ptr, uint32_t buf_size, double val,
                            uint32_t field_offset) {
    if (field_offset + 8 > buf_size) {
        return -1;
    }

    *(reinterpret_cast<double*>(buf_ptr + field_offset)) = val;
    return 8;
}

int32_t AppendString(int8_t* buf_ptr, uint32_t buf_size, int8_t* val,
                     uint32_t size, uint32_t str_start_offset,
                     uint32_t str_field_offset, uint32_t str_addr_space,
                     uint32_t str_body_offset);

inline int8_t GetAddrSpace(uint32_t size) {
    if (size <= UINT8_MAX) {
        return 1;
    } else if (size <= UINT16_MAX) {
        return 2;
    } else if (size <= 1 << 24) {
        return 3;
    } else {
        return 4;
    }
}

inline int8_t GetBoolField(const int8_t* row, uint32_t offset) {
    int8_t value = *(row + offset);
    return value;
}

inline int16_t GetInt16Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int16_t*>(row + offset));
}

inline int32_t GetInt32Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int32_t*>(row + offset));
}

inline int64_t GetInt64Field(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const int64_t*>(row + offset));
}

inline float GetFloatField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const float*>(row + offset));
}

inline double GetDoubleField(const int8_t* row, uint32_t offset) {
    return *(reinterpret_cast<const double*>(row + offset));
}

// native get string field method
int32_t GetStrField(const int8_t* row, uint32_t str_field_offset,
                    uint32_t next_str_field_offset, uint32_t str_start_offset,
                    uint32_t addr_space, int8_t** data, uint32_t* size);
int32_t GetCol(int8_t* input, int32_t offset, int32_t type_id, int8_t* data);
int32_t GetStrCol(int8_t* input, int32_t str_field_offset,
                  int32_t next_str_field_offset, int32_t str_start_offset,
                  int32_t type_id, int8_t* data);
}  // namespace v1

}
}

#endif /* !CODEC_H */

