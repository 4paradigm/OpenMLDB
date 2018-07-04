//
// flat_array.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-23
//


#ifndef RTIDB_FLAT_ARRAY_H
#define RTIDB_FLAT_ARRAY_H

#include <string>
#include <cstring>
#include <stdint.h>
#include <vector>
#include "base/schema_codec.h"
#include "base/endianconv.h"

namespace rtidb {
namespace base {

static const uint8_t bool_true = 1;
static const uint8_t bool_false = 0;
static const uint32_t max_row_size = 1024 * 1024;

class FlatArrayCodec {

public:
    FlatArrayCodec(std::string* buffer, 
                   uint8_t col_cnt):buffer_(buffer), col_cnt_(col_cnt),
    cur_cnt_(0), datas_(col_cnt_){
    }

    ~FlatArrayCodec() {}

    bool Append(bool data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        if (data) {
            Encode(kBool, static_cast<const void*>(&bool_true), 1);
        }else {
            Encode(kBool, static_cast<const void*>(&bool_false), 1);
        }
        cur_cnt_ ++;
        return true;
    }

    bool Append(uint16_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev16ifbe(static_cast<void*>(&data));
        Encode(kUInt16, static_cast<const void*>(&data), 2);
        cur_cnt_ ++;
        return true;
    }

    bool Append(int16_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev16ifbe(static_cast<void*>(&data));
        Encode(kInt16, static_cast<const void*>(&data), 2);
        cur_cnt_ ++;
        return true;
    }

    bool Append(float data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev32ifbe(static_cast<void*>(&data));
        Encode(kFloat, static_cast<const void*>(&data), 4);
        cur_cnt_ ++;
        return true;
    }

    bool Append(int32_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev32ifbe(static_cast<void*>(&data));
        Encode(kInt32, static_cast<const void*>(&data), 4);
        cur_cnt_ ++;
        return true;
    }

    bool Append(int64_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev64ifbe(static_cast<void*>(&data));
        Encode(kInt64, static_cast<const void*>(&data), 8);
        cur_cnt_ ++;
        return true;
    }

    bool Append(uint32_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev32ifbe(static_cast<void*>(&data));
        Encode(kUInt32, static_cast<const void*>(&data), 4);
        cur_cnt_ ++;
        return true;
    }

    bool Append(uint64_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev64ifbe(static_cast<void*>(&data));
        Encode(kUInt64, static_cast<const void*>(&data), 8);
        cur_cnt_ ++;
        return true;
    }

    bool AppendTimestamp(uint64_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev64ifbe(static_cast<void*>(&data));
        Encode(kTimestamp, static_cast<const void*>(&data), 8);
        cur_cnt_ ++;
        return true;
    }

    bool AppendDate(uint64_t data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev64ifbe(static_cast<void*>(&data));
        Encode(kDate, static_cast<const void*>(&data), 8);
        cur_cnt_ ++;
        return true;
    }

    bool Append(double data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        memrev64ifbe(static_cast<void*>(&data));
        Encode(kDouble, static_cast<const void*>(&data), 8);
        cur_cnt_ ++;
        return true;
    }

    bool Append(const std::string& data) {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }

        if (data.length() > 128) {
            return false;
        }

        uint8_t size = (uint8_t) data.length();
        Encode(kString, static_cast<const void*>(data.c_str()), size);
        cur_cnt_ ++;
        return true;
    }

    bool AppendNull() {
        if (cur_cnt_ >= col_cnt_) {
            return false;
        }
        Encode(kNull, NULL, 0);
        cur_cnt_ ++;
        return true;
    }

    void Build() {
        //TODO limit the total size of single row
        buffer_->resize(GetSize());
        char* cbuffer = reinterpret_cast<char*>(&((*buffer_)[0]));
        memcpy(cbuffer, static_cast<const void*>(&col_cnt_), 1);
        cbuffer += 1;
        std::vector<Column>::iterator it = datas_.begin();
        for (; it != datas_.end(); ++it) {
            Column& col = *it;
            memcpy(cbuffer, static_cast<const void*>(&col.type), 1);
            cbuffer += 1;
            uint8_t buffer_size = (uint8_t)col.buffer.size();
            memcpy(cbuffer, static_cast<const void*>(&buffer_size), 1);
            cbuffer += 1;
            memcpy(cbuffer, static_cast<const void*>(col.buffer.c_str()), col.buffer.size());
            cbuffer += col.buffer.size();
        }
    }

private:
    
    uint32_t GetSize() {
        // one byte for column count
        uint32_t size = 1;
        std::vector<Column>::iterator it = datas_.begin();
        for (; it != datas_.end(); ++it) {
            Column& col = *it;
            size += (col.buffer.size() + 1 + 1);
        }
        return size;
    }

    // encode data to buffer
    void Encode(const ColType& type, const void* data, uint8_t size) {
        Column& col = datas_[cur_cnt_];
        col.buffer.resize(size);
        char* buffer = reinterpret_cast<char*>(&(col.buffer[0]));
        col.type = type;
        if (type != kNull) {
            memcpy(buffer, data, size);
        }
    }

private:
    std::string* buffer_;
    uint8_t col_cnt_;
    uint8_t cur_cnt_;
    std::vector<Column> datas_;
};

class FlatArrayIterator {

public:

    FlatArrayIterator(const char* buffer, uint32_t bsize):buffer_(buffer), 
    col_cnt_(0), bsize_(bsize), type_(kUnknown), fsize_(0), offset_(0){
        memcpy(static_cast<void*>(&col_cnt_), buffer_, 1);
        buffer_ += 1;
        offset_ += 1;
        Next();
    }

    ~FlatArrayIterator() {}

    // Get the column count 
    uint8_t Size() {
        return col_cnt_;
    }

    // Get float can be only invoked once when after Next
    bool GetFloat(float* value) {
        if (type_ != kFloat) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 4 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 4);
        memrev32ifbe(static_cast<void*>(value));
        buffer_ += 4;
        offset_ += 4;
        return true;
    }

    bool GetUInt32(uint32_t* value) {
        if (type_ != kUInt32) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 4 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 4);
        memrev32ifbe(static_cast<void*>(value));
        buffer_ += 4;
        offset_ += 4;
        return true;
    }

    bool GetInt32(int32_t* value) {
        if (type_ != kInt32) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 4 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 4);
        memrev32ifbe(static_cast<void*>(value));
        buffer_ += 4;
        offset_ += 4;
        return true;
    }
    
    bool GetUInt64(uint64_t* value) {
        if (type_ != kUInt64) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 8 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 8);
        memrev64ifbe(static_cast<void*>(value));
        buffer_ += 8;
        offset_ += 8;
        return true;
    }

    bool GetBool(bool* value) {
        if (type_ != kBool) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 1 > bsize_) {
            return false;
        }

        uint8_t bool_value = 0;
        memcpy(static_cast<void*>(&bool_value), buffer_, 1);
        buffer_ += 1;
        offset_ += 1;
        if (bool_value == 1) {
            *value = true;
        }else {
            *value = false; 
        }
        return true;
    }

    bool GetInt64(int64_t* value) {
        if (type_ != kInt64) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 8 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 8);
        memrev64ifbe(static_cast<void*>(value));
        buffer_ += 8;
        offset_ += 8;
        return true;
    }

    bool GetUInt16(uint16_t* value) {
        if (type_ != kInt16) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 2 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 2);
        memrev16ifbe(static_cast<void*>(value));
        buffer_ += 2;
        offset_ += 2;
        return true;
    }

    bool GetInt16(int16_t* value) {
        if (type_ != kInt16) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 2 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 2);
        memrev16ifbe(static_cast<void*>(value));
        buffer_ += 2;
        offset_ += 2;
        return true;
    }

    bool GetDate(uint64_t* date) {
        if (type_ != kDate) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 8 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(date), buffer_, 8);
        memrev64ifbe(static_cast<void*>(date));
        buffer_ += 8;
        offset_ += 8;
        return true;
    }

    bool GetTimestamp(uint64_t* ts) {
        if (type_ != kTimestamp) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 8 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(ts), buffer_, 8);
        memrev64ifbe(static_cast<void*>(ts));
        buffer_ += 8;
        offset_ += 8;
        return true;
    }
    
    bool GetDouble(double* value) {
        if (type_ != kDouble) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + 8 > bsize_) {
            return false;
        }
        memcpy(static_cast<void*>(value), buffer_, 8);
        memrev64ifbe(static_cast<void*>(value));
        buffer_ += 8;
        offset_ += 8;
        return true;
    }

    bool GetString(std::string* value) {
        if (type_ != kString) {
            return false;
        }
        if (fsize_ == 0) {
            return true;
        }
        if (offset_ + fsize_ > bsize_) {
            return false;
        }
        value->resize(fsize_);
        memcpy(static_cast<void*>(&((*value)[0])), buffer_, fsize_);
        buffer_ += fsize_;
        offset_ += fsize_;
        return true;
    }

    ColType GetType() {
        return type_;
    }

    void Next() {
        if (offset_ + 2 > bsize_) {
            offset_ += 2;
            return;
        }
        uint8_t type = 0;
        memcpy(static_cast<void*>(&type), buffer_, 1);
        type_ = static_cast<ColType>(type);
        buffer_ += 1;
        memcpy(static_cast<void*>(&fsize_), buffer_, 1);
        buffer_ += 1;
        offset_ += 2;
    }

    bool Valid() {
        if (bsize_ < 2 || offset_ > bsize_) {
            return false;
        }
        return true;
    }

private:
    const char* buffer_;
    uint8_t col_cnt_;
    uint32_t bsize_;
    // some run time field
    ColType type_;
    // data size of field
    uint8_t fsize_;
    uint32_t offset_;
};

}
}

#endif /* !RTIDB_FLAT_ARRAY_H */
