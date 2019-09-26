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
static const uint16_t MAX_STRING_LENGTH = 32767;

class FlatArrayCodec {

public:
    FlatArrayCodec(std::string* buffer, 
            uint16_t col_cnt):buffer_(buffer), col_cnt_(col_cnt),
    cur_cnt_(0), datas_(col_cnt_), modify_times_(-1){
    }

    FlatArrayCodec(std::string* buffer, 
            uint16_t col_cnt, int modify_times):buffer_(buffer), col_cnt_(col_cnt),
    cur_cnt_(0), datas_(col_cnt_), modify_times_(modify_times){
    }
    FlatArrayCodec():col_cnt_(0),cur_cnt_(0),modify_times_(-1){}
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

        if (data.length() > MAX_STRING_LENGTH) {
            return false;
        }

        uint16_t size = (uint16_t)data.length();
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
        //for the case of adding field
        if (modify_times_ > -1) {
            uint8_t modify_count = (uint8_t)(modify_times_ | 0x80);
            memcpy(cbuffer, static_cast<const void*>(&modify_count), 1);
            cbuffer += 1;
        } else {
            if (col_cnt_ >= 128) {
                memcpy(cbuffer, static_cast<const void*>(&col_cnt_), 2);
                memrev16ifbe(static_cast<void*>(cbuffer));
                cbuffer += 2;
            } else {
                uint8_t col_cnt_tmp = (uint8_t)col_cnt_;
                memcpy(cbuffer, static_cast<const void*>(&col_cnt_tmp), 1);
                cbuffer += 1;
            }
        }
        std::vector<Column>::iterator it = datas_.begin();
        for (; it != datas_.end(); ++it) {
            Column& col = *it;
            memcpy(cbuffer, static_cast<const void*>(&col.type), 1);
            cbuffer += 1;
            uint16_t buffer_size = (uint16_t)col.buffer.size();
            if (buffer_size < 128) {
                uint8_t size_value = (uint8_t)buffer_size;
                memcpy(cbuffer, static_cast<const void*>(&size_value), 1);
                cbuffer += 1;
            } else {
                uint8_t first_value = (uint8_t)(buffer_size >> 8 | 0x80);
                uint8_t second_value = (uint8_t)(buffer_size & 0xFF);
                memcpy(cbuffer, static_cast<const void*>(&first_value), 1);
                cbuffer += 1;
                memcpy(cbuffer, static_cast<const void*>(&second_value), 1);
                cbuffer += 1;
            }
            memcpy(cbuffer, static_cast<const void*>(col.buffer.c_str()), col.buffer.size());
            cbuffer += col.buffer.size();
        }
    }

private:
    
    uint32_t GetSize() {
        // one byte for column count
        uint32_t size = 1;
        if (datas_.size() >= 128) {
            size++;
        }
        std::vector<Column>::iterator it = datas_.begin();
        for (; it != datas_.end(); ++it) {
            Column& col = *it;
            size += (col.buffer.size() + 1 + 1);
            if (col.buffer.size() >= 128) {
                size++;
            }
        }
        return size;
    }

    // encode data to buffer
    void Encode(const ColType& type, const void* data, uint16_t size) {
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
    uint16_t col_cnt_;
    uint16_t cur_cnt_;
    std::vector<Column> datas_;
    int modify_times_;
};

class FlatArrayIterator {

public:

    FlatArrayIterator(const char* buffer, uint32_t bsize, uint16_t column_size):buffer_(buffer), 
    col_cnt_(0), bsize_(bsize), type_(kUnknown), fsize_(0), offset_(0){
        //for the case of adding field
        if ((uint8_t)(buffer_[0] & 0x80) != 0) {
            memcpy(static_cast<void*>(&col_cnt_), buffer_, 1);
            buffer_ += 1;
            offset_ += 1;
        } else {
            //for the old data before adding field
            column_size = (uint16_t)(buffer_[0] & 0x7F);
            if (column_size >= 128) {
                memcpy(static_cast<void*>(&col_cnt_), buffer_, 2);
                memrev16ifbe(static_cast<void*>(&col_cnt_));
                buffer_ += 2;
                offset_ += 2;
            } else {
                uint8_t col_cnt_tmp = 0;
                memcpy(static_cast<void*>(&col_cnt_tmp), buffer_, 1);
                col_cnt_ = col_cnt_tmp;
                buffer_ += 1;
                offset_ += 1;
            }
        } 
        Next();
    }

    ~FlatArrayIterator() {}

    // Get the column count 
    uint16_t Size() {
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
        if (type_ != kUInt16) {
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
        if (type_ != kString && type_ != kEmptyString) {
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
        offset_++;
        if (((uint8_t)(buffer_[0]) & 0x80) == 0) {
            fsize_ = (uint8_t)(buffer_[0]);
            buffer_ += 1;
            offset_++;
        } else {
            fsize_ = (((uint8_t)(buffer_[0]) & 0x7F) << 8) | ((uint8_t)buffer_[1] & 0xFF);
            buffer_ += 2;
            offset_ += 2;
        }
    }

    bool Valid() {
        if (bsize_ < 2 || offset_ > bsize_) {
            return false;
        }
        return true;
    }

private:
    const char* buffer_;
    uint16_t col_cnt_;
    uint32_t bsize_;
    // some run time field
    ColType type_;
    // data size of field
    uint16_t fsize_;
    uint32_t offset_;
};

}
}

#endif /* !RTIDB_FLAT_ARRAY_H */
