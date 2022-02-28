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

#pragma once

#include <stdint.h>
#include <string.h>

#include <algorithm>
#include <string>
#include <vector>

#include "base/endianconv.h"
#include "base/glog_wapper.h"
#include "base/strings.h"
#include "boost/lexical_cast.hpp"
#include "codec/memcomparable_format.h"
#include "proto/type.pb.h"
#include "sdk/base.h"

namespace openmldb {
namespace codec {

using ::openmldb::type::DataType;
/**
 *  encode part
 */
static inline void Convert(bool data, char* buffer) {
    uint8_t bool_true = 1;
    uint8_t bool_false = 0;
    if (data) {
        memcpy(buffer, static_cast<const void*>(&bool_true), 1);
    } else {
        memcpy(buffer, static_cast<const void*>(&bool_false), 1);
    }
}

static inline void Convert(int16_t data, char* buffer) {
    memrev16ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 2);
}

static inline void Convert(int32_t data, char* buffer) {
    memrev32ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 4);
}

static inline void Convert(int64_t data, char* buffer) {
    memrev64ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 8);
}

static inline void Convert(float data, char* buffer) {
    memrev32ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 4);
}

static inline void Convert(double data, char* buffer) {
    memrev64ifbe(static_cast<void*>(&data));
    memcpy(buffer, static_cast<const void*>(&data), 8);
}

static inline bool Convert(const std::string& str, DataType data_type, std::string* out) {
    try {
        switch (data_type) {
            case ::openmldb::type::kBool: {
                out->resize(1);
                char* buffer = const_cast<char*>(out->data());
                std::string tmp = str;
                std::transform(tmp.begin(), tmp.end(), tmp.begin(), ::tolower);
                if (tmp == "true") {
                    Convert(true, buffer);
                } else if (tmp == "false") {
                    Convert(false, buffer);
                } else {
                    PDLOG(WARNING, "input format error, %s.", str.c_str());
                    return false;
                }
                break;
            }
            case ::openmldb::type::kSmallInt: {
                out->resize(2);
                char* buffer = const_cast<char*>(out->data());
                int16_t val = boost::lexical_cast<int16_t>(str);
                Convert(val, buffer);
                break;
            }
            case ::openmldb::type::kInt: {
                out->resize(4);
                char* buffer = const_cast<char*>(out->data());
                int32_t val = boost::lexical_cast<int32_t>(str);
                Convert(val, buffer);
                break;
            }
            case ::openmldb::type::kBigInt:
            case ::openmldb::type::kTimestamp: {
                out->resize(8);
                char* buffer = const_cast<char*>(out->data());
                int64_t val = boost::lexical_cast<int64_t>(str);
                Convert(val, buffer);
                break;
            }
            case ::openmldb::type::kFloat: {
                out->resize(4);
                char* buffer = const_cast<char*>(out->data());
                float val = boost::lexical_cast<float>(str);
                Convert(val, buffer);
                break;
            }
            case ::openmldb::type::kDouble: {
                out->resize(8);
                char* buffer = const_cast<char*>(out->data());
                double val = boost::lexical_cast<double>(str);
                Convert(val, buffer);
                break;
            }
            case ::openmldb::type::kVarchar:
            case ::openmldb::type::kString: {
                *out = str;
                break;
            }
            case ::openmldb::type::kDate: {
                std::vector<std::string> parts;
                ::openmldb::base::SplitString(str, "-", parts);
                if (parts.size() != 3) {
                    PDLOG(WARNING, "bad data format, data type %s.", openmldb::type::DataType_Name(data_type).c_str());
                    return false;
                }
                uint32_t year = boost::lexical_cast<uint32_t>(parts[0]);
                uint32_t month = boost::lexical_cast<uint32_t>(parts[1]);
                uint32_t day = boost::lexical_cast<uint32_t>(parts[2]);
                if (year < 1900 || year > 9999) return false;
                if (month < 1 || month > 12) return false;
                if (day < 1 || day > 31) return false;
                int32_t data = (year - 1900) << 16;
                data = data | ((month - 1) << 8);
                data = data | day;
                out->resize(4);
                char* buffer = const_cast<char*>(out->data());
                Convert(data, buffer);
                break;
            }
            default: {
                PDLOG(WARNING, "unsupported data type %s.", openmldb::type::DataType_Name(data_type).c_str());
                return false;
            }
        }
    } catch (std::exception const& e) {
        PDLOG(WARNING, "input format error, %s.", str.c_str());
        return false;
    }
    return true;
}

/**
 *  decode part
 */
static inline void GetBool(const char* ch, void* res) { memcpy(res, ch, 1); }

static inline void GetInt16(const char* ch, void* res) {
    memcpy(res, ch, 2);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetInt32(const char* ch, void* res) {
    memcpy(res, ch, 4);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetInt64(const char* ch, void* res) {
    memcpy(res, ch, 8);
    memrev64ifbe(static_cast<void*>(res));
}

static inline void GetFloat(const char* ch, void* res) {
    memcpy(res, ch, 4);
    memrev32ifbe(static_cast<void*>(res));
}

static inline void GetDouble(const char* ch, void* res) {
    memcpy(res, ch, 8);
    memrev64ifbe(static_cast<void*>(res));
}

__attribute__((unused)) static bool PackValue(const void* from, ::openmldb::type::DataType data_type,
                                              std::string* key) {
    size_t k_size = key->size();
    int ret = 0;
    switch (data_type) {
        case ::openmldb::type::kBool: {
            key->resize(sizeof(int8_t) + k_size);
            char* to = const_cast<char*>(key->data()) + k_size;
            ret = ::openmldb::codec::PackInteger(from, sizeof(int8_t), false, to);
            break;
        }
        case ::openmldb::type::kSmallInt: {
            key->resize(sizeof(int16_t) + k_size);
            char* to = const_cast<char*>(key->data()) + k_size;
            ret = ::openmldb::codec::PackInteger(from, sizeof(int16_t), false, to);
            break;
        }
        case ::openmldb::type::kInt:
        case ::openmldb::type::kDate: {
            key->resize(sizeof(int32_t) + k_size);
            char* to = const_cast<char*>(key->data()) + k_size;
            ret = ::openmldb::codec::PackInteger(from, sizeof(int32_t), false, to);
            break;
        }
        case ::openmldb::type::kBigInt:
        case ::openmldb::type::kTimestamp: {
            key->resize(sizeof(int64_t) + k_size);
            char* to = const_cast<char*>(key->data()) + k_size;
            ret = ::openmldb::codec::PackInteger(from, sizeof(int64_t), false, to);
            break;
        }
        case ::openmldb::type::kFloat: {
            key->resize(sizeof(float) + k_size);
            char* to = const_cast<char*>(key->data()) + k_size;
            ret = ::openmldb::codec::PackFloat(from, to);
            break;
        }
        case ::openmldb::type::kDouble: {
            key->resize(sizeof(double) + k_size);
            char* to = const_cast<char*>(key->data()) + k_size;
            ret = ::openmldb::codec::PackDouble(from, to);
            break;
        }
        default: {
            PDLOG(WARNING, "unsupported data type %s.", openmldb::type::DataType_Name(data_type).c_str());
            return false;
        }
    }
    if (ret < 0) {
        return false;
    }
    return true;
}

template <typename T>
static bool AppendColumnValue(const std::string& v, hybridse::sdk::DataType type, bool is_not_null,
                       const std::string& null_value, T row) {
    // check if null
    if (v == null_value) {
        if (is_not_null) {
            return false;
        }
        return row->AppendNULL();
    }
    try {
        switch (type) {
            case hybridse::sdk::kTypeBool: {
                bool ok = false;
                std::string b_val = v;
                std::transform(b_val.begin(), b_val.end(), b_val.begin(), ::tolower);
                if (b_val == "true") {
                    ok = row->AppendBool(true);
                } else if (b_val == "false") {
                    ok = row->AppendBool(false);
                }
                return ok;
            }
            case hybridse::sdk::kTypeInt16: {
                return row->AppendInt16(boost::lexical_cast<int16_t>(v));
            }
            case hybridse::sdk::kTypeInt32: {
                return row->AppendInt32(boost::lexical_cast<int32_t>(v));
            }
            case hybridse::sdk::kTypeInt64: {
                return row->AppendInt64(boost::lexical_cast<int64_t>(v));
            }
            case hybridse::sdk::kTypeFloat: {
                return row->AppendFloat(boost::lexical_cast<float>(v));
            }
            case hybridse::sdk::kTypeDouble: {
                return row->AppendDouble(boost::lexical_cast<double>(v));
            }
            case hybridse::sdk::kTypeString: {
                return row->AppendString(v);
            }
            case hybridse::sdk::kTypeDate: {
                std::vector<std::string> parts;
                ::openmldb::base::SplitString(v, "-", parts);
                if (parts.size() != 3) {
                    return false;
                }
                auto year = boost::lexical_cast<int32_t>(parts[0]);
                auto mon = boost::lexical_cast<int32_t>(parts[1]);
                auto day = boost::lexical_cast<int32_t>(parts[2]);
                return row->AppendDate(year, mon, day);
            }
            case hybridse::sdk::kTypeTimestamp: {
                return row->AppendTimestamp(boost::lexical_cast<int64_t>(v));
            }
            default: {
                return false;
            }
        }
    } catch (std::exception const& e) {
        return false;
    }
}

}  // namespace codec
}  // namespace openmldb
