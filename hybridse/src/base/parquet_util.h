/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_BASE_PARQUET_UTIL_H_
#define SRC_BASE_PARQUET_UTIL_H_

#include "glog/logging.h"
#include "parquet/schema.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace base {

inline bool MapParquetType(const parquet::ColumnDescriptor* column_desc,
                           type::Type* type) {
    if (column_desc == nullptr || type == nullptr) {
        LOG(WARNING) << "input args is nullptr";
        return false;
    }

    switch (column_desc->physical_type()) {
        case ::parquet::Type::BOOLEAN: {
            *type = ::hybridse::type::kBool;
            return true;
        }

        case ::parquet::Type::FLOAT: {
            *type = ::hybridse::type::kFloat;
            return true;
        }
        case ::parquet::Type::DOUBLE: {
            *type = ::hybridse::type::kDouble;
            return true;
        }

        case ::parquet::Type::INT32: {
            *type = ::hybridse::type::kInt32;
            return true;
        }

        case ::parquet::Type::INT64: {
            *type = ::hybridse::type::kInt64;
            return true;
        }
        default: {
        }
    }

    switch (column_desc->logical_type()->type()) {
        case ::parquet::LogicalType::Type::STRING: {
            *type = ::hybridse::type::kVarchar;
            return true;
        }
        case ::parquet::LogicalType::Type::DECIMAL: {
            switch (column_desc->physical_type()) {
                case ::parquet::Type::FLOAT: {
                    *type = ::hybridse::type::kFloat;
                    return true;
                }
                case ::parquet::Type::DOUBLE: {
                    *type = ::hybridse::type::kDouble;
                    return true;
                }
                default: {
                    LOG(WARNING)
                        << column_desc->ToString() << " is not supported type";
                    return false;
                }
            }
        }
        case ::parquet::LogicalType::Type::INT: {
            switch (column_desc->physical_type()) {
                case ::parquet::Type::INT32: {
                    *type = ::hybridse::type::kInt32;
                    return true;
                }
                case ::parquet::Type::INT64: {
                    *type = ::hybridse::type::kInt64;
                    return true;
                }
                default: {
                    LOG(WARNING)
                        << column_desc->ToString() << " is not supported type";
                    return false;
                }
            }
        }
        case ::parquet::LogicalType::Type::DATE: {
            *type = ::hybridse::type::kDate;
            return true;
        }
        case ::parquet::LogicalType::Type::TIMESTAMP:
        case ::parquet::LogicalType::Type::TIME: {
            *type = ::hybridse::type::kTimestamp;
            return true;
        }
        default: {
            LOG(WARNING) << column_desc->ToString() << " is not supported type";
            return false;
        }
    }
}

}  // namespace base
}  // namespace hybridse
#endif  // SRC_BASE_PARQUET_UTIL_H_
