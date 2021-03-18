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

#ifndef SRC_CASE_CASE_DATA_MOCK_H_
#define SRC_CASE_CASE_DATA_MOCK_H_

#include <memory>
#include <random>
#include <string>
#include <vector>
#include "case/sql_case.h"
#include "codec/fe_row_codec.h"
#include "vm/catalog.h"
#include "vm/mem_catalog.h"
namespace hybridse {
namespace sqlcase {
using hybridse::codec::Row;
template <class T>
class Repeater {
 public:
    Repeater() : idx_(0), values_({}) {}
    explicit Repeater(T value) : idx_(0), values_({value}) {}
    explicit Repeater(const std::vector<T>& values)
        : idx_(0), values_(values) {}

    virtual T GetValue() {
        T value = values_[idx_];
        idx_ = (idx_ + 1) % values_.size();
        return value;
    }

    uint32_t idx_;
    std::vector<T> values_;
};

template <class T>
class NumberRepeater : public Repeater<T> {
 public:
    void Range(T min, T max, T step) {
        this->values_.clear();
        for (T v = min; v <= max; v += step) {
            this->values_.push_back(v);
        }
    }
};

template <class T>
class IntRepeater : public NumberRepeater<T> {
 public:
    void Random(T min, T max, int32_t random_size) {
        this->values_.clear();
        std::default_random_engine e;
        std::uniform_int_distribution<T> u(min, max);
        for (int i = 0; i < random_size; ++i) {
            this->values_.push_back(u(e));
        }
    }
};

template <class T>
class RealRepeater : public NumberRepeater<T> {
 public:
    void Random(T min, T max, int32_t random_size) {
        std::default_random_engine e;
        std::uniform_real_distribution<T> u(min, max);
        for (int i = 0; i < random_size; ++i) {
            this->values_.push_back(u(e));
        }
    }
};

class CaseDataMock {
 public:
    static void BuildOnePkTableData(type::TableDef& table_def,  // NOLINT
                             std::vector<Row>& buffer,   // NOLINT
                             int64_t data_size);
    static void BuildTableAndData(type::TableDef& table_def,  // NOLINT
                                    std::vector<Row>& buffer,   // NOLINT
                                    int64_t data_size);
    static bool LoadResource(const std::string& resource_path,
                             type::TableDef& table_def,  // NOLINT
                             std::vector<Row>& rows);    // NOLINT
};

class CaseSchemaMock {
 public:
    static void BuildTableDef(::hybridse::type::TableDef& table); // NOLINT
};




}  // namespace sqlcase
}  // namespace hybridse

#endif  // SRC_CASE_CASE_DATA_MOCK_H_
