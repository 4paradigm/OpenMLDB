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

#ifndef HYBRIDSE_SRC_VM_AGGREGATOR_H_
#define HYBRIDSE_SRC_VM_AGGREGATOR_H_

#include <string>
#include "codec/fe_row_codec.h"

namespace hybridse {
namespace vm {

using codec::Row;
using codec::RowView;
using vm::Schema;

class BaseAggregator {
 public:
    BaseAggregator(type::Type type, const Schema& output_schema)
        : type_(type), output_schema_(output_schema), row_builder_(output_schema) {}

    virtual ~BaseAggregator() {}

    virtual void Update(const std::string& val) = 0;

    // output final row
    virtual Row Output() = 0;

    type::Type type() const {
        return type_;
    }

 protected:
    type::Type type_;
    const Schema& output_schema_;
    codec::RowBuilder row_builder_;
};

template <class T>
class Aggregator : public BaseAggregator {
 public:
    Aggregator(type::Type type, const Schema& output_schema, T init_val = 0)
        : BaseAggregator(type, output_schema), val_(0) {}

    ~Aggregator() override {}

    virtual void Update(T val) = 0;
    void Update(const std::string& val) override = 0;

    Row Output() override {
        int str_len = 0;
        auto output_type = output_schema_.Get(0).type();
        DLOG(INFO) << "output_type = " << Type_Name(output_type);
        if (output_type == type::kVarchar) {
            str_len = sizeof(T);
        }

        uint32_t total_len = this->row_builder_.CalTotalLength(str_len);
        int8_t* buf = static_cast<int8_t*>(malloc(total_len));
        this->row_builder_.SetBuffer(buf, total_len);

        switch (output_type) {
            case type::kInt16:
                this->row_builder_.AppendInt16(val_);
                break;
            case type::kInt32:
                this->row_builder_.AppendInt32(val_);
                break;
            case type::kInt64:
                this->row_builder_.AppendInt64(val_);
                break;
            case type::kFloat:
                this->row_builder_.AppendFloat(val_);
                break;
            case type::kDouble:
                this->row_builder_.AppendDouble(val_);
                break;
            case type::kVarchar: {
                this->row_builder_.AppendString(reinterpret_cast<char*>(&val_), str_len);
                break;
            }
            default:
                LOG(ERROR) << "Aggregator not support type: " << Type_Name(output_type);
                break;
        }
        return Row(base::RefCountedSlice::CreateManaged(buf, total_len));
    }

 protected:
    T val_ = 0;
};

template <class T>
class SumStateAggregator : public Aggregator<T> {
 public:
    SumStateAggregator(type::Type type, const Schema& output_schema, T init_val = 0)
        : Aggregator<T>(type, output_schema, init_val) {}

    void Update(T val) override {
        this->val_ += val;
        DLOG(INFO) << "Update " << Type_Name(this->type_) << " val " << val << ", sum = " << this->val_;
    }

    void Update(const std::string& bval) override {
        T val = *reinterpret_cast<const T*>(bval.c_str());
        DLOG(INFO) << "Update binary value " << val;
        Update(val);
    }
};
}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_AGGREGATOR_H_
