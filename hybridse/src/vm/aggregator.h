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

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <boost/algorithm/string/compare.hpp>

#include "codec/fe_row_codec.h"
#include "codec/row.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace vm {

using codec::Row;
using codec::RowView;
using codec::Schema;

class BaseAggregator {
 public:
    BaseAggregator(type::Type type, const Schema& output_schema)
        : type_(type), output_schema_(output_schema), row_builder_(output_schema) {}

    virtual ~BaseAggregator() {}

    // update aggregator states by encoded string
    // used usually by update states from pre-agg talbe (encoded multi-rows)
    virtual void Update(const std::string& val) = 0;

    // output final row
    virtual Row Output() = 0;

    // aggr col type
    type::Type type() const {
        return type_;
    }

    // representative type of the aggr val
    virtual type::Type GetRepType() const {
        return type_;
    }

    virtual bool IsNull() const {
        return counter_ == 0;
    }

    virtual void Reset() {
        counter_ = 0;
    }

 protected:
    type::Type type_;
    const Schema& output_schema_;
    codec::RowBuilder row_builder_;
    int64_t counter_ = 0;
};

template <class T>
class Aggregator : public BaseAggregator {
 public:
    Aggregator(type::Type type, const Schema& output_schema, T init_val = 0)
        : BaseAggregator(type, output_schema), val_(init_val), init_val_(init_val) {}

    ~Aggregator() override {}

    virtual void UpdateValue(const T& val) = 0;

    void Update(const std::string& bval) override {
        UpdateInternal(bval);
    }

    virtual const T& val() {
        return val_;
    }

    Row Output() override {
        auto row = OutputInternal();
        // reset the aggregator
        Reset();
        return row;
    }

    void Reset() override {
        BaseAggregator::Reset();
        val_ = init_val_;
    }

 protected:
    // T is numeric
    template <class TT = T>
    void UpdateInternal(const std::string& bval, std::enable_if_t<std::is_arithmetic<TT>{}>* = nullptr) {
        if (bval.size() != sizeof(T)) {
            LOG(ERROR) << "ERROR: encoded aggr val is not valid";
            return;
        }

        T val = *reinterpret_cast<const T*>(bval.c_str());
        DLOG(INFO) << "Update binary value " << val;
        UpdateValue(val);
    }

    // T is std::string
    template <class TT = T>
    void UpdateInternal(const std::string& bval, std::enable_if_t<!std::is_arithmetic<TT>{}>* = nullptr) {
        UpdateValue(bval);
    }

    // T is numeric
    template <class TT = T>
    Row OutputInternal(std::enable_if_t<std::is_arithmetic<TT>{}>* = nullptr) {
        int str_len = 0;
        auto output_type = output_schema_.Get(0).type();
        DLOG(INFO) << "output_type = " << Type_Name(output_type);
        if (!IsNull() && output_type == type::kVarchar) {
            str_len = sizeof(T);
        }

        uint32_t total_len = this->row_builder_.CalTotalLength(str_len);
        int8_t* buf = static_cast<int8_t*>(malloc(total_len));
        this->row_builder_.SetBuffer(buf, total_len);

        if (IsNull()) {
            this->row_builder_.AppendNULL();
        } else {
            T output_val = val();
            switch (output_type) {
                case type::kInt16:
                    this->row_builder_.AppendInt16(output_val);
                    break;
                case type::kInt32:
                    this->row_builder_.AppendInt32(output_val);
                    break;
                case type::kDate:
                    this->row_builder_.AppendDate(output_val);
                    break;
                case type::kInt64:
                    this->row_builder_.AppendInt64(output_val);
                    break;
                case type::kTimestamp:
                    this->row_builder_.AppendTimestamp(output_val);
                    break;
                case type::kFloat:
                    this->row_builder_.AppendFloat(output_val);
                    break;
                case type::kDouble:
                    this->row_builder_.AppendDouble(output_val);
                    break;
                case type::kVarchar: {
                    this->row_builder_.AppendString(reinterpret_cast<char*>(&output_val), str_len);
                    break;
                }
                default:
                    LOG(ERROR) << "Aggregator not support type: " << Type_Name(output_type);
                    break;
            }
        }
        return Row(base::RefCountedSlice::CreateManaged(buf, total_len));
    }

    // T is std::string
    template <class TT = T>
    Row OutputInternal(std::enable_if_t<!std::is_arithmetic<TT>{}>* = nullptr) {
        int str_len = 0;
        auto output_type = output_schema_.Get(0).type();
        DLOG(INFO) << "output_type = " << Type_Name(output_type);
        if (output_type != type::kVarchar) {
            LOG(ERROR) << "Unexpect output type for aggregation on kVarchar columns";
            return Row();
        }

        if (!IsNull()) {
            str_len = val().length();
        }
        uint32_t total_len = this->row_builder_.CalTotalLength(str_len);
        int8_t* buf = static_cast<int8_t*>(malloc(total_len));
        this->row_builder_.SetBuffer(buf, total_len);

        if (IsNull()) {
            this->row_builder_.AppendNULL();
        } else {
            this->row_builder_.AppendString(this->val_.c_str(), str_len);
        }
        return Row(base::RefCountedSlice::CreateManaged(buf, total_len));
    }

    T val_ = 0;
    T init_val_ = 0;
};

template <class T>
class SumAggregator : public Aggregator<T> {
 public:
    SumAggregator(type::Type type, const Schema& output_schema)
        : Aggregator<T>(type, output_schema, 0) {}

    // val is assumed to be not null
    void UpdateValue(const T& val) override {
        this->val_ += val;
        this->counter_++;
        DLOG(INFO) << "Update " << Type_Name(this->type_) << " val " << val << ", sum = " << this->val_;
    }

    type::Type GetRepType() const override {
        switch (this->type()) {
            case type::kInt16:
            case type::kInt32:
            case type::kInt64:
            case type::kDate:  // actually sum doesn't support `Date` type
            case type::kTimestamp:
                return type::kInt64;
            default:
                return this->type();
        }
    }
};

class CountAggregator : public Aggregator<int64_t> {
 public:
    CountAggregator(type::Type type, const Schema& output_schema)
        : Aggregator<int64_t>(type, output_schema, 0) {}

    // val is assumed to be not null
    void UpdateValue(const int64_t& val) override {
        this->val_ += val;
        this->counter_++;
        DLOG(INFO) << "Update " << Type_Name(this->type_) << " val " << val << ", count = " << this->val_;
    }

    bool IsNull() const override {
        return false;
    }

    type::Type GetRepType() const override {
        return type::kInt64;
    }
};

class AvgAggregator : public Aggregator<double> {
 public:
    AvgAggregator(type::Type type, const Schema& output_schema)
        : Aggregator<double>(type, output_schema, 0) {}

    void UpdateValue(const double& sum_val) override {
        UpdateAvgValue(sum_val, 1);
    }

    // val is assumed to be not null
    void UpdateAvgValue(const double& sum_val, int64_t count) {
        this->val_ += sum_val;
        this->counter_ += count;
        DLOG(INFO) << "Update " << Type_Name(this->type_) << " val " << sum_val << ", sum = " << this->val_
                   << ", count = " << this->counter_;
    }

    void Update(const std::string& bval) override {
        size_t bval_size_expected = sizeof(double) + sizeof(int64_t);
        if (bval.size() != bval_size_expected) {
            LOG(ERROR) << "encoded aggr val is not valid";
            return;
        }

        double val = *reinterpret_cast<const double*>(bval.c_str());
        int64_t count = *reinterpret_cast<const int64_t*>(&bval.at(sizeof(double)));
        DLOG(INFO) << "Update binary value " << "sum = " << val << ", count = " << count;
        UpdateAvgValue(val, count);
    }

    const double& val() override {
        if (this->counter_ != 0) {
            avg_ = this->val_ / this->counter_;
        } else {
            LOG(ERROR) << "Aggregator value is null";
        }
        return avg_;
    }

    type::Type GetRepType() const override {
        return type::kDouble;
    }

    void Reset() override {
        Aggregator::Reset();
        avg_ = 0;
    }

 private:
    double avg_ = 0;
};

template <class T>
class MinAggregator : public Aggregator<T> {
 public:
    MinAggregator(type::Type type, const Schema& output_schema)
        : Aggregator<T>(type, output_schema, std::numeric_limits<T>::max()) {}

    // val is assumed to be not null
    void UpdateValue(const T& val) override {
        UpdateInternal(val);
        this->counter_++;
        DLOG(INFO) << "Update " << Type_Name(this->type_) << " val " << val << ", min = " << this->val_;
    }

 protected:
    template <class TT = T>
    void UpdateInternal(const TT& val, std::enable_if_t<std::is_arithmetic<TT>{}>* = nullptr) {
        this->val_ = std::min(val, this->val_);
    }

    template <class TT = T>
    void UpdateInternal(const TT& val, std::enable_if_t<!std::is_arithmetic<TT>{}>* = nullptr) {
        if (this->IsNull()) {
            this->val_ = val;
        } else {
            if (val.compare(this->val_) < 0) {
                this->val_ = val;
            }
        }
    }
};

template <class T>
class MaxAggregator : public Aggregator<T> {
 public:
    MaxAggregator(type::Type type, const Schema& output_schema)
        : Aggregator<T>(type, output_schema, std::numeric_limits<T>::min()) {}

    // val is assumed to be not null
    void UpdateValue(const T& val) override {
        UpdateInternal(val);
        this->counter_++;
        DLOG(INFO) << "Update " << Type_Name(this->type_) << " val " << val << ", min = " << this->val_;
    }

 protected:
    // val is assumed to be not null
    template <class TT = T>
    void UpdateInternal(const TT& val, std::enable_if_t<std::is_arithmetic<TT>{}>* = nullptr) {
        this->val_ = std::max(val, this->val_);
    }

    template <class TT = T>
    void UpdateInternal(const TT& val, std::enable_if_t<!std::is_arithmetic<TT>{}>* = nullptr) {
        if (this->IsNull()) {
            this->val_ = val;
        } else {
            if (val.compare(this->val_) > 0) {
                this->val_ = val;
            }
        }
    }
};

template <template<class> class AggregatorClass>
std::unique_ptr<BaseAggregator> MakeOverflowAggregator(type::Type agg_col_type, const Schema& output_schema) {
    switch (agg_col_type) {
        case type::kInt16:
        case type::kInt32:
        case type::kTimestamp:
        case type::kInt64: {
            return std::make_unique<AggregatorClass<int64_t>>(agg_col_type, output_schema);
        }
        case type::kFloat: {
            return std::make_unique<AggregatorClass<float>>(agg_col_type, output_schema);
            break;
        }
        case type::kDouble: {
            return std::make_unique<AggregatorClass<double>>(agg_col_type, output_schema);
            break;
        }
        default:
            LOG(ERROR) << "Not support for type " << Type_Name(agg_col_type);
            return nullptr;
    }
}

template <template<class> class AggregatorClass>
std::unique_ptr<BaseAggregator> MakeSameTypeAggregator(type::Type agg_col_type, const Schema& output_schema) {
    switch (agg_col_type) {
        case type::kInt16:
            return std::make_unique<AggregatorClass<int16_t>>(agg_col_type, output_schema);
        case type::kInt32:
        case type::kDate:
            return std::make_unique<AggregatorClass<int32_t>>(agg_col_type, output_schema);
        case type::kTimestamp:
        case type::kInt64: {
            return std::make_unique<AggregatorClass<int64_t>>(agg_col_type, output_schema);
        }
        case type::kFloat: {
            return std::make_unique<AggregatorClass<float>>(agg_col_type, output_schema);
        }
        case type::kDouble: {
            return std::make_unique<AggregatorClass<double>>(agg_col_type, output_schema);
        }
        case type::kVarchar: {
            return std::make_unique<AggregatorClass<std::string>>(agg_col_type, output_schema);
        }
        default:
            LOG(ERROR) << "Not support for type " << Type_Name(agg_col_type);
            return nullptr;
    }
}

template <class T>
std::enable_if_t<std::is_arithmetic<T>{}> AggregatorUpdate(BaseAggregator* aggregator, const T& val) {
    switch (aggregator->GetRepType()) {
        case type::kInt16:
            dynamic_cast<Aggregator<int16_t>*>(aggregator)->UpdateValue(val);
            break;
        case type::kDate:
        case type::kInt32:
            dynamic_cast<Aggregator<int32_t>*>(aggregator)->UpdateValue(val);
            break;
        case type::kTimestamp:
        case type::kInt64:
            dynamic_cast<Aggregator<int64_t>*>(aggregator)->UpdateValue(val);
            break;
        case type::kFloat:
            dynamic_cast<Aggregator<float>*>(aggregator)->UpdateValue(val);
            break;
        case type::kDouble:
            dynamic_cast<Aggregator<double>*>(aggregator)->UpdateValue(val);
            break;
        default:
            LOG(ERROR) << "ERROR: unsupport type " << Type_Name(aggregator->GetRepType());
            break;
    }
}

template <class T>
std::enable_if_t<!std::is_arithmetic<T>{}> AggregatorUpdate(BaseAggregator* aggregator, const T& val) {
    switch (aggregator->GetRepType()) {
        case type::kVarchar:
            dynamic_cast<Aggregator<std::string>*>(aggregator)->UpdateValue(val);
            break;
        default:
            LOG(ERROR) << "ERROR: unsupport type " << Type_Name(aggregator->GetRepType());
            break;
    }
}

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_AGGREGATOR_H_
