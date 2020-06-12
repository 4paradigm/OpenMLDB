/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * literal_case.h
 *
 * Author: chenjing
 * Date: 2020/4/23
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CASE_LITERAL_CASE_H_
#define SRC_CASE_LITERAL_CASE_H_
#include <string>
#include "proto/fe_type.pb.h"
#include "codec/fe_row_codec.h"
#include "codec/row.h"

namespace fesql {
namespace literal_case {

template <typename T>
class LiteralWrapper {
 public:
    explicit LiteralWrapper(T* ptr):
        ptr_(ptr), managed_(false) {}

    explicit LiteralWrapper(std::nullptr_t n):
        ptr_(nullptr), managed_(false) {}

    explicit LiteralWrapper(const T& value):
        ptr_(new T(value)), managed_(true) {}

    ~LiteralWrapper() {
        if (ptr_ != nullptr && managed_) {
            delete ptr_;
        }
    }

    T* get() const { return ptr_; }

    bool is_null() const { return ptr_ == nullptr; }

 private:
    T* ptr_;
    bool managed_;
};


template <typename T>
struct LiteralTypeTrait {
    static fesql::type::Type get();
    static void encode(codec::RowBuilder* builder, const T& v);
};

template <typename T>
struct LiteralNull {};

template <typename T> struct LiteralTypeTrait<LiteralNull<T>> {
    static fesql::type::Type get() { return LiteralTypeTrait<T>::get(); }
    static size_t size(const LiteralNull<T>& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const LiteralNull<T>& v) {
        builder->AppendNULL();
    }
};
template <> struct LiteralTypeTrait<bool> {
    static fesql::type::Type get() { return fesql::type::kBool; }
    static size_t size(const bool& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const bool& v) {
        builder->AppendBool(v);
    }
};
template <> struct LiteralTypeTrait<int16_t> {
    static fesql::type::Type get() { return fesql::type::kInt16; }
    static size_t size(const int16_t& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const int16_t& v) {
        builder->AppendInt16(v);
    }
};
template <> struct LiteralTypeTrait<int32_t> {
    static fesql::type::Type get() { return fesql::type::kInt32; }
    static size_t size(const int32_t& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const int32_t& v) {
        builder->AppendInt32(v);
    }
};
template <> struct LiteralTypeTrait<int64_t> {
    static fesql::type::Type get() { return fesql::type::kInt64; }
    static size_t size(const int64_t& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const int64_t& v) {
        builder->AppendInt64(v);
    }
};
template <> struct LiteralTypeTrait<float> {
    static fesql::type::Type get() { return fesql::type::kFloat; }
    static size_t size(const float& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const float& v) {
        builder->AppendFloat(v);
    }
};
template <> struct LiteralTypeTrait<double> {
    static fesql::type::Type get() { return fesql::type::kDouble; }
    static size_t size(const double& str) { return 0; }
    static void encode(codec::RowBuilder* builder, const double& v) {
        builder->AppendDouble(v);
    }
};
template <> struct LiteralTypeTrait<std::string> {
    static fesql::type::Type get() { return fesql::type::kVarchar; }
    static size_t size(const std::string& str) { return str.size(); }
    static void encode(codec::RowBuilder* builder, const std::string& v) {
        builder->AppendString(v.c_str(), v.length());
    }
};
template <> struct LiteralTypeTrait<const char*> {
    static fesql::type::Type get() { return fesql::type::kVarchar; }
    static size_t size(const char *const& str) { return strlen(str); }
    static void encode(codec::RowBuilder* builder, const char *const& v) {
        builder->AppendString(v, strlen(v));
    }
};


template <typename T>
static void __UpdateSchema(const T& arg,
                           codec::Schema* schema,
                           size_t* string_size) {
    fesql::type::Type ty = LiteralTypeTrait<T>::get();
    if (ty == fesql::type::kVarchar) {
        *string_size += LiteralTypeTrait<T>::size(arg);
    }
    type::ColumnDef* def = schema->Add();
    def->set_type(ty);
    def->set_name(std::string("col_") + std::to_string(schema->size()));
}

template <typename ...Args>
codec::Row CreateLiteralRow(Args... args) {
    size_t string_size = 0;
    codec::Schema schema;

    int dummy[sizeof...(args)] = {
        (__UpdateSchema(args, &schema, &string_size), 0)...};

    codec::RowBuilder builder(schema);
    size_t buf_size = builder.CalTotalLength(string_size);
    auto buf = reinterpret_cast<int8_t*>(malloc(buf_size));
    auto slice = base::RefCountedSlice::CreateManaged(buf, buf_size);
    builder.SetBuffer(buf, buf_size);

    int dummy2[sizeof...(args)] = {
        (LiteralTypeTrait<Args>::encode(&builder, args), 0)...};

    return codec::Row(slice);
}






}  // namespace literal_case
}  // namespace fesql
#endif  // SRC_CASE_LITERAL_CASE_H_
