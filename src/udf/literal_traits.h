/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * literal_traits.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_LITERAL_TRAITS_H_
#define SRC_UDF_LITERAL_TRAITS_H_

#include <limits>
#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/fe_status.h"
#include "node/node_manager.h"
#include "node/sql_node.h"

namespace fesql {
namespace udf {

using fesql::base::Status;
using fesql::codec::StringRef;
using fesql::node::ExprListNode;
using fesql::node::ExprNode;
using fesql::node::SQLNode;

template <typename T>
struct DataTypeTrait {
    static std::string to_string();
    static node::TypeNode* to_type_node(node::NodeManager* nm);
    static node::ExprNode* to_const(node::NodeManager* nm, const T&);
    static const T minimum_value();
    static const T maximum_value();
};

struct AnyArg {
    AnyArg() = delete;
};

template <typename T>
struct Opaque {
    Opaque() = delete;
};

template <>
struct DataTypeTrait<AnyArg> {
    static std::string to_string() { return "?"; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nullptr;
    }
};

template <>
struct DataTypeTrait<bool> {
    static std::string to_string() { return "bool"; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kBool);
    }
};

template <>
struct DataTypeTrait<int16_t> {
    static std::string to_string() { return "int16"; }
    static node::DataType to_type_enum() { return node::kInt16; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kInt16);
    }
    static int32_t codec_type_enum() { return fesql::type::kInt16; }
    static node::ExprNode* to_const(node::NodeManager* nm, const int16_t& v) {
        return nm->MakeConstNode(v);
    }
    static const int16_t minimum_value() {
        return std::numeric_limits<int16_t>::min();
    }
    static const int16_t maximum_value() {
        return std::numeric_limits<int16_t>::max();
    }
};

template <>
struct DataTypeTrait<int32_t> {
    static std::string to_string() { return "int32"; }
    static node::DataType to_type_enum() { return node::kInt32; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kInt32);
    }
    static int32_t codec_type_enum() { return fesql::type::kInt32; }
    static node::ExprNode* to_const(node::NodeManager* nm, const int32_t& v) {
        return nm->MakeConstNode(v);
    }
    static const int32_t minimum_value() {
        return std::numeric_limits<int32_t>::min();
    }
    static const int32_t maximum_value() {
        return std::numeric_limits<int32_t>::max();
    }
};

template <>
struct DataTypeTrait<int64_t> {
    static std::string to_string() { return "int64"; }
    static node::DataType to_type_enum() { return node::kInt64; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kInt64);
    }
    static int32_t codec_type_enum() { return fesql::type::kInt64; }
    static node::ExprNode* to_const(node::NodeManager* nm, const int64_t& v) {
        return nm->MakeConstNode(v);
    }
    static const int64_t minimum_value() {
        return std::numeric_limits<int64_t>::min();
    }
    static const int64_t maximum_value() {
        return std::numeric_limits<int64_t>::max();
    }
};

template <>
struct DataTypeTrait<float> {
    static std::string to_string() { return "float"; }
    static node::DataType to_type_enum() { return node::kFloat; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kFloat);
    }
    static int32_t codec_type_enum() { return fesql::type::kFloat; }
    static node::ExprNode* to_const(node::NodeManager* nm, const float& v) {
        return nm->MakeConstNode(v);
    }
    static const float minimum_value() {
        return std::numeric_limits<float>::min();
    }
    static const float maximum_value() {
        return std::numeric_limits<float>::max();
    }
};

template <>
struct DataTypeTrait<double> {
    static std::string to_string() { return "double"; }
    static node::DataType to_type_enum() { return node::kDouble; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kDouble);
    }
    static int32_t codec_type_enum() { return fesql::type::kDouble; }
    static node::ExprNode* to_const(node::NodeManager* nm, const double& v) {
        return nm->MakeConstNode(v);
    }
    static const double minimum_value() {
        return std::numeric_limits<double>::min();
    }
    static const double maximum_value() {
        return std::numeric_limits<double>::max();
    }
};

template <>
struct DataTypeTrait<codec::Timestamp> {
    static std::string to_string() { return "timestamp"; }
    static node::DataType to_type_enum() { return node::kTimestamp; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kTimestamp);
    }
    static node::ExprNode* to_const(node::NodeManager* nm,
                                    const codec::Timestamp& v) {
        return nm->MakeConstNode(v.ts_, node::kTimestamp);
    }
    static int32_t codec_type_enum() { return fesql::type::kTimestamp; }
    static const codec::Timestamp minimum_value() {
        return codec::Timestamp(0);
    }
    static const codec::Timestamp maximum_value() {
        return codec::Timestamp(std::numeric_limits<int64_t>::max());
    }
};

template <>
struct DataTypeTrait<codec::Date> {
    static std::string to_string() { return "date"; }
    static node::DataType to_type_enum() { return node::kDate; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kDate);
    }
    static int32_t codec_type_enum() { return fesql::type::kDate; }
    static node::ExprNode* to_const(node::NodeManager* nm,
                                    const codec::Date& v) {
        return nm->MakeConstNode(v.date_, node::kDate);
    }
    static const codec::Date minimum_value() { return codec::Date(0); }
    static const codec::Date maximum_value() {
        return codec::Date(std::numeric_limits<int32_t>::max());
    }
};

template <>
struct DataTypeTrait<codec::StringRef> {
    static std::string to_string() { return "string"; }
    static node::DataType to_type_enum() { return node::kVarchar; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kVarchar);
    }
    static int32_t codec_type_enum() { return fesql::type::kVarchar; }
};

template <typename T>
struct DataTypeTrait<codec::ListRef<T>> {
    static std::string to_string() {
        return "list_" + DataTypeTrait<T>::to_string();
    }
    static node::DataType to_type_enum() { return node::kList; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeTypeNode(node::kList,
                                DataTypeTrait<T>::to_type_node(nm));
    }
};

template <typename T>
struct DataTypeTrait<Opaque<T>> {
    static std::string to_string() {
        return "opaque<" + std::to_string(sizeof(T)) + ">";
    }
    static node::DataType to_type_enum() { return node::kOpaque; }
    static node::TypeNode* to_type_node(node::NodeManager* nm) {
        return nm->MakeOpaqueType(sizeof(T));
    }
};

template <typename T>
struct CCallDataTypeTrait {
    using LiteralTag = T;
};
template <typename V>
struct CCallDataTypeTrait<V*> {
    using LiteralTag = Opaque<V>;
};
template <>
struct CCallDataTypeTrait<codec::Timestamp*> {
    using LiteralTag = codec::Timestamp;
};
template <>
struct CCallDataTypeTrait<codec::Date*> {
    using LiteralTag = codec::Date;
};
template <>
struct CCallDataTypeTrait<codec::StringRef*> {
    using LiteralTag = codec::StringRef;
};
template <typename V>
struct CCallDataTypeTrait<codec::ListRef<V>*> {
    using LiteralTag = codec::ListRef<V>;
};

template <typename... LiteralArgTypes>
const std::string LiteralToArgTypesSignature() {
    std::stringstream ss;
    size_t idx = 0;
    for (auto type_str : {DataTypeTrait<LiteralArgTypes>::to_string()...}) {
        ss << type_str;
        if (idx < sizeof...(LiteralArgTypes) - 1) {
            ss << ", ";
        }
        idx += 1;
    }
    return ss.str();
}

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_LITERAL_TRAITS_H_
