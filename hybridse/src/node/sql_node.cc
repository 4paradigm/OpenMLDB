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

#include "node/sql_node.h"

#include <algorithm>
#include <numeric>
#include <string>
#include <unordered_map>
#include <utility>

#include "absl/algorithm/container.h"
#include "absl/container/flat_hash_map.h"
#include "absl/strings/str_cat.h"
#include "absl/strings/str_join.h"
#include "absl/strings/string_view.h"
#include "absl/strings/substitute.h"
#include "boost/algorithm/string/case_conv.hpp"
#include "glog/logging.h"
#include "node/node_enum.h"
#include "node/node_manager.h"
#include "udf/udf_library.h"

namespace hybridse {
namespace node {

using base::Status;
using common::kTypeError;

static const std::unordered_map<std::string, DataType const> type_map = {
    {"bool", kBool},   {"in1", kBool},      {"i16", kInt16},      {"int16", kInt16},         {"smallint", kInt16},
    {"i32", kInt32},   {"int32", kInt32},   {"int", kInt32},      {"integer", kInt32},       {"i64", kInt64},
    {"int64", kInt64}, {"bigint", kInt64},  {"string", kVarchar}, {"varchar", kVarchar},     {"float32", kFloat},
    {"float", kFloat}, {"double", kDouble}, {"float64", kDouble}, {"timestamp", kTimestamp}, {"date", kDate},
};

static absl::flat_hash_map<CmdType, absl::string_view> CreateCmdTypeNamesMap() {
    absl::flat_hash_map<CmdType, absl::string_view> map = {
        {CmdType::kCmdShowDatabases, "show databases"},
        {CmdType::kCmdShowTables, "show tables"},
        {CmdType::kCmdUseDatabase, "use database"},
        {CmdType::kCmdDropDatabase, "drop database"},
        {CmdType::kCmdCreateDatabase, "create database"},
        {CmdType::kCmdDescTable, "desc table"},
        {CmdType::kCmdDropTable, "drop table"},
        {CmdType::kCmdShowProcedures, "show procedures"},
        {CmdType::kCmdShowCreateSp, "show create procedure"},
        {CmdType::kCmdShowCreateTable, "show create table"},
        {CmdType::kCmdDropSp, "drop procedure"},
        {CmdType::kCmdDropIndex, "drop index"},
        {CmdType::kCmdExit, "exit"},
        {CmdType::kCmdCreateIndex, "create index"},
        {CmdType::kCmdShowDeployment, "show deployment"},
        {CmdType::kCmdShowDeployments, "show deployments"},
        {CmdType::kCmdDropDeployment, "drop deployment"},
        {CmdType::kCmdShowJob, "show job"},
        {CmdType::kCmdShowJobs, "show jobs"},
        {CmdType::kCmdStopJob, "stop job"},
        {CmdType::kCmdShowGlobalVariables, "show global variables"},
        {CmdType::kCmdShowSessionVariables, "show session variables"},
        {CmdType::kCmdShowComponents, "show components"},
        {CmdType::kCmdShowTableStatus, "show table status"},
        {CmdType::kCmdDropFunction, "drop function"},
        {CmdType::kCmdShowFunctions, "show functions"},
        {CmdType::kCmdShowJobLog, "show joblog"},
        {CmdType::kCmdTruncate, "truncate table"},
    };
    for (auto kind = 0; kind < CmdType::kLastCmd; ++kind) {
        DCHECK(map.find(static_cast<CmdType>(kind)) != map.end());
    }
    return map;
}

static const absl::flat_hash_map<CmdType, absl::string_view>& GetCmdTypeNamesMap() {
  static const absl::flat_hash_map<CmdType, std::string_view>& map = *new auto(CreateCmdTypeNamesMap());
  return map;
}

static absl::flat_hash_map<DataType, absl::string_view> CreateDataTypeNamesMap() {
  absl::flat_hash_map<DataType, absl::string_view> map = {
      {kBool, "bool"},     {kInt16, "int16"},   {kInt32, "int32"},    {kInt64, "int64"},
      {kFloat, "float"},   {kDouble, "double"}, {kVarchar, "string"}, {kTimestamp, "timestamp"},
      {kDate, "date"},     {kList, "list"},     {kMap, "map"},        {kIterator, "iterator"},
      {kRow, "row"},       {kSecond, "second"}, {kMinute, "minute"},  {kHour, "hour"},
      {kNull, "null"},     {kArray, "array"},   {kVoid, "void"},      {kPlaceholder, "placeholder"},
      {kOpaque, "opaque"}, {kTuple, "tuple"},   {kDay, "day"},        {kInt8Ptr, "int8ptr"},
  };

  for (auto kind = 0; kind < DataType::kLastDataType; ++kind) {
        DCHECK(map.find(static_cast<DataType>(kind)) != map.end());
  }
  DCHECK(map.find(kVoid) != map.end());
  DCHECK(map.find(kNull) != map.end());
  DCHECK(map.find(kPlaceholder) != map.end());

  return map;
}

static const absl::flat_hash_map<DataType, absl::string_view>& GetDataTypeNamesMap() {
  static const absl::flat_hash_map<DataType, std::string_view> &map = *new auto(CreateDataTypeNamesMap());
  return map;
}

static absl::flat_hash_map<ExprType, absl::string_view> CreateExprTypeNamesMap() {
  absl::flat_hash_map<ExprType, absl::string_view> map = {
      {kExprPrimary, "primary"},
      {kExprParameter, "parameter"},
      {kExprId, "id"},
      {kExprBinary, "binary"},
      {kExprUnary, "unary"},
      {kExprCall, "function"},
      {kExprCase, "case"},
      {kExprWhen, "when"},
      {kExprBetween, "between"},
      {kExprColumnRef, "column ref"},
      {kExprColumnId, "column id"},
      {kExprCast, "cast"},
      {kExprAll, "all"},
      {kExprStruct, "struct"},
      {kExprQuery, "query"},
      {kExprOrder, "order"},
      {kExprGetField, "get field"},
      {kExprCond, "cond"},
      {kExprUnknow, "unknow"},
      {kExprIn, "in"},
      {kExprList, "expr_list"},
      {kExprForIn, "for_in"},
      {kExprRange, "range"},
      {kExprOrderExpression, "order"},
      {kExprEscaped, "escape"},
      {kExprArray, "array"},
  };
  for (auto kind = 0; kind < ExprType::kExprLast; ++kind) {
        DCHECK(map.find(static_cast<ExprType>(kind)) != map.end());
  }
  return map;
}

static const absl::flat_hash_map<ExprType, absl::string_view>& GetExprTypeNamesMap() {
  static const absl::flat_hash_map<ExprType, std::string_view> &map = *new auto(CreateExprTypeNamesMap());
  return map;
}

template <typename NodeType, typename Allocator>
void PrintNodeList(const std::vector<NodeType *, Allocator> &node_list, std::ostream &output,
                   const std::string &org_tab) {
  for (auto iter = node_list.cbegin(); iter != node_list.cend(); iter++) {
        (*iter)->Print(output, org_tab);
        if (iter + 1 != node_list.end()) {
            output << "\n";
        }
  }
}

bool SqlEquals(const SqlNode *left, const SqlNode *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}

bool SqlListEquals(const SqlNodeList *left, const SqlNodeList *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}
bool ExprEquals(const ExprNode *left, const ExprNode *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}
bool FnDefEquals(const FnDefNode *left, const FnDefNode *right) {
    return left == right ? true : nullptr == left ? false : left->Equals(right);
}
bool TypeEquals(const TypeNode *left, const TypeNode *right) {
    if (left == nullptr || right == nullptr) {
        return false;  // ? != ?
    }
    return left == right || left->Equals(right);
}
void PrintSqlNode(std::ostream &output, const std::string &org_tab, const SqlNode *node_ptr,
                  const std::string &item_name, bool last_child) {
    output << org_tab << SPACE_ST << item_name << ":";

    if (nullptr == node_ptr) {
        output << " null";
    } else if (last_child) {
        output << "\n";
        node_ptr->Print(output, org_tab + INDENT);
    } else {
        output << "\n";
        node_ptr->Print(output, org_tab + OR_INDENT);
    }
}
void PrintSqlVector(std::ostream &output, const std::string &tab, const std::vector<FnNode *> &vec,
                    const std::string &vector_name, bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]:\n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintSqlNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSqlNode(output, space, vec[i], "" + std::to_string(i), true);
}
void PrintSqlVector(std::ostream &output, const std::string &tab, const std::vector<ExprNode *> &vec,
                    const std::string &vector_name, bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]:\n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintSqlNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSqlNode(output, space, vec[i], "" + std::to_string(i), true);
}

void PrintSqlVector(std::ostream &output, const std::string &tab,
                    const std::vector<std::pair<std::string, DataType>> &vec, const std::string &vector_name,
                    bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]:\n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintValue(output, space, DataTypeName(vec[i].second), "" + vec[i].first, false);
        output << "\n";
    }
    PrintValue(output, space, DataTypeName(vec[i].second), "" + vec[i].first, true);
}

void PrintSqlVector(std::ostream &output, const std::string &tab, const NodePointVector &vec,
                    const std::string &vector_name, bool last_item) {
    if (0 == vec.size()) {
        output << tab << SPACE_ST << vector_name << ": []";
        return;
    }
    output << tab << SPACE_ST << vector_name << "[list]:\n";
    const std::string space = last_item ? (tab + INDENT) : tab + OR_INDENT;
    int count = vec.size();
    int i = 0;
    for (i = 0; i < count - 1; ++i) {
        PrintSqlNode(output, space, vec[i], "" + std::to_string(i), false);
        output << "\n";
    }
    PrintSqlNode(output, space, vec[i], "" + std::to_string(i), true);
}

void SelectQueryNode::PrintSqlNodeList(std::ostream &output, const std::string &tab, SqlNodeList *list,
                                       const std::string &name, bool last_item) const {
    if (nullptr == list) {
        output << tab << SPACE_ST << name << ": []";
        return;
    }
    PrintSqlVector(output, tab, list->GetList(), name, last_item);
}

void PrintValue(std::ostream &output, const std::string &org_tab, const std::string &value,
                const std::string &item_name, bool last_child) {
    output << org_tab << SPACE_ST << item_name << ": " << (value.empty() ? "<nil>" : value);
}

void PrintValue(std::ostream &output, const std::string &org_tab, const std::vector<std::string> &vec,
                const std::string &item_name, bool last_child) {
    std::stringstream ss;
    ss << "[";
    for (size_t i = 0; i < vec.size(); ++i) {
        ss << vec[i];
        if (i + 1 < vec.size()) {
            ss << ", ";
        }
    }
    ss << "]";
    output << org_tab << SPACE_ST << item_name << ": " << ss.str();
}

void PrintValue(std::ostream &output, const std::string &org_tab, const OptionsMap* value,
                const std::string &item_name, bool last_child) {
    output << org_tab << SPACE_ST << item_name << ":";
    if (value == nullptr || value->empty()) {
        output << " <nil>";
        return;
    }
    auto new_tab = org_tab;
    if (last_child) {
        absl::StrAppend(&new_tab, INDENT);
    } else {
        absl::StrAppend(&new_tab, OR_INDENT);
    }
    for (auto it = value->begin(); it != value->end(); ++it) {
        output << "\n";
        PrintSqlNode(output, new_tab, it->second, it->first, std::next(it) == value->end());
    }
}

Status ValidateArgs(const std::string& function_name, const std::vector<const TypeNode *> &actual_types,
        const std::vector<const node::TypeNode *>& arg_types, int variadic_pos) {
    size_t actual_arg_num = actual_types.size();
    CHECK_TRUE(actual_arg_num >= arg_types.size(), kTypeError, function_name, " take at least ", arg_types.size(),
               " arguments, but get ", actual_arg_num);
    if (arg_types.size() < actual_arg_num) {
        CHECK_TRUE(variadic_pos >= 0 && static_cast<size_t>(variadic_pos) == arg_types.size(), kTypeError,
                   function_name, " take explicit ", arg_types.size(), " arguments, but get ", actual_arg_num);
    }
    for (size_t i = 0; i < arg_types.size(); ++i) {
        auto actual_ty = actual_types[i];
        if (actual_ty == nullptr) {
            continue;
        }
        CHECK_TRUE(arg_types[i] != nullptr, kTypeError, i, "th argument is not inferred");
        CHECK_TRUE(arg_types[i]->Equals(actual_ty), kTypeError, function_name, "'s ", i,
                   "th actual argument mismatch: get ", actual_ty->GetName(), " but expect ", arg_types[i]->GetName());
    }
    return Status::OK();
}

bool SqlNode::Equals(const SqlNode *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || type_ != that->type_) {
        return false;
    }
    return true;
}

void SqlNodeList::Print(std::ostream &output, const std::string &tab) const {
    PrintSqlVector(output, tab, list_, "list", true);
}
bool SqlNodeList::Equals(const SqlNodeList *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || this->list_.size() != that->list_.size()) {
        return false;
    }

    auto iter1 = list_.cbegin();
    auto iter2 = that->list_.cbegin();
    while (iter1 != list_.cend()) {
        if (!(*iter1)->Equals(*iter2)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

void QueryNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    output << ": " << QueryTypeName(query_type_);

    std::string new_tab = org_tab + INDENT;
    if (!with_clauses_.empty()) {
        output << "\n" << new_tab << SPACE_ST << "with_clause[list]:";
        for (size_t i = 0; i <  with_clauses_.size(); i++) {
            auto node = with_clauses_[i];
            output << "\n";
            PrintSqlNode(output, new_tab + INDENT, node, std::to_string(i), false);
        }
    }

    if (config_options_.get() != nullptr) {
        output << "\n";
        PrintValue(output, org_tab + INDENT + SPACE_ED, config_options_.get(), "config_options", false);
    }
}
bool QueryNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }

    const QueryNode *that = dynamic_cast<const QueryNode *>(node);
    return that != nullptr && this->query_type_ == that->query_type_ &&
           absl::c_equal(with_clauses_, that->with_clauses_,
                         [](WithClauseEntry *lhs, WithClauseEntry *rhs) { return SqlEquals(lhs, rhs); });
}

const std::string AllNode::GetExprString() const {
    std::string str = "";
    if (!db_name_.empty()) {
        str.append(db_name_).append(".");
    }
    if (!relation_name_.empty()) {
        str.append(relation_name_).append(".");
    }
    str.append("*");
    return str;
}
bool AllNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const AllNode *that = dynamic_cast<const AllNode *>(node);
    return this->relation_name_ == that->relation_name_ && ExprNode::Equals(node);
}
void ParameterExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";
    auto tab = org_tab + INDENT;
    PrintValue(output, tab, std::to_string(position()), "position", false);
}
bool ParameterExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const auto *that = dynamic_cast<const ParameterExpr *>(node);
    return this->position_ == that->position_;
}
const std::string ParameterExpr::GetExprString() const { return "?" + std::to_string(position_); }
void ConstNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";
    auto tab = org_tab + INDENT;
    PrintValue(output, tab, GetExprString(), "value", false);
    output << "\n";
    PrintValue(output, tab, DataTypeName(data_type_), "type", true);
}

const std::string ConstNode::GetExprString() const {
    switch (data_type_) {
        case hybridse::node::kBool:
            return GetBool() ? "true" : "false";
        case hybridse::node::kInt16:
            return std::to_string(val_.vsmallint);
        case hybridse::node::kInt32:
            return std::to_string(val_.vint);
        case hybridse::node::kInt64:
            return std::to_string(val_.vlong);
        case hybridse::node::kFloat:
            return std::to_string(val_.vfloat);
        case hybridse::node::kDouble:
            return std::to_string(val_.vdouble);
            return std::to_string(val_.vlong);
        case hybridse::node::kVarchar:
            return val_.vstr;
        case hybridse::node::kDate:
            return "Date(" + std::to_string(val_.vlong) + ")";
        case hybridse::node::kTimestamp:
            return "Timestamp(" + std::to_string(val_.vlong) + ")";
        case hybridse::node::kHour:
            return std::to_string(val_.vlong).append("h");
        case hybridse::node::kMinute:
            return std::to_string(val_.vlong).append("m");
        case hybridse::node::kSecond:
            return std::to_string(val_.vlong).append("s");
        case hybridse::node::kDay:
            return std::to_string(val_.vlong).append("d");
        case hybridse::node::kVoid:
            return "void";
        case hybridse::node::kNull:
            return "null";
        case hybridse::node::kPlaceholder:
            return "?";
        default:
            return "unknown";
    }
}
bool ConstNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const auto *that = dynamic_cast<const ConstNode *>(node);
    return this->data_type_ == that->data_type_ && GetExprString() == that->GetExprString() && ExprNode::Equals(node);
}

ConstNode *ConstNode::CastFrom(ExprNode *node) { return dynamic_cast<ConstNode *>(node); }

void LimitNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(limit_cnt_), "limit_cnt", true);
}
bool LimitNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const LimitNode *that = dynamic_cast<const LimitNode *>(node);
    return this->limit_cnt_ == that->limit_cnt_;
}

void TableNode::Print(std::ostream &output, const std::string &org_tab) const {
    TableRefNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, org_table_name_, "table", false);
    output << "\n";
    PrintValue(output, tab, alias_table_name_, "alias", true);
}
bool TableNode::Equals(const SqlNode *node) const {
    if (!TableRefNode::Equals(node)) {
        return false;
    }

    const TableNode *that = dynamic_cast<const TableNode *>(node);
    return this->org_table_name_ == that->org_table_name_;
}

void ColumnIdNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(this->GetColumnID()), "column_id", false);
}

ColumnIdNode *ColumnIdNode::CastFrom(ExprNode *node) { return dynamic_cast<ColumnIdNode *>(node); }

const std::string ColumnIdNode::GenerateExpressionName() const { return "#" + std::to_string(this->GetColumnID()); }

const std::string ColumnIdNode::GetExprString() const { return "#" + std::to_string(this->GetColumnID()); }

bool ColumnIdNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const ColumnIdNode *that = dynamic_cast<const ColumnIdNode *>(node);
    return this->GetColumnID() == that->GetColumnID();
}

void ColumnRefNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, db_name_.empty() ? relation_name_ : db_name_ + "." + relation_name_, "relation_name",
               false);
    output << "\n";
    PrintValue(output, tab, column_name_, "column_name", true);
}

ColumnRefNode *ColumnRefNode::CastFrom(ExprNode *node) { return dynamic_cast<ColumnRefNode *>(node); }

const std::string ColumnRefNode::GenerateExpressionName() const {
    std::string str = "";
    str.append(column_name_);
    return str;
}
const std::string ColumnRefNode::GetExprString() const {
    std::string path = "";
    if (!db_name_.empty()) {
        path.append(db_name_).append(".");
    }
    if (!relation_name_.empty()) {
        path.append(relation_name_).append(".");
    }
    path.append(column_name_);
    return path;
}
bool ColumnRefNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const ColumnRefNode *that = dynamic_cast<const ColumnRefNode *>(node);
    return this->relation_name_ == that->relation_name_ && this->column_name_ == that->column_name_ &&
           ExprNode::Equals(node);
}

void GetFieldExpr::Print(std::ostream &output, const std::string &org_tab) const {
    auto input = GetChild(0);
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, input, "input", true);
    output << "\n";
    if (input->GetOutputType() != nullptr && input->GetOutputType()->base() == kTuple) {
        PrintValue(output, tab, std::to_string(column_id_), "field_index", true);
    } else {
        PrintValue(output, tab, std::to_string(column_id_), "column_id", true);
        output << "\n";
        PrintValue(output, tab, column_name_, "column_name", true);
    }
}

const std::string GetFieldExpr::GenerateExpressionName() const {
    std::string str = GetChild(0)->GenerateExpressionName();
    str.append(".");
    str.append(column_name_);
    return str;
}
const std::string GetFieldExpr::GetExprString() const {
    std::string str = "";
    str.append("#");
    str.append(std::to_string(column_id_));
    str.append(":");
    str.append(column_name_);
    return str;
}
bool GetFieldExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    auto that = dynamic_cast<const GetFieldExpr *>(node);
    return this->GetRow()->Equals(that->GetRow()) && this->column_id_ == that->column_id_ &&
           this->column_name_ == that->column_name_ && ExprNode::Equals(node);
}
void OrderExpression::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, GetExprString(), "order_expression", false);
}
const std::string OrderExpression::GetExprString() const {
    if (nullptr == expr_) {
        return is_asc() ? "ASC" : "DESC";
    }
    std::string str = "";
    str.append(expr()->GetExprString());
    str.append(is_asc() ? " ASC" : " DESC");
    return str;
}
bool OrderExpression::Equals(const ExprNode *node) const {
    if (!ExprNode::Equals(node)) {
        return false;
    }
    const OrderExpression *that = dynamic_cast<const OrderExpression *>(node);
    return is_asc_ == that->is_asc_ && ExprEquals(expr_, that->expr_);
}

void OrderByNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, GetExprString(), "order_expressions", false);
}
const std::string OrderByNode::GetExprString() const {
    if (nullptr == order_expressions_) {
        return "()";
    }
    std::string str = "";
    str.append("(");
    for (size_t i = 0; i < order_expressions_->children_.size(); ++i) {
        str.append(order_expressions_->children_[i]->GetExprString());
        if (i < order_expressions_->children_.size() - 1) {
            str.append(", ");
        }
    }
    str.append(")");
    return str;
}
bool OrderByNode::Equals(const ExprNode *node) const {
    if (!ExprNode::Equals(node)) {
        return false;
    }
    const OrderByNode *that = dynamic_cast<const OrderByNode *>(node);
    return ExprEquals(order_expressions_, that->order_expressions_);
}

void FrameNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, FrameTypeName(frame_type_), "frame_type", false);
    if (nullptr != frame_range_) {
        output << "\n";
        PrintSqlNode(output, tab, frame_range_, "frame_range", false);
    }
    if (nullptr != frame_rows_) {
        output << "\n";
        PrintSqlNode(output, tab, frame_rows_, "frame_rows", false);
    }
    if (0 != frame_maxsize_) {
        output << "\n";
        PrintValue(output, tab, std::to_string(frame_maxsize_), "frame_maxsize", false);
    }
}
void FrameExtent::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    if (NULL == start_) {
        PrintValue(output, tab, "UNBOUNDED", "start", false);
    } else {
        PrintSqlNode(output, tab, start_, "start", false);
    }
    output << "\n";
    if (NULL == end_) {
        PrintValue(output, tab, "UNBOUNDED", "end", true);
    } else {
        PrintSqlNode(output, tab, end_, "end", true);
    }
}
bool FrameExtent::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const FrameExtent *that = dynamic_cast<const FrameExtent *>(node);
    return SqlEquals(this->start_, that->start_) && SqlEquals(this->end_, that->end_);
}

std::string FrameExtent::GetExprString() const {
    std::string str = "[";
    if (nullptr == start_) {
        str.append("UNBOUND");
    } else {
        str.append(start_->GetExprString());
    }
    str.append(",");
    if (nullptr == end_) {
        str.append("UNBOUND");
    } else {
        str.append(end_->GetExprString());
    }

    str.append("]");
    return str;
}

bool FrameExtent::Valid() const {
    return GetStartOffset() <= GetEndOffset();
}

FrameExtent* FrameExtent::ShadowCopy(NodeManager* nm) const {
    return nm->MakeFrameExtent(start(), end());
}

bool FrameNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const FrameNode *that = dynamic_cast<const FrameNode *>(node);
    return this->frame_type_ == that->frame_type_ && SqlEquals(this->frame_range_, that->frame_range_) &&
           SqlEquals(this->frame_rows_, that->frame_rows_) && (this->frame_maxsize_ == that->frame_maxsize_) &&
           exclude_current_row_ == that->exclude_current_row_;
}

const std::string FrameNode::GetExprString() const {
    std::string str = "";

    if (nullptr != frame_range_) {
        str.append("range").append(frame_range_->GetExprString());
    }
    if (nullptr != frame_rows_) {
        if (!str.empty()) {
            str.append(",");
        }
        str.append("rows").append(frame_rows_->GetExprString());
    }
    return str;
}

FrameNode *FrameNode::ShadowCopy(NodeManager *nm) const {
    return dynamic_cast<FrameNode *>(nm->MakeFrameNode(frame_type(), frame_range(), frame_rows(), frame_maxsize()));
}

bool FrameNode::CanMergeWith(const FrameNode *that, const bool enbale_merge_with_maxsize) const {
    if (Equals(that)) {
        return true;
    }

    // Frame can't merge with null frame
    if (nullptr == that) {
        return false;
    }

    // RowsRange-like frames with frame_maxsize can't be merged when
    if (this->IsRowsRangeLikeFrame() && that->IsRowsRangeLikeFrame()) {
        // frame_maxsize_ > 0 and enbale_merge_with_maxsize=false
        if (!enbale_merge_with_maxsize && (this->frame_maxsize() > 0 || that->frame_maxsize_ > 0)) {
            return false;
        }
        // with different frame_maxsize can't be merged
        if (this->frame_maxsize_ != that->frame_maxsize_) {
            return false;
        }
    }

    // RowsRange-like pure history frames Can't be Merged with Rows Frame
    if (this->IsRowsRangeLikeFrame() && this->IsPureHistoryFrame() && kFrameRows == that->frame_type_) {
        return false;
    }
    if (that->IsRowsRangeLikeFrame() && that->IsPureHistoryFrame() && kFrameRows == this->frame_type_) {
        return false;
    }

    // Handle RowsRange-like frame with MAXSIZE and RowsFrame
    if (this->IsRowsRangeLikeMaxSizeFrame() && kFrameRows == that->frame_type_) {
        // Pure History RowRangeLike Frame with maxsize can't be merged with
        // Rows frame
        if (this->IsPureHistoryFrame()) {
            return false;
        }

        // RowRangeLike Frame with maxsize can't be merged with
        // Rows frame when maxsize <= row_preceding
        if (this->frame_maxsize() < that->GetHistoryRowsStartPreceding()) {
            return false;
        }
    }
    if (that->IsRowsRangeLikeMaxSizeFrame() && kFrameRows == this->frame_type_) {
        // Pure History RowRangeLike Frame with maxsize can't be merged with
        // Rows frame
        if (that->IsPureHistoryFrame()) {
            return false;
        }

        // RowRangeLike Frame with maxsize can't be merged with
        // Rows frame when maxsize <= row_preceding
        if (that->frame_maxsize() < this->GetHistoryRowsStartPreceding()) {
            return false;
        }
    }

    if (this->frame_type_ == kFrameRange || that->frame_type_ == kFrameRange) {
        return false;
    }
    return true;
}
void CastExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintValue(output, tab, DataTypeName(cast_type_), "cast_type", false);
    output << "\n";
    PrintSqlNode(output, tab, expr(), "expr", true);
}
const std::string CastExprNode::GetExprString() const {
    std::string str = DataTypeName(cast_type_);
    str.append("(").append(ExprString(expr())).append(")");
    return str;
}
bool CastExprNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const CastExprNode *that = dynamic_cast<const CastExprNode *>(node);
    return this->cast_type_ == that->cast_type_ && ExprEquals(expr(), that->expr());
}
CastExprNode *CastExprNode::CastFrom(ExprNode *node) { return dynamic_cast<CastExprNode *>(node); }

void WhenExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, when_expr(), "when", false);
    output << "\n";
    PrintSqlNode(output, tab, then_expr(), "then", true);
}
const std::string WhenExprNode::GetExprString() const {
    std::string str = "";
    str.append("when ").append(ExprString(when_expr())).append(" ").append("then ").append(ExprString(then_expr()));
    return str;
}
bool WhenExprNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const WhenExprNode *that = dynamic_cast<const WhenExprNode *>(node);
    return ExprEquals(when_expr(), that->when_expr()) && ExprEquals(then_expr(), that->then_expr());
}
void CaseWhenExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, when_expr_list(), "when_expr_list", false);
    output << "\n";
    PrintSqlNode(output, tab, else_expr(), "else_expr", true);
}
const std::string CaseWhenExprNode::GetExprString() const {
    std::string str = "";
    str.append("case ")
        .append(ExprString(when_expr_list()))
        .append(" ")
        .append("else ")
        .append(ExprString(else_expr()));
    return str;
}
bool CaseWhenExprNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const CaseWhenExprNode *that = dynamic_cast<const CaseWhenExprNode *>(node);
    return ExprEquals(when_expr_list(), that->when_expr_list()) && ExprEquals(else_expr(), that->else_expr());
}

void CallExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintSqlNode(output, tab, GetFnDef(), "function", false);
    size_t i = 0;
    bool has_over = over_ != nullptr;
    for (auto child : children_) {
        output << "\n";
        bool is_last_arg = i == children_.size() - 1;
        PrintSqlNode(output, tab, child, "arg[" + std::to_string(i++) + "]", is_last_arg);
    }
    if (has_over) {
        output << "\n";
        PrintSqlNode(output, tab, over_, "over", true);
    }
}
const std::string CallExprNode::GetExprString() const {
    std::string str = GetFnDef()->GetName();
    str.append("(");
    for (size_t i = 0; i < children_.size(); ++i) {
        str.append(children_[i]->GetExprString());
        if (i < children_.size() - 1) {
            str.append(", ");
        }
    }
    str.append(")");
    if (nullptr != over_) {
        if (over_->GetName().empty()) {
            str.append("over ANONYMOUS_WINDOW ");
        } else {
            str.append("over ").append(over_->GetName());
        }
    }
    return str;
}
bool CallExprNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    if (GetChildNum() != node->GetChildNum()) {
        return false;
    }
    for (size_t i = 0; i < GetChildNum(); ++i) {
        if (!ExprEquals(GetChild(i), node->GetChild(i))) {
            return false;
        }
    }
    const CallExprNode *that = dynamic_cast<const CallExprNode *>(node);
    return FnDefEquals(this->GetFnDef(), that->GetFnDef()) && SqlEquals(this->over_, that->over_) &&
           ExprNode::Equals(node);
}

void WindowDefNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab;
    output << "\n";
    PrintValue(output, tab, window_name_, "window_name", false);
    if (nullptr != union_tables_) {
        output << "\n";
        PrintSqlVector(output, tab, union_tables_->GetList(), "union_tables", false);
    }
    output << "\n";
    PrintValue(output, tab, ExprString(partitions_), "partitions", false);

    output << "\n";
    PrintValue(output, tab, ExprString(orders_), "orders", false);

    output << "\n";

    std::vector<std::string_view> attrs;
    if (exclude_current_time_) {
        attrs.emplace_back("exclude_current_time");
    }
    if (exclude_current_row()) {
        attrs.emplace_back("exclude_current_row");
    }
    if (instance_not_in_window_) {
        attrs.emplace_back("instance_not_in_window");
    }
    if (attrs.empty()) {
        PrintSqlNode(output, tab, frame_ptr_, "frame", true);
    } else {
        PrintSqlNode(output, tab, frame_ptr_, "frame", false);
        output << "\n";
        PrintValue(output, tab, absl::StrJoin(attrs, ", "), "attributes", true);
    }
}

// test if two window can be merged into single one
// besides the two windows is the same one, two can also merged when all of those condition meet:
// - union table equal
// - exclude current time equal
// - instance not in window equal
// - order equal
// - partion equal
// - window frame can be merged
// - exclude current row equal (frame type equal must)
bool WindowDefNode::CanMergeWith(const WindowDefNode *that, const bool enable_window_maxsize_merged) const {
    if (nullptr == that) {
        return false;
    }
    if (Equals(that)) {
        return true;
    }
    bool can_merge = SqlListEquals(this->union_tables_, that->union_tables_) &&
                     this->exclude_current_time() == that->exclude_current_time() &&
                     this->exclude_current_row() == that->exclude_current_row() &&
                     this->instance_not_in_window() == that->instance_not_in_window() &&
                     ExprEquals(this->orders_, that->orders_) && ExprEquals(this->partitions_, that->partitions_) &&
                     nullptr != frame_ptr_ &&
                     this->frame_ptr_->CanMergeWith(that->frame_ptr_, enable_window_maxsize_merged);

    if (this->exclude_current_row() && that->exclude_current_row()) {
        // two window different frame (rows & rows_range) can merge
        // only when they do not set exclude_current_row neither
        can_merge &= this->GetFrame()->frame_type() == that->GetFrame()->frame_type();
    }
    return can_merge;
}

WindowDefNode* WindowDefNode::ShadowCopy(NodeManager *nm) const {
    return dynamic_cast<WindowDefNode *>(nm->MakeWindowDefNode(union_tables_, GetPartitions(), GetOrders(), GetFrame(),
                                                               exclude_current_time_, instance_not_in_window_));
}

bool WindowDefNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const WindowDefNode *that = dynamic_cast<const WindowDefNode *>(node);
    return this->window_name_ == that->window_name_ && this->exclude_current_time_ == that->exclude_current_time_ &&
           this->instance_not_in_window_ == that->instance_not_in_window_ &&
           SqlListEquals(this->union_tables_, that->union_tables_) && ExprEquals(this->orders_, that->orders_) &&
           ExprEquals(this->partitions_, that->partitions_) && SqlEquals(this->frame_ptr_, that->frame_ptr_);
}

void ResTarget::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    output << "\n";
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintSqlNode(output, tab, val_, "val", false);
    output << "\n";
    PrintValue(output, tab, name_, "name", true);
}
bool ResTarget::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const ResTarget *that = dynamic_cast<const ResTarget *>(node);
    return this->name_ == that->name_ && ExprEquals(this->val_, that->val_);
}

void SelectQueryNode::Print(std::ostream &output, const std::string &org_tab) const {
    QueryNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    bool last_child = false;
    PrintValue(output, tab, distinct_opt_ ? "true" : "false", "distinct_opt", last_child);
    output << "\n";
    PrintSqlNode(output, tab, where_clause_ptr_, "where_expr", last_child);
    output << "\n";
    PrintSqlNode(output, tab, group_clause_ptr_, "group_expr_list", last_child);
    output << "\n";
    PrintSqlNode(output, tab, having_clause_ptr_, "having_expr", last_child);
    output << "\n";
    PrintSqlNode(output, tab, order_clause_ptr_, "order_expr_list", last_child);
    output << "\n";
    PrintSqlNode(output, tab, limit_ptr_, "limit", last_child);
    output << "\n";
    PrintSqlNodeList(output, tab, select_list_, "select_list", last_child);
    output << "\n";
    PrintSqlNodeList(output, tab, tableref_list_, "tableref_list", last_child);
    output << "\n";
    last_child = true;
    PrintSqlNodeList(output, tab, window_list_, "window_list", last_child);
}
bool SelectQueryNode::Equals(const SqlNode *node) const {
    if (!QueryNode::Equals(node)) {
        return false;
    }
    const SelectQueryNode *that = dynamic_cast<const SelectQueryNode *>(node);
    return this->distinct_opt_ == that->distinct_opt_ && SqlListEquals(this->select_list_, that->select_list_) &&
           SqlListEquals(this->tableref_list_, that->tableref_list_) &&
           SqlListEquals(this->window_list_, that->window_list_) &&
           SqlEquals(this->where_clause_ptr_, that->where_clause_ptr_) &&
           SqlEquals(this->group_clause_ptr_, that->group_clause_ptr_) &&
           SqlEquals(this->having_clause_ptr_, that->having_clause_ptr_) &&
           ExprEquals(this->order_clause_ptr_, that->order_clause_ptr_) &&
           SqlEquals(this->limit_ptr_, that->limit_ptr_);
}

static absl::flat_hash_map<SqlNodeType, absl::string_view> CreateSqlNodeTypeToNamesMap() {
    absl::flat_hash_map<SqlNodeType, absl::string_view> map = {
        {kCreateStmt, "CREATE"},
        {kCmdStmt, "CMD"},
        {kShowStmt, "kShowStmt"},
        {kExplainStmt, "EXPLAIN"},
        {kName, "kName"},
        {kType, "kType"},
        {kNodeList, "kNodeList"},
        {kResTarget, "kResTarget"},
        {kTableRef, "kTableRef"},
        {kQuery, "kQuery"},
        {kColumnDesc, "kColumnDesc"},
        {kColumnIndex, "kColumnIndex"},
        {kExpr, "kExpr"},
        {kWindowDef, "kWindowDef"},
        {kFrames, "kFrame"},
        {kFrameExtent, "kFrameExtent"},
        {kFrameBound, "kBound"},
        {kConst, "kConst"},
        {kLimit, "kLimit"},
        {kFnList, "kFnList"},
        {kFnDef, "kFnDef"},
        {kFnHeader, "kFnHeader"},
        {kFnPara, "kFnPara"},
        {kFnReturnStmt, "kFnReturnStmt"},
        {kFnAssignStmt, "kFnAssignStmt"},
        {kFnIfStmt, "kFnIfStmt"},
        {kFnElifStmt, "kFnElseifStmt"},
        {kFnElseStmt, "kFnElseStmt"},
        {kFnIfBlock, "kFnIfBlock"},
        {kFnElseBlock, "kFnElseBlock"},
        {kFnIfElseBlock, "kFnIfElseBlock"},
        {kFnElifBlock, "kFnElIfBlock"},
        {kFnValue, "kFnValue"},
        {kFnForInStmt, "kFnForInStmt"},
        {kFnForInBlock, "kFnForInBlock"},
        {kExternalFnDef, "kExternFnDef"},
        {kUdfDef, "kUdfDef"},
        {kUdfByCodeGenDef, "kUdfByCodeGenDef"},
        {kUdafDef, "kUdafDef"},
        {kLambdaDef, "kLambdaDef"},
        {kPartitionMeta, "kPartitionMeta"},
        {kCreateIndexStmt, "kCreateIndexStmt"},
        {kInsertStmt, "kInsertStmt"},
        {kWindowFunc, "kWindowFunc"},
        {kIndexKey, "kIndexKey"},
        {kIndexTs, "kIndexTs"},
        {kIndexTTLType, "kIndexTTLType"},
        {kIndexTTL, "kIndexTTL"},
        {kIndexVersion, "kIndexVersion"},
        {kReplicaNum, "kReplicaNum"},
        {kPartitionNum, "kPartitionNum"},
        {kStorageMode, "kStorageMode"},
        {kCompressType, "kCompressType"},
        {kFn, "kFn"},
        {kFnParaList, "kFnParaList"},
        {kCreateSpStmt, "kCreateSpStmt"},
        {kDistributions, "kDistributions"},
        {kInputParameter, "kInputParameter"},
        {kDeployStmt, "kDeployStmt"},
        {kSelectIntoStmt, "kSelectIntoStmt"},
        {kLoadDataStmt, "kLoadDataStmt"},
        {kSetStmt, "kSetStmt"},
        {kDeleteStmt, "kDeleteStmt"},
        {kCreateFunctionStmt, "kCreateFunctionStmt"},
        {kDynamicUdfFnDef, "kDynamicUdfFnDef"},
        {kDynamicUdafFnDef, "kDynamicUdafFnDef"},
        {kWithClauseEntry, "kWithClauseEntry"},
        {kAlterTableStmt, "kAlterTableStmt"},
    };
    for (auto kind = 0; kind < SqlNodeType::kSqlNodeTypeLast; ++kind) {
        DCHECK(map.find(static_cast<SqlNodeType>(kind)) != map.end())
            << "name of " << kind << " not exist";
    }
    return map;
}

static const auto& GetSqlNodeTypeToNamesMap() {
    static const auto &map = *new auto(CreateSqlNodeTypeToNamesMap());
    return map;
}

std::string NameOfSqlNodeType(const SqlNodeType &type) {
    auto& map = GetSqlNodeTypeToNamesMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return std::string(it->second);
    }
    return "kUnknow";
}

absl::string_view CmdTypeName(const CmdType type) {
    auto &map = GetCmdTypeNamesMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return it->second;
    }
    return "undefined cmd type";
}

std::string SetOperatorName(SetOperationType type, bool dis) {
    std::string distinct = dis ? "DISTINCT" : "ALL";
    switch (type) {
        case SetOperationType::UNION:
            return "UNION " + distinct;
        case SetOperationType::EXCEPT:
            return "EXCEPT " + distinct;
        case SetOperationType::INTERSECT:
            return "INTERSECT " + distinct;
    }
}

std::string DataTypeName(DataType type) {
    auto &map = GetDataTypeNamesMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return std::string(it->second);
    }
    return "unknown";
}

std::string ExprTypeName(ExprType type) {
    auto &map = GetExprTypeNamesMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return std::string(it->second);
    }
    return "unknown";
}

std::ostream &operator<<(std::ostream &output, const SqlNode &thiz) {
    thiz.Print(output, "");
    return output;
}
std::ostream &operator<<(std::ostream &output, const SqlNodeList &thiz) {
    thiz.Print(output, "");
    return output;
}

void FillSqlNodeList2NodeVector(SqlNodeList *node_list_ptr,
                                std::vector<SqlNode *> &node_list  // NOLINT (runtime/references)
) {
    if (nullptr != node_list_ptr) {
        for (auto item : node_list_ptr->GetList()) {
            node_list.push_back(item);
        }
    }
}
void ColumnOfExpression(const ExprNode *node_ptr, std::vector<const node::ExprNode *> *columns) {
    if (nullptr == columns || nullptr == node_ptr) {
        return;
    }
    switch (node_ptr->expr_type_) {
        case kExprPrimary: {
            return;
        }
        case kExprOrderExpression: {
            ColumnOfExpression(dynamic_cast<const node::OrderExpression *>(node_ptr)->expr(), columns);
            return;
        }
        case kExprColumnRef: {
            columns->push_back(dynamic_cast<const node::ColumnRefNode *>(node_ptr));
            return;
        }
        case kExprColumnId: {
            columns->push_back(dynamic_cast<const node::ColumnIdNode *>(node_ptr));
            return;
        }
        default: {
            for (auto child : node_ptr->children_) {
                ColumnOfExpression(child, columns);
            }
        }
    }
}
// Check if given expression is or based on an aggregation expression.
bool IsAggregationExpression(const udf::UdfLibrary *lib, const ExprNode *node_ptr) {
    if (kExprCall == node_ptr->GetExprType()) {
        const CallExprNode *func_node_ptr = dynamic_cast<const CallExprNode *>(node_ptr);
        if (lib->IsUdaf(func_node_ptr->GetFnDef()->GetName(), func_node_ptr->GetChildNum())) {
            return true;
        }
    }
    for (auto child : node_ptr->children_) {
        if (IsAggregationExpression(lib, child)) {
            return true;
        }
    }
    return false;
}

bool WindowOfExpression(const std::map<std::string, const WindowDefNode *> &windows, ExprNode *node_ptr,
                        const WindowDefNode **output) {
    // try to resolved window ptr from expression like: call(args...) over window
    if (kExprCall == node_ptr->GetExprType()) {
        CallExprNode *func_node_ptr = dynamic_cast<CallExprNode *>(node_ptr);
        if (nullptr != func_node_ptr->GetOver()) {
            if (func_node_ptr->GetOver()->GetName().empty()) {
                // anonymous over
                *output = func_node_ptr->GetOver();
            } else {
                auto iter = windows.find(func_node_ptr->GetOver()->GetName());
                if (iter == windows.cend()) {
                    LOG(WARNING) << "Fail to resolved window from expression: " << func_node_ptr->GetOver()->GetName()
                                 << " undefined";
                    return false;
                }
                *output = iter->second;
            }
        }
    }

    // try to resolved windows of children
    // make sure there is only one window for the whole expression
    for (auto child : node_ptr->children_) {
        const WindowDefNode *w = nullptr;
        if (!WindowOfExpression(windows, child, &w)) {
            return false;
        }
        // resolve window of child
        if (nullptr != w) {
            if (*output == nullptr) {
                *output = w;
            } else if (!node::SqlEquals(*output, w)) {
                LOG(WARNING) << "Fail to resolved window from expression: "
                             << "expression depends on more than one window";
                return false;
            }
        }
    }
    return true;
}
std::string ExprString(const ExprNode *expr) { return nullptr == expr ? std::string() : expr->GetExprString(); }
const bool IsNullPrimary(const ExprNode *expr) {
    return nullptr != expr && expr->expr_type_ == hybridse::node::kExprPrimary &&
           dynamic_cast<const node::ConstNode *>(expr)->IsNull();
}

bool ExprListNullOrEmpty(const ExprListNode *expr) { return nullptr == expr || expr->IsEmpty(); }
bool ExprIsSimple(const ExprNode *expr) {
    if (nullptr == expr) {
        return false;
    }

    switch (expr->expr_type_) {
        case node::kExprPrimary: {
            return true;
        }
        case node::kExprColumnRef: {
            return true;
        }
        case node::kExprAll: {
            return true;
        }
        default: {
            return false;
        }
    }
    return true;
}
bool ExprIsConst(const ExprNode *expr) {
    if (nullptr == expr) {
        return true;
    }
    switch (expr->expr_type_) {
        case node::kExprPrimary: {
            return true;
        }
        case node::kExprBetween: {
            std::vector<node::ExprNode *> expr_list;
            auto between_expr = dynamic_cast<const node::BetweenExpr *>(expr);
            expr_list.push_back(between_expr->GetLow());
            expr_list.push_back(between_expr->GetHigh());
            expr_list.push_back(between_expr->GetLhs());
            return ExprListIsConst(expr_list);
        }
        case node::kExprCall: {
            auto call_expr = dynamic_cast<const node::CallExprNode *>(expr);
            std::vector<node::ExprNode *> expr_list(call_expr->children_);
            if (nullptr != call_expr->GetOver()) {
                if (nullptr != call_expr->GetOver()->GetOrders()) {
                    expr_list.push_back(call_expr->GetOver()->GetOrders());
                }
                if (nullptr != call_expr->GetOver()->GetPartitions()) {
                    for (auto expr : call_expr->GetOver()->GetPartitions()->children_) {
                        expr_list.push_back(expr);
                    }
                }
            }
            return ExprListIsConst(expr_list);
        }
        case node::kExprColumnRef:
        case node::kExprId:
        case node::kExprAll: {
            return false;
        }
        default: {
            return ExprListIsConst(expr->children_);
        }
    }
}

bool ExprListIsConst(const std::vector<node::ExprNode *> &exprs) {
    if (exprs.empty()) {
        return true;
    }
    for (auto expr : exprs) {
        if (!ExprIsConst(expr)) {
            return false;
        }
    }
    return true;
}
void CreateStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, db_name_.empty() ? table_name_ : db_name_ + "." + table_name_, "table", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(op_if_not_exist_), "IF NOT EXIST", false);
    output << "\n";
    PrintSqlVector(output, tab, column_desc_list_, "column_desc_list", false);
    output << "\n";
    if (like_clause_ != nullptr) {
        like_clause_->Print(output, tab);
    }
    PrintSqlVector(output, tab, table_option_list_, "table_option_list", true);
}

void CreateTableLikeClause::Print(std::ostream &output, const std::string &tab) const {
    output << tab << SPACE_ST << "like:";
    output << "\n";
    PrintValue(output, tab + INDENT, ToKindString(kind_), "kind", false);
    output << "\n";
    PrintValue(output, tab + INDENT, path_, "path", false);
    output << "\n";
}

void ColumnDefNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, column_name_, "column_name", false);
    output << "\n";
    PrintValue(output, tab, DataTypeName(column_type_), "column_type", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(op_not_null_), "NOT NULL", !default_value_);
    if (default_value_) {
        output << "\n";
        PrintSqlNode(output, tab, default_value_, "default_value", true);
    }
}

void ColumnIndexNode::SetTTL(ExprListNode *ttl_node_list) {
    if (nullptr == ttl_node_list) {
        abs_ttl_ = -1;
        lat_ttl_ = -1;
        return;
    } else {
        uint32_t node_num = ttl_node_list->GetChildNum();
        if (node_num > 2) {
            abs_ttl_ = -1;
            lat_ttl_ = -1;
            return;
        }
        for (uint32_t i = 0; i < node_num; i++) {
            auto ttl_node = ttl_node_list->GetChild(i);
            if (ttl_node == nullptr) {
                abs_ttl_ = -1;
                lat_ttl_ = -1;
                return;
            }
            switch (ttl_node->GetExprType()) {
                case kExprPrimary: {
                    const ConstNode *ttl = dynamic_cast<ConstNode *>(ttl_node);
                    switch (ttl->GetDataType()) {
                        case hybridse::node::kInt32:
                            if (ttl->GetTTLType() == hybridse::node::kAbsolute) {
                                abs_ttl_ = -1;
                                lat_ttl_ = -1;
                                return;
                            } else {
                                lat_ttl_ = ttl->GetInt();
                            }
                            break;
                        case hybridse::node::kInt64:
                            if (ttl->GetTTLType() == hybridse::node::kAbsolute) {
                                abs_ttl_ = -1;
                                lat_ttl_ = -1;
                                return;
                            } else {
                                lat_ttl_ = ttl->GetLong();
                            }
                            break;
                        case hybridse::node::kDay:
                        case hybridse::node::kHour:
                        case hybridse::node::kMinute:
                        case hybridse::node::kSecond:
                            if (ttl->GetTTLType() == hybridse::node::kAbsolute) {
                                abs_ttl_ = ttl->GetMillis();
                            } else {
                                abs_ttl_ = -1;
                                lat_ttl_ = -1;
                                return;
                            }
                            break;
                        default: {
                            return;
                        }
                    }
                    break;
                }
                default: {
                    LOG(WARNING) << "can't set ttl with expr type " << ExprTypeName(ttl_node->GetExprType());
                    return;
                }
            }
        }
    }
}

void ColumnIndexNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, key_, "keys", false);
    output << "\n";
    PrintValue(output, tab, ts_, "ts_col", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(abs_ttl_), "abs_ttl", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(lat_ttl_), "lat_ttl", false);
    output << "\n";
    PrintValue(output, tab, ttl_type_, "ttl_type", false);
    output << "\n";
    PrintValue(output, tab, version_, "version_column", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(version_count_), "version_count", true);
}
void CmdNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::string(CmdTypeName(cmd_type_)), "cmd_type", false);
    output << "\n";
    if (IsIfNotExists()) {
        PrintValue(output, tab, "true", "if_not_exists", false);
        output << "\n";
    }
    if (IsIfExists()) {
        PrintValue(output, tab, "true", "if_exists", false);
        output << "\n";
    }
    PrintValue(output, tab, args_, "args", true);
}

bool CmdNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    auto* cnode = dynamic_cast<const CmdNode*>(node);
    return cnode != nullptr && GetCmdType() == cnode->GetCmdType() && IsIfNotExists() == cnode->IsIfNotExists() &&
           std::equal(std::begin(GetArgs()), std::end(GetArgs()), std::begin(cnode->GetArgs()),
                      std::end(cnode->GetArgs()));
}

bool ShowNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    auto* show_node = dynamic_cast<const ShowNode*>(node);
    return show_node != nullptr && GetShowType() == show_node->GetShowType() && GetTarget() == show_node->GetTarget()
        && GetLikeStr() == show_node->GetLikeStr();
}

void ShowNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, ShowStmtTypeName(show_type_), "show type", false);
    output << "\n";
    PrintValue(output, tab, target_, "target", false);
    output << "\n";
    PrintValue(output, tab, like_str_, "like_str", true);
}

void CreateFunctionNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, function_name_, "function_name", false);
    output << "\n";
    PrintSqlNode(output, tab, return_type_, "return_type", false);
    output << "\n";
    PrintSqlVector(output, tab, args_type_, "args_type", false);
    output << "\n";
    PrintValue(output, tab, IsAggregate() ? "true" : "false", "is_aggregate", false);
    output << "\n";
    PrintValue(output, tab, Options().get(), "options", true);
}

void CreateIndexNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, index_name_, "index_name", false);
    output << "\n";
    PrintValue(output, tab, db_name_.empty() ? table_name_ : db_name_ + "." + table_name_, "table_name", false);
    output << "\n";
    PrintSqlNode(output, tab, index_, "index", true);
}
void ExplainNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, ExplainTypeName(explain_type_), "explain_type", false);
    output << "\n";
    PrintSqlNode(output, tab, query_, "query", true);
}

void DeployNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);

    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, if_not_exists_ ? "true" : "false", "if_not_exists", false);
    output << "\n";
    PrintValue(output, tab, name_, "name", false);
    output << "\n";
    PrintValue(output, tab, Options().get(), "options", false);
    output << "\n";
    PrintSqlNode(output, tab, stmt_, "stmt", true);
}

void SelectIntoNode::Print(std::ostream &output, const std::string &tab) const {
    SqlNode::Print(output, tab);
    const std::string new_tab = tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, new_tab, OutFile(), "out_file", false);
    output << "\n";
    PrintSqlNode(output, new_tab, Query(), "query", false);
    output << "\n";
    PrintValue(output, new_tab, Options().get(), "options", false);
    output << "\n";
    PrintValue(output, new_tab, ConfigOptions().get(), "config_options", true);
}

void LoadDataNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);

    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, File(), "file", false);
    output << "\n";
    PrintValue(output, tab, Db(), "db", false);
    output << "\n";
    PrintValue(output, tab, Table(), "table", false);
    output << "\n";
    PrintValue(output, tab, Options().get(), "options", false);
    output << "\n";
    PrintValue(output, tab, ConfigOptions().get(), "config_options", true);
}

void SetNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, node::VariableScopeName(Scope()), "scope", false);
    output << "\n";
    PrintValue(output, tab, Key(), "key", false);
    output << "\n";
    PrintSqlNode(output, tab, Value(), "value", true);
}

void InsertStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, db_name_.empty() ? table_name_ : db_name_ + "." + table_name_, "table_name", false);
    output << "\n";
    if (is_all_) {
        PrintValue(output, tab, "all", "columns", false);
    } else {
        PrintValue(output, tab, columns_, "columns", false);
    }
    output << "\n";
    PrintSqlVector(output, tab, values_, "values", false);
}
void BinaryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlVector(output, tab, children_, ExprOpTypeName(op_), true);
}
const std::string BinaryExpr::GetExprString() const {
    std::string str = "";
    str.append("")
        .append(children_[0]->GetExprString())
        .append(" ")
        .append(ExprOpTypeName(op_))
        .append(" ")
        .append(children_[1]->GetExprString())
        .append("");
    return str;
}
bool BinaryExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const BinaryExpr *that = dynamic_cast<const BinaryExpr *>(node);
    return this->op_ == that->op_ && ExprNode::Equals(node);
}

void UnaryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlVector(output, tab, children_, ExprOpTypeName(op_), true);
}
const std::string UnaryExpr::GetExprString() const {
    std::string str = "";
    if (op_ == kFnOpBracket) {
        str.append("(").append(children_[0]->GetExprString()).append(")");
        return str;
    }
    str.append(ExprOpTypeName(op_)).append(" ").append(children_[0]->GetExprString());
    return str;
}
bool UnaryExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const UnaryExpr *that = dynamic_cast<const UnaryExpr *>(node);
    return this->op_ == that->op_ && ExprNode::Equals(node);
}

bool ExprIdNode::IsListReturn(ExprAnalysisContext *ctx) const {
    return GetOutputType() != nullptr && GetOutputType()->base() == kList;
}

void ExprIdNode::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, GetExprString(), "var", true);
}
const std::string ExprIdNode::GetExprString() const { return "%" + std::to_string(id_) + "(" + name_ + ")"; }
bool ExprIdNode::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const ExprIdNode *that = dynamic_cast<const ExprIdNode *>(node);
    return this->name_ == that->name_ && this->id_ == that->id_;
}

void ExprNode::Print(std::ostream &output, const std::string &org_tab) const {
    output << org_tab << SPACE_ST << "expr[" << ExprTypeName(expr_type_) << "]";
}
const std::string ExprNode::GetExprString() const { return ""; }
const std::string ExprNode::GenerateExpressionName() const { return GetExprString(); }
bool ExprNode::Equals(const ExprNode *that) const {
    if (this == that) {
        return true;
    }
    if (nullptr == that || expr_type_ != that->expr_type_ || children_.size() != that->children_.size()) {
        return false;
    }

    auto iter1 = children_.cbegin();
    auto iter2 = that->children_.cbegin();
    while (iter1 != children_.cend()) {
        if (!(*iter1)->Equals(*iter2)) {
            return false;
        }
        iter1++;
        iter2++;
    }
    return true;
}

void ExprListNode::Print(std::ostream &output, const std::string &org_tab) const {
    PrintNodeList(children_, output, org_tab);
}

void ArrayExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    output << "\n";

    auto sub_indent = org_tab + INDENT;
    output << sub_indent << SPACE_ST << "values:";
    if (!children_.empty()) {
        output << "\n";
    }
    PrintNodeList(children_, output, sub_indent + INDENT);
    if (specific_type_ != nullptr) {
        output << "\n";
        PrintValue(output, sub_indent, specific_type_->DebugString(), "type", true);
    }
}

const std::string ExprListNode::GetExprString() const {
    if (children_.empty()) {
        return "()";
    } else {
        std::string str = "(";
        auto iter = children_.cbegin();
        str.append((*iter)->GetExprString());
        iter++;
        for (; iter != children_.cend(); iter++) {
            str.append(",");
            str.append((*iter)->GetExprString());
        }
        str.append(")");
        return str;
    }
}

// brackets (`[]`) represents array expr, prefix by the optional 'ARRAY<type>'
const std::string ArrayExpr::GetExprString() const {
    return absl::StrCat(
        (specific_type_ != nullptr ? specific_type_->DebugString() : ""), "[",
        absl::StrJoin(children_, ",",
                      [](std::string *out, const ExprNode *expr) { absl::StrAppend(out, expr->GetExprString()); }),
        "]");
}

void FnParaNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";

    PrintSqlNode(output, tab, GetParaType(), GetName(), true);
}
void FnNodeFnHeander::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, this->name_, "func_name", true);
    output << "\n";
    PrintSqlNode(output, tab, ret_type_, "return_type", true);
    output << "\n";
    PrintSqlNode(output, tab, reinterpret_cast<const SqlNode *>(parameters_), "parameters", true);
}

const std::string FnNodeFnHeander::GeIRFunctionName() const {
    std::string fn_name = name_;
    if (!parameters_->children.empty()) {
        for (node::SqlNode *node : parameters_->children) {
            node::FnParaNode *para_node = dynamic_cast<node::FnParaNode *>(node);

            switch (para_node->GetParaType()->base_) {
                case hybridse::node::kList:
                case hybridse::node::kIterator:
                case hybridse::node::kMap:
                    fn_name.append(".").append(para_node->GetParaType()->GetName());
                    break;
                default: {
                    fn_name.append(".").append(para_node->GetParaType()->GetName());
                }
            }
        }
    }
    return fn_name;
}
void FnNodeFnDef::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, header_, "header", false);
    output << "\n";
    PrintSqlNode(output, tab, block_, "block", true);
}
void FnNodeList::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlVector(output, tab, children, "list", true);
}
void FnAssignNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, is_ssa_ ? "true" : "false", "ssa", false);
    output << "\n";
    PrintSqlNode(output, tab, reinterpret_cast<const SqlNode *>(expression_), GetName(), true);
}
void FnReturnStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, return_expr_, "return", true);
}

void FnIfNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, expression_, "if", true);
}
void FnElifNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, expression_, "elif", true);
}
void FnElseNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    output << "\n";
}
void FnForInNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, var_->GetName(), "var", false);
    output << "\n";
    PrintSqlNode(output, tab, in_expression_, "in", true);
}

void FnIfBlock::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, if_node, "if", false);
    output << "\n";
    PrintSqlNode(output, tab, block_, "block", true);
}

void FnElifBlock::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, elif_node_, "elif", false);
    output << "\n";
    PrintSqlNode(output, tab, block_, "block", true);
}
void FnElseBlock::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, block_, "block", true);
}
void FnIfElseBlock::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, if_block_, "if", false);
    output << "\n";
    PrintSqlVector(output, tab, elif_blocks_, "elif_list", false);
    output << "\n";
    PrintSqlNode(output, tab, else_block_, "else", true);
}

void FnForInBlock::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, for_in_node_, "for", false);
    output << "\n";
    PrintSqlNode(output, tab, block_, "body", true);
}
void StructExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    PrintValue(output, tab, class_name_, "name", false);
    output << "\n";
    PrintSqlNode(output, tab, fileds_, "fileds", false);
    output << "\n";
    PrintSqlNode(output, tab, methods_, "methods", true);
}

void TypeNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;

    output << "\n";
    PrintValue(output, tab, GetName(), "type", true);
}
bool TypeNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }

    const TypeNode *that = dynamic_cast<const TypeNode *>(node);
    return this->base_ == that->base_ &&
           std::equal(
               this->generics_.cbegin(), this->generics_.cend(), that->generics_.cbegin(),
               [&](const hybridse::node::TypeNode *a, const hybridse::node::TypeNode *b) { return TypeEquals(a, b); });
}

void JoinNode::Print(std::ostream &output, const std::string &org_tab) const {
    TableRefNode::Print(output, org_tab);

    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, JoinTypeName(join_type_), "join_type", false);
    output << "\n";
    PrintSqlNode(output, tab, left_, "left", false);
    output << "\n";
    PrintSqlNode(output, tab, right_, "right", true);
    output << "\n";
    PrintSqlNode(output, tab, orders_, "order_expressions", true);
    output << "\n";
    PrintSqlNode(output, tab, condition_, "on", true);
}

bool JoinNode::Equals(const SqlNode *node) const {
    if (!TableRefNode::Equals(node)) {
        return false;
    }
    const JoinNode *that = dynamic_cast<const JoinNode *>(node);
    return join_type_ == that->join_type_ && ExprEquals(condition_, that->condition_) &&
           ExprEquals(this->orders_, that->orders_) && SqlEquals(this->left_, that->right_);
}

void QueryExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, query_, "query", true);
}
bool QueryExpr::Equals(const ExprNode *node) const {
    if (this == node) {
        return true;
    }
    if (nullptr == node || expr_type_ != node->expr_type_) {
        return false;
    }
    const QueryExpr *that = dynamic_cast<const QueryExpr *>(node);
    return SqlEquals(this->query_, that->query_) && ExprNode::Equals(node);
}
const std::string QueryExpr::GetExprString() const { return "query expr"; }

void TableRefNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    output << ": " << TableRefTypeName(ref_type_);
}
bool TableRefNode::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }

    const TableRefNode *that = dynamic_cast<const TableRefNode *>(node);
    return this->ref_type_ == that->ref_type_ && this->alias_table_name_ == that->alias_table_name_;
}

void QueryRefNode::Print(std::ostream &output, const std::string &org_tab) const {
    TableRefNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, query_, "query", true);
}
bool QueryRefNode::Equals(const SqlNode *node) const {
    if (!TableRefNode::Equals(node)) {
        return false;
    }
    const QueryRefNode *that = dynamic_cast<const QueryRefNode *>(node);
    return SqlEquals(this->query_, that->query_);
}

void FrameBound::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT;
    output << "\n";
    PrintValue(output, tab, BoundTypeName(bound_type_), "bound", false);

    if (kPrecedingUnbound != bound_type_ && kFollowingUnbound != bound_type_) {
        // unbound information is enough from `bound:` field
        output << "\n";
        PrintValue(output, tab, std::to_string(offset_), "offset", true);
    }
}

bool FrameBound::is_offset_bound() const {
    return bound_type_ == kPreceding || bound_type_ == kOpenPreceding || bound_type_ == kFollowing ||
           bound_type_ == kOpenFollowing;
}

int FrameBound::Compare(const FrameBound *bound1, const FrameBound *bound2) {
    if (SqlEquals(bound1, bound2)) {
        return 0;
    }

    if (nullptr == bound1) {
        return -1;
    }

    if (nullptr == bound2) {
        return 1;
    }
    // FromeBound itself do not know it is start or end frame, just assume same for the two
    int64_t offset1 = bound1->GetSignedOffset(true);
    int64_t offset2 = bound2->GetSignedOffset(true);
    return offset1 == offset2 ? 0 : offset1 > offset2 ? 1 : -1;
}
bool FrameBound::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const FrameBound *that = dynamic_cast<const FrameBound *>(node);
    return this->bound_type_ == that->bound_type_ && this->offset_ == that->offset_;
}

void BetweenExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, is_not_between() ? "true" : "false", "is_not_between", false);
    output << "\n";
    PrintSqlNode(output, tab, GetLhs(), "value", false);
    output << "\n";
    PrintSqlNode(output, tab, GetLow(), "left", false);
    output << "\n";
    PrintSqlNode(output, tab, GetHigh(), "right", true);
}
const std::string BetweenExpr::GetExprString() const {
    std::string str = "";
    str.append(ExprString(GetLhs()));
    if (is_not_between_) {
        str.append(" not ");
    }
    str.append(" between ").append(ExprString(GetLow())).append(" and ").append(ExprString(GetHigh()));
    return str;
}
bool BetweenExpr::Equals(const ExprNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }
    const BetweenExpr *that = dynamic_cast<const BetweenExpr *>(node);
    return is_not_between() == that->is_not_between() && ExprEquals(GetLhs(), that->GetLhs()) &&
           ExprEquals(GetLow(), that->GetLow()) && ExprEquals(GetHigh(), that->GetHigh());
}

void InExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, IsNot() ? "true" : "false", "is_not", false);
    output << "\n";
    PrintSqlNode(output, tab, GetLhs(), "lhs", false);
    output << "\n";
    PrintSqlNode(output, tab, GetInList(), GetInTypeString(), true);
}

std::string InExpr::GetInTypeString() const {
    auto in_list = GetInList();
    if (in_list == nullptr) {
        return "unknown";
    }
    switch (in_list->GetExprType()) {
        case kExprList:
            return "in_list";
        case kExprQuery:
            return "query";
        default:
            return "unknown";
    }
}

const std::string InExpr::GetExprString() const {
    std::string str = "";
    absl::StrAppend(&str, ExprString(GetLhs()));
    if (IsNot()) {
        absl::StrAppend(&str, " not");
    }
    absl::StrAppend(&str, " in ");
    absl::StrAppend(&str, ExprString(GetInList()));
    return str;
}

bool InExpr::Equals(const ExprNode *node) const {
    if (!ExprNode::Equals(node)) {
        return false;
    }
    const InExpr *in_expr = dynamic_cast<const InExpr *>(node);
    return in_expr != nullptr && IsNot() == in_expr->IsNot();
}

void EscapedExpr::Print(std::ostream &output, const std::string &org_tab) const {
    ExprNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlNode(output, tab, GetPattern(), "pattern", false);
    output << "\n";
    PrintSqlNode(output, tab, GetEscape(), "escape", true);
}

const std::string EscapedExpr::GetExprString() const {
    std::string str = "";
    absl::StrAppend(&str, ExprString(GetPattern()));
    absl::StrAppend(&str, " ESCAPE ");
    absl::StrAppend(&str, ExprString(GetEscape()));
    return str;
}

std::string FnDefNode::GetFlatString() const {
    std::stringstream ss;
    ss << GetName() << "(";
    for (size_t i = 0; i < GetArgSize(); ++i) {
        if (IsArgNullable(i)) {
            ss << "nullable ";
        }
        auto arg_type = GetArgType(i);
        if (arg_type != nullptr) {
            ss << arg_type->GetName();
        } else {
            ss << "?";
        }
        if (i < GetArgSize() - 1) {
            ss << ", ";
        }
    }
    ss << ")";
    return ss.str();
}

bool FnDefNode::RequireListAt(ExprAnalysisContext *ctx, size_t index) const {
    return index < GetArgSize() && GetArgType(index)->base() == kList;
}
bool FnDefNode::IsListReturn(ExprAnalysisContext *ctx) const {
    return GetReturnType() != nullptr && GetReturnType()->base() == kList;
}

bool ExternalFnDefNode::RequireListAt(ExprAnalysisContext *ctx, size_t index) const {
    if (IsResolved()) {
        return index < GetArgSize() && GetArgType(index)->base() == kList;
    } else {
        return ctx->library()->RequireListAt(GetName(), index);
    }
}
bool ExternalFnDefNode::IsListReturn(ExprAnalysisContext *ctx) const {
    if (IsResolved()) {
        return GetReturnType() != nullptr && GetReturnType()->base() == kList;
    } else {
        return ctx->library()->IsListReturn(GetName());
    }
}

void ExternalFnDefNode::Print(std::ostream &output, const std::string &org_tab) const {
    if (!IsResolved()) {
        output << org_tab << "[Unresolved](" << function_name_ << ")";
    } else {
        output << org_tab << "[kExternalFnDef] ";
        if (GetReturnType() == nullptr) {
            output << "?";
        } else {
            output << GetReturnType()->GetName();
        }
        output << " " << function_name_ << "(";
        for (size_t i = 0; i < GetArgSize(); ++i) {
            auto arg_ty = GetArgType(i);
            if (arg_ty == nullptr) {
                output << "?";
            } else {
                output << arg_ty->GetName();
            }
            if (i < GetArgSize() - 1) {
                output << ", ";
            }
        }
        if (variadic_pos_ >= 0) {
            output << ", ...";
        }
        output << ")";
        if (return_by_arg_) {
            output << "\n";
            const std::string tab = org_tab + INDENT;
            PrintValue(output, tab, "true", "return_by_arg", true);
        }
    }
}

bool ExternalFnDefNode::Equals(const SqlNode *node) const {
    auto other = dynamic_cast<const ExternalFnDefNode *>(node);
    return other != nullptr && other->function_name() == function_name();
}

Status ExternalFnDefNode::Validate(const std::vector<const TypeNode *> &actual_types) const {
    return ValidateArgs(function_name_, actual_types, arg_types_, variadic_pos_);
}

void DynamicUdfFnDefNode::Print(std::ostream &output, const std::string &org_tab) const {
    if (!IsResolved()) {
        output << org_tab << "[Unresolved](" << function_name_ << ")";
    } else {
        output << org_tab << "[kDynamicUdfFnDef] ";
        if (GetReturnType() == nullptr) {
            output << "?";
        } else {
            output << GetReturnType()->GetName();
        }
        output << " " << function_name_ << "(";
        for (size_t i = 0; i < GetArgSize(); ++i) {
            auto arg_ty = GetArgType(i);
            if (arg_ty == nullptr) {
                output << "?";
            } else {
                output << arg_ty->GetName();
            }
            if (i < GetArgSize() - 1) {
                output << ", ";
            }
        }
        output << ")";
        if (return_by_arg_) {
            output << "\n";
            const std::string tab = org_tab + INDENT;
            PrintValue(output, tab, "true", "return_by_arg", true);
        }
    }
}

bool DynamicUdfFnDefNode::Equals(const SqlNode *node) const {
    auto other = dynamic_cast<const DynamicUdfFnDefNode*>(node);
    return other != nullptr && other->GetName() == GetName();
}

Status DynamicUdfFnDefNode::Validate(const std::vector<const TypeNode *> &actual_types) const {
    return ValidateArgs(function_name_, actual_types, arg_types_, -1);
}

void UdfDefNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << "UdfDefNode {\n";
    def_->Print(output, tab + INDENT);
    output << tab << "\n}";
}

bool UdfDefNode::Equals(const SqlNode *node) const {
    auto other = dynamic_cast<const UdfDefNode *>(node);
    return other != nullptr && def_->Equals(other->def_);
}

void UdfByCodeGenDefNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << "[kCodeGenFnDef] " << name_;
}

bool UdfByCodeGenDefNode::Equals(const SqlNode *node) const {
    auto other = dynamic_cast<const UdfByCodeGenDefNode *>(node);
    return other != nullptr && name_ == other->name_ && gen_impl_ == other->gen_impl_;
}

void LambdaNode::Print(std::ostream &output, const std::string &tab) const {
    output << tab << "[kLambda](";
    for (size_t i = 0; i < GetArgSize(); ++i) {
        auto arg = GetArg(i);
        output << arg->GetExprString() << ":";
        if (arg->GetOutputType() == nullptr) {
            output << "?";
        } else {
            output << arg->GetOutputType()->GetName();
        }
        if (i < GetArgSize() - 1) {
            output << ", ";
        }
    }
    output << ")\n";
    body()->Print(output, tab + INDENT);
}

bool LambdaNode::Equals(const SqlNode *node) const {
    auto other = dynamic_cast<const LambdaNode *>(node);
    if (other == nullptr) {
        return false;
    }
    if (this->GetArgSize() != other->GetArgSize()) {
        return false;
    }
    for (size_t i = 0; i < GetArgSize(); ++i) {
        if (ExprEquals(GetArg(i), other->GetArg(i))) {
            return false;
        }
    }
    return ExprEquals(this->body(), other->body());
}

Status LambdaNode::Validate(const std::vector<const TypeNode *> &actual_types) const {
    CHECK_TRUE(actual_types.size() == GetArgSize(), kTypeError, "Lambda expect ", GetArgSize(), " arguments but get ",
               actual_types.size());
    for (size_t i = 0; i < GetArgSize(); ++i) {
        CHECK_TRUE(GetArgType(i) != nullptr, kTypeError);
        if (actual_types[i] == nullptr) {
            continue;
        }
        CHECK_TRUE(GetArgType(i)->Equals(actual_types[i]), kTypeError, "Lambda's", i, "th argument type should be ",
                   GetArgType(i)->GetName(), ", but get ", actual_types[i]->GetName());
    }
    return Status::OK();
}

const TypeNode *UdafDefNode::GetElementType(size_t i) const {
    if (i > arg_types_.size() || arg_types_[i] == nullptr || arg_types_[i]->generics_.size() < 1) {
        return nullptr;
    }
    return arg_types_[i]->generics_[0];
}

bool UdafDefNode::IsElementNullable(size_t i) const {
    if (i > arg_types_.size() || arg_types_[i] == nullptr || arg_types_[i]->generics_.size() < 1) {
        return false;
    }
    return arg_types_[i]->generics_nullable_[0];
}

Status UdafDefNode::Validate(const std::vector<const TypeNode *> &arg_types) const {
    // check non-null fields
    CHECK_TRUE(update_func() != nullptr, kTypeError, "update func is null");
    for (auto ty : arg_types_) {
        CHECK_TRUE(ty != nullptr && ty->base() == kList, kTypeError, "udaf's argument type must be list");
    }
    // init check
    CHECK_TRUE(GetStateType() != nullptr, kTypeError, "State type not inferred");
    if (init_expr() == nullptr) {
        CHECK_TRUE(arg_types_.size() == 1, kTypeError, "Only support single input if init not set");
    } else {
        CHECK_TRUE(init_expr()->GetOutputType() != nullptr, kTypeError)
        CHECK_TRUE(init_expr()->GetOutputType()->Equals(GetStateType()), kTypeError, "Init type expect to be ",
                   GetStateType()->GetName(), ", but get ", init_expr()->GetOutputType()->GetName());
    }
    // update check
    CHECK_TRUE(update_func()->GetArgSize() == 1 + arg_types_.size(), kTypeError, "Update should take ",
               1 + arg_types_.size(), ", get ", update_func()->GetArgSize());
    for (size_t i = 0; i < arg_types_.size() + 1; ++i) {
        auto arg_type = update_func()->GetArgType(i);
        CHECK_TRUE(arg_type != nullptr, kTypeError, i, "th update argument type is not inferred");
        if (i == 0) {
            CHECK_TRUE(arg_type->Equals(GetStateType()), kTypeError, "Update's first argument type should be ",
                       GetStateType()->GetName(), ", but get ", arg_type->GetName());
        } else {
            CHECK_TRUE(arg_type->Equals(GetElementType(i - 1)), kTypeError, "Update's ", i,
                       "th argument type should be ", GetElementType(i - 1), ", but get ", arg_type->GetName());
        }
    }
    // merge check
    if (merge_func() != nullptr) {
        CHECK_TRUE(merge_func()->GetArgSize() == 2, kTypeError, "Merge should take 2 arguments, but get ",
                   merge_func()->GetArgSize());
        CHECK_TRUE(merge_func()->GetArgType(0) != nullptr, kTypeError);
        CHECK_TRUE(merge_func()->GetArgType(0)->Equals(GetStateType()), kTypeError,
                   "Merge's 0th argument type should be ", GetStateType()->GetName(), ", but get ",
                   merge_func()->GetArgType(0)->GetName());
        CHECK_TRUE(merge_func()->GetArgType(1) != nullptr, kTypeError);
        CHECK_TRUE(merge_func()->GetArgType(1)->Equals(GetStateType()), kTypeError,
                   "Merge's 1th argument type should be ", GetStateType(), ", but get ",
                   merge_func()->GetArgType(1)->GetName());
        CHECK_TRUE(merge_func()->GetReturnType() != nullptr, kTypeError);
        CHECK_TRUE(merge_func()->GetReturnType()->Equals(GetStateType()), kTypeError, "Merge's return type should be ",
                   GetStateType(), ", but get ", merge_func()->GetReturnType()->GetName());
    }
    // output check
    if (output_func() != nullptr) {
        CHECK_TRUE(output_func()->GetArgSize() == 1, kTypeError, "Output should take 1 arguments, but get ",
                   output_func()->GetArgSize());
        CHECK_TRUE(output_func()->GetArgType(0) != nullptr, kTypeError);
        CHECK_TRUE(output_func()->GetArgType(0)->Equals(GetStateType()), kTypeError,
                   "Output's 0th argument type should be ", GetStateType(), ", but get ",
                   output_func()->GetArgType(0)->GetName());
        CHECK_TRUE(output_func()->GetReturnType() != nullptr, kTypeError);
    }
    // actual args check
    CHECK_TRUE(arg_types.size() == arg_types_.size(), kTypeError, GetName(), " expect ", arg_types_.size(),
               " inputs, but get ", arg_types.size());
    for (size_t i = 0; i < arg_types.size(); ++i) {
        if (arg_types[i] != nullptr) {
            CHECK_TRUE(arg_types_[i]->Equals(arg_types[i]), kTypeError, GetName(), "'s ", i, "th argument expect ",
                       arg_types_[i]->GetName(), ", but get ", arg_types[i]->GetName());
        }
    }
    return Status::OK();
}

bool UdafDefNode::Equals(const SqlNode *node) const {
    auto other = dynamic_cast<const UdafDefNode *>(node);
    return other != nullptr && init_expr_->Equals(other->init_expr()) && update_->Equals(other->update_) &&
           FnDefEquals(merge_, other->merge_) && FnDefEquals(output_, other->output_);
}

void UdafDefNode::Print(std::ostream &output, const std::string &org_tab) const {
    output << org_tab << "[kUdafFDef] " << name_;
    output << "(";
    for (size_t i = 0; i < GetArgSize(); ++i) {
        if (arg_types_[i] == nullptr) {
            output << "?";
        } else {
            output << arg_types_[i]->GetName();
        }
        if (i < GetArgSize() - 1) {
            output << ", ";
        }
    }
    output << ")\n";
    const std::string tab = org_tab + INDENT;
    PrintSqlNode(output, tab, init_expr_, "init", false);
    output << "\n";
    PrintSqlNode(output, tab, update_, "update", false);
    output << "\n";
    PrintSqlNode(output, tab, merge_, "merge", false);
    output << "\n";
    PrintSqlNode(output, tab, output_, "output", true);
}

void CondExpr::Print(std::ostream &output, const std::string &org_tab) const {
    output << org_tab << "[kCondExpr]"
           << "\n";
    const std::string tab = org_tab + INDENT;
    PrintSqlNode(output, tab, GetCondition(), "condition", false);
    output << "\n";
    PrintSqlNode(output, tab, GetLeft(), "left", false);
    output << "\n";
    PrintSqlNode(output, tab, GetRight(), "right", true);
}

const std::string CondExpr::GetExprString() const {
    std::stringstream ss;
    ss << "cond(";
    ss << (GetCondition() != nullptr ? GetCondition()->GetExprString() : "");
    ss << ", ";
    ss << (GetLeft() != nullptr ? GetLeft()->GetExprString() : "");
    ss << ", ";
    ss << (GetRight() != nullptr ? GetRight()->GetExprString() : "");
    ss << ")";
    return ss.str();
}

bool CondExpr::Equals(const ExprNode *node) const {
    auto other = dynamic_cast<const CondExpr *>(node);
    return other != nullptr && ExprEquals(other->GetCondition(), this->GetCondition()) &&
           ExprEquals(other->GetLeft(), this->GetLeft()) && ExprEquals(other->GetRight(), this->GetRight());
}

ExprNode *CondExpr::GetCondition() const {
    if (GetChildNum() > 0) {
        return GetChild(0);
    } else {
        return nullptr;
    }
}

ExprNode *CondExpr::GetLeft() const {
    if (GetChildNum() > 1) {
        return GetChild(1);
    } else {
        return nullptr;
    }
}

ExprNode *CondExpr::GetRight() const {
    if (GetChildNum() > 2) {
        return GetChild(2);
    } else {
        return nullptr;
    }
}

void PartitionMetaNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, endpoint_, "endpoint", false);
    output << "\n";
    PrintValue(output, tab, RoleTypeName(role_type_), "role_type", true);
}

void ReplicaNumNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(replica_num_), "replica_num", true);
}

void StorageModeNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, StorageModeName(storage_mode_), "storage_mode", true);
}

void CompressTypeNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    if (compress_type_ == CompressType::kSnappy) {
        PrintValue(output, tab, "snappy", "compress_type", true);
    }  else {
        PrintValue(output, tab, "nocompress", "compress_type", true);
    }
}

void PartitionNumNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, std::to_string(partition_num_), "partition_num", true);
}

void DistributionsNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintSqlVector(output, tab, distribution_list_, "distribution_list", true);
}

void CreateSpStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, sp_name_, "sp_name", false);
    output << "\n";
    PrintSqlVector(output, tab, input_parameter_list_, "input_parameter_list", false);
    output << "\n";
    PrintSqlVector(output, tab, inner_node_list_, "inner_node_list", true);
}

void InputParameterNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, column_name_, "column_name", false);
    output << "\n";
    PrintValue(output, tab, DataTypeName(column_type_), "column_type", false);
    output << "\n";
    PrintValue(output, tab, std::to_string(is_constant_), "is_constant", true);
}

void DeleteNode::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, GetTargetString(), "target", false);
    output << "\n";
    if (target_ == DeleteTarget::JOB) {
        PrintValue(output, tab, GetJobId(), "job_id", true);
    } else {
        PrintValue(output, tab, db_name_.empty() ? table_name_ : db_name_ + "." + table_name_, "table_name", false);
        output << "\n";
        PrintSqlNode(output, tab, condition_, "condition", true);
    }
}

std::string DeleteTargetString(DeleteTarget target) {
    switch (target) {
        case DeleteTarget::JOB: {
            return "JOB";
        }
        case DeleteTarget::TABLE: {
            return "TABLE";
        }
    }
    return "unknown";
}

std::string DeleteNode::GetTargetString() const { return DeleteTargetString(target_); }

Status StringToDataType(const std::string identifier, DataType *type) {
    CHECK_TRUE(nullptr != type, common::kNullPointer, "Can't convert type string, output datatype is nullptr")
    const auto lower_identifier = boost::to_lower_copy(identifier);
    auto it = type_map.find(lower_identifier);
    if (it == type_map.end()) {
        return Status(common::kTypeError, "Unknow DataType identifier: " + identifier);
    }

    *type = it->second;
    return Status::OK();
}

void WithClauseEntry::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    const std::string tab = org_tab + INDENT + SPACE_ED;
    output << "\n";
    PrintValue(output, tab, alias_, "alias", false);
    output << "\n";
    PrintSqlNode(output, tab, query_, "query", true);
}

bool WithClauseEntry::Equals(const SqlNode *node) const {
    if (!SqlNode::Equals(node)) {
        return false;
    }

    auto rhs = dynamic_cast<const WithClauseEntry*>(node);
    return rhs != nullptr && alias_ == rhs->alias_ && SqlEquals(this, rhs);
}

void AlterTableStmt::Print(std::ostream &output, const std::string &org_tab) const {
    SqlNode::Print(output, org_tab);
    output << "\n";
    auto tab = org_tab + INDENT;
    PrintValue(output, tab, db_ + "." + table_, "path", false);
    output << "\n";
    output << tab << SPACE_ST << "actions:" << "\n";
    for (decltype(actions_.size()) i = 0; i < actions_.size(); ++i) {
        PrintValue(output, tab + INDENT, actions_[i]->DebugString(), std::to_string(i), false);
        if (i + 1 < actions_.size()) {
            output << "\n";
        }
    }
}

std::string AddPathAction::DebugString() const {
    return absl::Substitute("AddPathAction ($0)", target_);
}

std::string DropPathAction::DebugString() const {
    return absl::Substitute("DropPathAction ($0)", target_);
}

bool SetOperationNode::Equals(const SqlNode *node) const {
    auto *rhs = dynamic_cast<const SetOperationNode *>(node);
    return this->QueryNode::Equals(node) && this->op_type() == rhs->op_type() && this->distinct() == rhs->distinct() &&
           absl::c_equal(this->inputs(), rhs->inputs(),
                         [](const QueryNode *const l, const QueryNode *const r) { return SqlEquals(l, r); });
}
void SetOperationNode::Print(std::ostream &output, const std::string &org_tab) const {
    QueryNode::Print(output, org_tab);
    output << "\n";
    PrintValue(output, org_tab + INDENT, SetOperatorName(op_type_, distinct_), "operator", false);
    output << "\n" << org_tab + INDENT << SPACE_ST << "inputs[list]:";
    for (size_t i = 0; i < inputs().size(); i++) {
        auto node = inputs()[i];
        output << "\n";
        PrintSqlNode(output, org_tab + INDENT + INDENT, node, std::to_string(i), i + 1 == inputs().size());
    }
}
}  // namespace node
}  // namespace hybridse
