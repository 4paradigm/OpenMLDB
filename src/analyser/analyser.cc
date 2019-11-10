/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * analyser.cc
 *
 * Author: chenjing
 * Date: 2019/10/30
 *--------------------------------------------------------------------------
 **/
#include "analyser/analyser.h"
#include <string>
#include "../node/node_enum.h"
namespace fesql {
namespace analyser {

int FeSQLAnalyser::Analyse(NodePointVector &parser_trees,
                           NodePointVector &query_trees) {
  if (parser_trees.empty()) {
    return error::kAnalyserErrorParserTreeEmpty;
  }

  for (auto tree : parser_trees) {
    int ret = Analyse(tree);
    if (0 != ret) {
      LOG(WARNING) << "fail to analyser parser tree";
      return ret;
    }

    query_trees.push_back(tree);
  }
  return 0;
}

int FeSQLAnalyser::Analyse(SQLNode *parser_tree) {
  if (nullptr == parser_tree) {
    return error::kPlanErrorNullNode;
  }
  int ret = 0;
  switch (parser_tree->GetType()) {
    case node::kSelectStmt:
      ret = TransformSelectNode((node::SelectStmt *)parser_tree);
      break;
    case node::kCreateStmt:
      ret = TransformCreateNode((node::CreateStmt *)parser_tree);
      break;
    default: {
      LOG(WARNING) << "can not support "
                   << node::NameOfSQLNodeType(parser_tree->GetType());
      ret = error::kAnalyserErrorSQLTypeNotSupport;
    }
  }

  return ret;
}

int FeSQLAnalyser::TransformMultiTableSelectNode(
    node::SelectStmt *parser_tree) {
  LOG(WARNING) << "can not support select query on multi tables";
  return error::kAnalyserErrorQueryMultiTable;
}

int FeSQLAnalyser::TransformSelectNode(node::SelectStmt *parser_tree) {
  if (parser_tree->GetTableRefList().empty()) {
    LOG(WARNING) << "can not transform select node when table references (from "
                    "table list) is empty";
    return error::kAnalyserErrorFromListEmpty;
  }

  if (parser_tree->GetTableRefList().size() == 1) {
    return TransformSingleTableSelectNode(parser_tree);
  } else {
    return TransformMultiTableSelectNode(parser_tree);
  }
}
int FeSQLAnalyser::TransformSingleTableSelectNode(
    node::SelectStmt *parser_tree) {
  node::TableNode *table_ref =
      (node::TableNode *)(parser_tree->GetTableRefList().at(0));

  if (nullptr == table_ref) {
    LOG(WARNING)
        << "can not transform select node when table reference is null";
    return error::kAnalyserErrorTableRefIsNull;
  }

  if (false == IsTableExist(table_ref->GetOrgTableName())) {
    LOG(WARNING) << "can not query select when table "
                 << table_ref->GetOrgTableName() << " is not exist in db";
    return error::kAnalyserErrorTableNotExist;
  }

  for (auto node : parser_tree->GetSelectList()) {
    node::ResTarget *target = (node::ResTarget *)node;
    int ret = 0;
    switch (target->GetVal()->GetType()) {
      case node::kColumnRef: {
        ret = TransformColumnRef((node::ColumnRefNode *)target->GetVal(),
                                 table_ref->GetOrgTableName());
        break;
      }
      case node::kFunc: {
        ret = TransformFuncNode((node::FuncNode *)target->GetVal(),
                                table_ref->GetOrgTableName());
        break;
      }
      case node::kAll: {
        ret = TransformAllRef((node::AllNode *)target->GetVal(),
                              table_ref->GetOrgTableName());
        break;
      }
      default: {
        LOG(WARNING) << "SELECT error: can not handle "
                     << node::NameOfSQLNodeType(target->GetVal()->GetType());
        return error::kAnalyserErrorSQLTypeNotSupport;
      }
    }
    if (0 != ret) {
      return ret;
    }
  }

  return 0;
}

bool FeSQLAnalyser::IsTableExist(std::string basic_string) {
  return table_map_.find(basic_string) != table_map_.end();
}

bool FeSQLAnalyser::IsColumnExistInTable(const std::string &column_name,
                                         const std::string &table_name) {
  if (table_map_.find(table_name) == table_map_.end()) {
    return false;
  }

  auto map = table_map_[table_name];
  return map.find(column_name) != map.end();
}

int FeSQLAnalyser::TransformFuncNode(node::FuncNode *node_ptr,
                                     const std::string &table_name) {
  // TODO(chenjing): 细化参数校验
  // TODO(chenjing): 表达式节点修改：需要带上DataType属性
  for (int i = 0; i < static_cast<int>(node_ptr->GetArgs().size()); ++i) {
    int ret = TransformExprNode(node_ptr->GetArgs()[i], table_name);
    if (0 != ret) {
      LOG(WARNING) << "can not transfrom func node when dealing with " << i + 1
                   << "th parameter";
    }
  }
  // TODO(chenjing): add function signature validate
  FuncDefType func_type = GetAggFunDefType(node_ptr);
  switch (func_type) {
    case kFuncTypeUnknow:
      LOG(WARNING) << "function '" << node_ptr->GetFunctionName()
                   << "' is undefined";
      return error::kAnalyserErrorGlobalAggFunction;
    case kFuncTypeAgg:
      node_ptr->SetAgg(true);
      break;
    case kFuncTypeScalar:
      node_ptr->SetAgg(false);
      break;
    default: {
      LOG(WARNING) << "FUNCTION error: can not hanlde " << func_type;
    }
  }

  if (nullptr == node_ptr->GetOver() && node_ptr->GetIsAgg()) {
    LOG(WARNING) << "can not apply agg function without 'over' window";
    return error::kAnalyserErrorGlobalAggFunction;
  }

  if (nullptr != node_ptr->GetOver()) {
    TransformWindowDef(node_ptr->GetOver(), table_name);
  }

  return 0;
}
int FeSQLAnalyser::TransformColumnRef(node::ColumnRefNode *node_ptr,
                                      const std::string &table_name) {
  if (node_ptr->GetColumnName().empty()) {
    LOG(WARNING) << "can not query select when column is empty";
    return error::kAnalyserErrorColumnNameIsEmpty;
  }

  if (node_ptr->GetRelationName().empty()) {
    node_ptr->SetRelationName(table_name);
  }
  if (false == IsColumnExistInTable(node_ptr->GetColumnName(),
                                    node_ptr->GetRelationName())) {
    LOG(WARNING) << "can not query select when column "
                 << node_ptr->GetColumnName() << " is not exit in table "
                 << node_ptr->GetRelationName();
    return error::kAnalyserErrorColumnNotExist;
  }

  return 0;
}

int FeSQLAnalyser::Initialize() {
  func_defs["SUBSTR"] = kFuncTypeScalar;
  func_defs["TRIM"] = kFuncTypeScalar;
  func_defs["COUNT"] = kFuncTypeAgg;
  func_defs["SUM"] = kFuncTypeAgg;
  func_defs["AVG"] = kFuncTypeAgg;
  func_defs["MIN"] = kFuncTypeAgg;
  func_defs["MAX"] = kFuncTypeAgg;

  if (tables_.empty()) {
    return 0;
  }
  table_map_.clear();
  for (auto table : tables_) {
    if (table_map_.find(table->name()) != table_map_.end()) {
      LOG(WARNING)
          << "error occur when initialize tables: table duplicate in db";
      table_map_.clear();
      return error::kAnalyserErrorInitialize;
    }

    std::map<std::string, const ColumnDef *> column_map;
    for (int i = 0; i < table->columns().size(); ++i) {
      column_map[table->columns(i).name()] = &(table->columns(i));
    }
    table_map_[table->name()] = column_map;
  }
  return 0;
}
int FeSQLAnalyser::TransformAllRef(node::AllNode *node_ptr,
                                   const std::string &relation_name) {
  if (node_ptr->GetRelationName().empty()) {
    node_ptr->SetRelationName(relation_name);
    return 0;
  }

  if (node_ptr->GetRelationName() == relation_name) {
    return 0;
  }

  LOG(WARNING) << "can not query " << node_ptr->GetRelationName()
               << ".* from table " << relation_name;
  return error::kAnalyserErrorTableNotExist;
}

FuncDefType FeSQLAnalyser::GetAggFunDefType(node::FuncNode *node_ptr) {
  if (func_defs.find(node_ptr->GetFunctionName()) == func_defs.end()) {
    return kFuncTypeUnknow;
  }
  return func_defs[node_ptr->GetFunctionName()];
}

int FeSQLAnalyser::TransformWindowDef(node::WindowDefNode *node_ptr,
                                      const std::string &table_name) {
  // TODO(chenjing): window is exist
  // TODO(chenjing): window is redefined
  for (auto partition : node_ptr->GetPartitions()) {
    int ret = TransformPartition(partition, table_name);
    if (ret != 0) {
      return ret;
    }
  }

  for (auto order : node_ptr->GetOrders()) {
    int ret = TransformOrder(order, table_name);
    if (ret != 0) {
      return ret;
    }
  }

  return 0;
}

int FeSQLAnalyser::TransformOrder(SQLNode *node_ptr,
                                  const std::string &table_name) {
  return 0;
}

int FeSQLAnalyser::TransformPartition(SQLNode *node_ptr,
                                      const std::string &table_name) {
  return 0;
}

int FeSQLAnalyser::TransformExprNode(SQLNode *node_ptr,
                                     const std::string &table_name) {
  switch (node_ptr->GetType()) {
    case node::kColumnRef:
      return TransformColumnRef((node::ColumnRefNode *)node_ptr, table_name);
    case node::kFunc:
      return TransformFuncNode((node::FuncNode *)node_ptr, table_name);
    case node::kPrimary:
      return error::kAnalyserErrorSQLTypeNotSupport;
      break;
    case node::kExpr:
      break;
      return error::kAnalyserErrorSQLTypeNotSupport;
    default: {
      return error::kAnalyserErrorSQLTypeNotSupport;
    }
  }
  return 0;
}

int FeSQLAnalyser::TransformCreateNode(node::CreateStmt *parser_node_ptr) {
  if (!parser_node_ptr->GetOpIfNotExist() &&
      IsTableExist(parser_node_ptr->GetTableName())) {
    LOG(WARNING) << "CREATE TABLE " << parser_node_ptr->GetTableName()
                 << "ALREADY EXISTS";
    return error::kAnalyserErrorTableAlreadyExist;
  }

  return 0;
}

}  // namespace analyser
}  // namespace fesql
