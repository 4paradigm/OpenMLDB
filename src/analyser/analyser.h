/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * analyser.h
 *
 * Author: chenjing
 * Date: 2019/10/30
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_ANALYSER_ANALYSER_H_
#define SRC_ANALYSER_ANALYSER_H_

#include <time.h>
#include <map>
#include <set>
#include <string>
#include <vector>
#include "node/node_manager.h"
#include "node/sql_node.h"

#include "proto/type.pb.h"

namespace fesql {
namespace analyser {

using node::NodeManager;
using node::NodePointVector;
using node::SQLNode;
using type::ColumnDef;
using type::IndexDef;
using type::TableDef;

enum FuncDefType {
  kFuncTypeUnknow = 1,
  kFuncTypeScalar,
  kFuncTypeAgg,
};
class FeSQLAnalyser {
 public:
  explicit FeSQLAnalyser(NodeManager *manager) : node_manager_(manager) {}

  FeSQLAnalyser(NodeManager *manager, TableDef *table)
      : node_manager_(manager) {
    tables_.clear();
    tables_.push_back(table);
    Initialize();
  }

  FeSQLAnalyser(NodeManager *manager, const std::vector<TableDef *> &tables)
      : node_manager_(manager) {
    tables_ = tables;
    Initialize();
  }

  int Analyse(NodePointVector &parser_trees, NodePointVector &query_tree);
  int Analyse(SQLNode *parser_tree);

  bool IsTableExist(std::string basic_string);
  bool IsColumnExistInTable(const std::string &oolumn_name,
                            const std::string &table_name);
  FuncDefType GetAggFunDefType(node::FuncNode *func_node);

  int transformTableDef(std::string table_name,
                        NodePointVector &column_desc_list, TableDef *table);

 private:
  NodeManager *node_manager_;
  std::vector<TableDef *> tables_;
  std::map<std::string, FuncDefType> func_defs;
  std::map<std::string, std::map<std::string, const ColumnDef *>> table_map_;

  int Initialize();
  int TransformSelectNode(node::SelectStmt *parser_node_ptr);
  int TransformCreateNode(node::CreateStmt *parser_node_ptr);
  int TransformMultiTableSelectNode(node::SelectStmt *parser_tree);
  int TransformSingleTableSelectNode(node::SelectStmt *parser_tree);
  int TransformColumnRef(node::ColumnRefNode *node_ptr,
                         const std::string &table_name);
  int TransformFuncNode(node::FuncNode *node_ptr,
                        const std::string &table_name);
  int TransformWindowDef(node::WindowDefNode *node_ptr,
                         const std::string &table_name);
  int TransformAllRef(node::AllNode *node_ptr,
                      const std::string &relation_name);
  int TransformPartition(SQLNode *node_ptr, const std::string &table_name);
  int TransformOrder(SQLNode *node_ptr, const std::string &table_name);
  int TransformExprNode(SQLNode *node, const std::string &table_name);
  std::string GenerateName(const std::string prefix, int id);
};

}  // namespace analyser
}  // namespace fesql
#endif  // SRC_ANALYSER_ANALYSER_H_
