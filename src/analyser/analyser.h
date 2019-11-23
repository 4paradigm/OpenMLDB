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
#include "base/status.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "proto/type.pb.h"

namespace fesql {
namespace analyser {

using base::Status;
using node::ExprNode;
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

    int Analyse(NodePointVector &parser_trees,  // NOLINT (runtime/references)
                NodePointVector &query_tree,    // NOLINT (runtime/references)
                Status &status);                // NOLINT (runtime/references)
    void Analyse(SQLNode *parser_tree,
                 Status &status);  // NOLINT (runtime/references)

    bool IsTableExist(std::string basic_string);
    bool IsColumnExistInTable(const std::string &oolumn_name,
                              const std::string &table_name);
    FuncDefType GetAggFunDefType(node::CallExprNode *func_node);

 private:
    NodeManager *node_manager_;
    std::vector<TableDef *> tables_;
    std::map<std::string, FuncDefType> func_defs;
    std::map<std::string, std::map<std::string, const ColumnDef *>> table_map_;

    void Initialize();
    void TransformSelectNode(node::SelectStmt *parser_node_ptr,
                             Status &status);  // NOLINT (runtime/references)
    void TransformCreateNode(node::CreateStmt *parser_node_ptr,
                             Status &status);  // NOLINT (runtime/references)
    void TransformMultiTableSelectNode(
        node::SelectStmt *parser_tree,
        Status &status);  // NOLINT (runtime/references)
    void TransformSingleTableSelectNode(
        node::SelectStmt *parser_tree,
        Status &status);  // NOLINT (runtime/references)
    void TransformColumnRef(node::ColumnRefNode *node_ptr,
                            const std::string &table_name,
                            Status &status);  // NOLINT (runtime/references)
    void TransformFuncNode(node::CallExprNode *node_ptr,
                           const std::string &table_name,
                           Status &status);  // NOLINT (runtime/references)
    void TransformWindowDef(node::WindowDefNode *node_ptr,
                            const std::string &table_name,
                            Status &status);  // NOLINT (runtime/references)
    void TransformAllRef(node::AllNode *node_ptr,
                         const std::string &relation_name,
                         Status &status);  // NOLINT (runtime/references)
    void TransformPartition(SQLNode *node_ptr, const std::string &table_name,
                            Status &status);  // NOLINT (runtime/references)
    void TransformOrder(SQLNode *node_ptr, const std::string &table_name,
                        Status &status);  // NOLINT (runtime/references)
    void TransformExprNode(SQLNode *node, const std::string &table_name,
                           Status &status);  // NOLINT (runtime/references)
    std::string GenerateName(const std::string prefix, int id);
    void TransformInsertNode(node::InsertStmt *pNode,
                             Status &status);  // NOLINT (runtime/references)
    void TransformCmdNode(node::CmdNode *pNode,
                          Status &status);  // NOLINT (runtime/references)
    void TransformFnDefListNode(node::FnNodeList *node_ptr, Status &status);
};

}  // namespace analyser
}  // namespace fesql
#endif  // SRC_ANALYSER_ANALYSER_H_
