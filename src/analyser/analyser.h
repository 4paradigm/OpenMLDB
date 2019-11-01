/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * analyser.h
 *      
 * Author: chenjing
 * Date: 2019/10/30 
 *--------------------------------------------------------------------------
**/

#ifndef FESQL_ANALYSER_H
#define FESQL_ANALYSER_H

#include "node/node_manager.h"
#include "node/sql_node.h"
#include <map>

#include "proto/type.pb.h"

namespace fesql {
namespace analyser {


using type::TableDef;
using node::NodePointVector;
using node::SQLNode;
using node::NodeManager;
typedef

class FeSQLAnalyser {
public:
    FeSQLAnalyser(NodeManager *manager) : node_manager_(manager) {
    }

    FeSQLAnalyser(NodeManager *manager, TableDef* table) : node_manager_(manager) {
        tables_.clear();
        tables_.push_back(table);
    }

    FeSQLAnalyser(NodeManager *manager, std::vector<TableDef*> &tables) : node_manager_(manager) {
        tables_ = tables;
    }

    int Analyse(NodePointVector &parser_trees, NodePointVector &query_tree);
    int Analyse(SQLNode *parser_tree, SQLNode *query_tree);

private:
    NodeManager *node_manager_;
    std::vector<TableDef*>  tables_;
    std::map<std::string, TableDef*> table_map_;

    int TransformSelectNode(node::SelectStmt *parser_node_ptr, node::SelectStmt *query_node_ptr);
    int TransformMultiTableSelectNode(node::SelectStmt *parser_tree, node::SelectStmt *query_tree);
    int TransformSingleTableSelectNode(node::SelectStmt *parser_tree, node::SelectStmt *query_tree);
    bool IsTableExist(std::string basic_string);
    int Initialize();
};

}
}
#endif //FESQL_ANALYSER_H
