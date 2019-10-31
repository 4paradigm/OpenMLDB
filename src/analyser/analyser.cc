/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * analyser.cc
 *      
 * Author: chenjing
 * Date: 2019/10/30 
 *--------------------------------------------------------------------------
**/
#include "analyser.h"
namespace fesql {
namespace analyser {

int FeSQLAnalyser::Analyse(NodePointVector &parser_trees,
                           NodePointVector &query_tree) {

    Initialize();
    if (parser_trees.empty()) {
        return error::kAnalyserErrorParserTreeEmpty;
    }

    for (auto tree : parser_trees) {
        SQLNode *query_tree = nullptr;
        switch (tree->GetType()) {
            case node::kSelectStmt:query_tree = node_manager_->MakeSQLNode(node::kSelectStmt);
                break;
            default:LOG(WARNING) << "can not analyser parser tree with type "
                                 << node::NameOfSQLNodeType(tree->GetType());
                return error::kAnalyserErrorSQLTypeNotSupport;
        }
        if (nullptr == query_tree) {
            LOG(WARNING) << "can not analyser parser tree";
            return error::kNodeErrorMakeNodeFail;
        }

        int ret = Analyse(tree, query_tree);
        if (0 != ret) {
            // TODO: print tree info if in debug mode
            LOG(WARNING) << "fail to analyser parser tree";
            return ret;
        }
    }
    return 0;
}

int FeSQLAnalyser::Analyse(SQLNode *parser_tree, SQLNode *query_tree) {
    int ret = 0;
    switch (parser_tree->GetType()) {
        case node::kSelectStmt:
            ret = TransformSelectNode((node::SelectStmt *) parser_tree,
                                      (node::SelectStmt *) query_tree);
            break;
        default:ret = error::kAnalyserErrorSQLTypeNotSupport;
    }
    return ret;
}

int FeSQLAnalyser::TransformMultiTableSelectNode(node::SelectStmt *parser_tree,
                                                 node::SelectStmt *query_tree) {
    LOG(WARNING) << "can not support select query on multi tables";
    return error::kAnalyserErrorQueryMultiTable;
}

int FeSQLAnalyser::TransformSelectNode(node::SelectStmt *parser_tree,
                                       node::SelectStmt *query_tree) {
    if (parser_tree->GetTableRefList().empty()) {
        LOG(WARNING) << "can not transform select node when table references (from table list) is empty";
        return error::kAnalyserErrorFromListEmpty;
    }

    if (parser_tree->GetTableRefList().size() == 1) {
        return TransformSingleTableSelectNode(parser_tree, query_tree);
    } else {
        return TransformMultiTableSelectNode(parser_tree, query_tree);
    }
}
int FeSQLAnalyser::TransformSingleTableSelectNode(node::SelectStmt *parser_tree, node::SelectStmt *query_tree) {
    int ret = 0;
    node::TableNode *table_ref = (node::TableNode *) (parser_tree->GetTableRefList().at(0));

    if (nullptr == table_ref) {
        LOG(WARNING) << "can not transform select node when table reference is null";
        return error::kAnalyserErrorTableRefIsNull;
    }

    if (false == IsTableExist(table_ref->GetOrgTableName())) {
        LOG(WARNING) << "can not query select when table " << table_ref->GetOrgTableName() << " is not exist in db";
        return error::kAnalyserErrorTableNotExist;
    }

    return ret;
}
bool FeSQLAnalyser::IsTableExist(std::string basic_string) {
    return table_map_.find(basic_string) != table_map_.end();
}

int FeSQLAnalyser::Initialize() {
    if (tables_.empty()) {
        return 0;
    }
    table_map_.clear();
    for (auto table: tables_) {
        if (table_map_.find(table->name()) != table_map_.end()) {
            LOG(WARNING) << "error occur when initialize tables: table duplicate in db";
            table_map_.clear();
            return error::kAnalyserErrorInitialize;
        }
        table_map_[table->name()] = table;
    }
    return 0;
}

}
}