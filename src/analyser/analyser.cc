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
                           NodePointVector &query_trees) {

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

        query_trees.push_back(query_tree);
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
    node::TableNode *table_ref = (node::TableNode *) (parser_tree->GetTableRefList().at(0));

    if (nullptr == table_ref) {
        LOG(WARNING) << "can not transform select node when table reference is null";
        return error::kAnalyserErrorTableRefIsNull;
    }

    if (false == IsTableExist(table_ref->GetOrgTableName())) {
        LOG(WARNING) << "can not query select when table " << table_ref->GetOrgTableName() << " is not exist in db";
        return error::kAnalyserErrorTableNotExist;
    }

    for (auto node : parser_tree->GetSelectList()) {

        node::ResTarget *target = (node::ResTarget *) node;
        int ret = 0;
        switch (target->GetVal()->GetType()) {
            case node::kColumnRef: {
                ret = TransformColumnRef((node::ColumnRefNode *) target->GetVal(), table_ref->GetOrgTableName());
                break;
            }
            case node::kFunc: {
                ret = TransformFuncNode((node::FuncNode *) target->GetVal(), table_ref->GetOrgTableName());
                break;
            }
            case node::kAll: {
                ret = TransformAllRef((node::AllNode *) target->GetVal(), table_ref->GetOrgTableName());
                break;
            }
            default: {}

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

bool FeSQLAnalyser::IsColumnExistInTable(const std::string &column_name, const std::string &table_name) {
    if (table_map_.find(table_name) == table_map_.end()) {
        return false;
    }

    auto map = table_map_[table_name];
    return map.find(column_name) != map.end();
}

int FeSQLAnalyser::TransformFuncNode(node::FuncNode *node_ptr, const std::string &table_name) {

    // TODO: 细化参数校验
    // TODO: 表达式节点修改：需要带上DataType属性
    for (int i = 0; i < node_ptr->GetArgs().size(); ++i) {
        int ret = TransformExprNode(node_ptr->GetArgs()[i], table_name);
        if (0 != ret) {
            LOG(WARNING) << "can not transfrom func node when dealing with " << i + 1 << "th parameter";
        }
    }

    // TODO:

    // TODO: add function signature validate
    switch (GetAggFunDefType(node_ptr)) {
        case kFuncTypeUnknow:LOG(WARNING) << "function '" << node_ptr->GetFunctionName() << "' is undefined";
            return error::kAnalyserErrorGlobalAggFunction;
        case kFuncTypeAgg:node_ptr->SetAgg(true);
            break;
        case kFuncTypeScalar:node_ptr->SetAgg(false);
            break;
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
int FeSQLAnalyser::TransformColumnRef(node::ColumnRefNode *node_ptr, const std::string &table_name) {
    if (node_ptr->GetColumnName().empty()) {
        LOG(WARNING) << "can not query select when column is empty";
        return error::kAnalyserErrorColumnNameIsEmpty;
    }

    if (node_ptr->GetRelationName().empty()) {
        node_ptr->SetRelationName(table_name);
    }
    if (false == IsColumnExistInTable(node_ptr->GetColumnName(), node_ptr->GetRelationName())) {
        LOG(WARNING) << "can not query select when column " << node_ptr->GetColumnName() << " is not exit in table "
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
    for (auto table: tables_) {
        if (table_map_.find(table->name()) != table_map_.end()) {
            LOG(WARNING) << "error occur when initialize tables: table duplicate in db";
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
int FeSQLAnalyser::TransformAllRef(node::AllNode *node_ptr, const std::string &relation_name) {
    if (node_ptr->GetRelationName().empty()) {
        node_ptr->SetRelationName(relation_name);
        return 0;
    }

    if (node_ptr->GetRelationName() == relation_name) {
        return 0;
    }

    LOG(WARNING) << "can not query " << node_ptr->GetRelationName() << ".* from table " << relation_name;
    return error::kAnalyserErrorTableNotExist;
}

FuncDefType FeSQLAnalyser::GetAggFunDefType(node::FuncNode *node_ptr) {

    if (func_defs.find(node_ptr->GetFunctionName()) == func_defs.end()) {
        return kFuncTypeUnknow;
    }
    return func_defs[node_ptr->GetFunctionName()];
}

int FeSQLAnalyser::TransformWindowDef(node::WindowDefNode *node_ptr, const std::string &table_name) {

    // TODO: window is exist
    // TODO: window is redefined
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

int FeSQLAnalyser::TransformOrder(SQLNode *node_ptr, const std::string &table_name) {
    return 0;
}

int FeSQLAnalyser::TransformPartition(SQLNode *node_ptr, const std::string &table_name) {
    return 0;
}

int FeSQLAnalyser::TransformExprNode(SQLNode *node_ptr, const std::string &table_name) {
    switch (node_ptr->GetType()) {
        case node::kColumnRef:return TransformColumnRef((node::ColumnRefNode *) node_ptr, table_name);
        case node::kFunc: return TransformFuncNode((node::FuncNode *) node_ptr, table_name);
        case node::kPrimary:break;
        case node::kExpr: break;
    }
    return 0;
}

}
}
