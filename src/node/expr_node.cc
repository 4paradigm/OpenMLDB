/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * expr_node.cc
 *
 * Author: chenjing
 * Date: 2019/10/11
 *--------------------------------------------------------------------------
 **/

#include "node/expr_node.h"
#include "codec/fe_row_codec.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "vm/schemas_context.h"
#include "vm/transform.h"

namespace fesql {
namespace node {

std::atomic<int64_t> ExprIdNode::expr_id_cnt_(0);

Status ColumnRefNode::InferAttr(ExprAnalysisContext* ctx) {
    auto schemas_context = ctx->schemas_context();
    CHECK_TRUE(schemas_context != nullptr, "No schema context provided");

    const vm::RowSchemaInfo* schema_info = nullptr;
    bool ok = schemas_context->ColumnRefResolved(relation_name_, column_name_,
                                                 &schema_info);
    CHECK_TRUE(ok, "Fail to resolve column ", GetExprString());

    codec::RowDecoder decoder(*schema_info->schema_);
    codec::ColInfo col_info;
    CHECK_TRUE(decoder.ResolveColumn(column_name_, &col_info),
               "Fail to resolve column ", GetExprString());

    node::DataType dtype;
    CHECK_TRUE(vm::SchemaType2DataType(col_info.type, &dtype),
               "Fail to convert type: ", col_info.type);

    auto nm = ctx->node_manager();
    if (ctx->is_multi_row()) {
        SetOutputType(nm->MakeTypeNode(node::kList, dtype));
    } else {
        SetOutputType(nm->MakeTypeNode(dtype));
    }
    return Status::OK();
}

Status ConstNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(ctx->node_manager()->MakeTypeNode(data_type_));
    return Status::OK();
}

Status CallExprNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(GetFnDef()->GetReturnType());
    return Status::OK();
}

Status ExprIdNode::InferAttr(ExprAnalysisContext* ctx) {
    // var node should be bind outside
    return Status::OK();
}

Status CastExprNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(ctx->node_manager()->MakeTypeNode(cast_type_));
    return Status::OK();
}

Status GetFieldExpr::InferAttr(ExprAnalysisContext* ctx) {
    auto row_type = dynamic_cast<const RowTypeNode*>(GetRow()->GetOutputType());
    CHECK_TRUE(row_type != nullptr, "Get field's input is not row");
    vm::SchemasContext schemas_context(row_type->schema_source());

    const vm::RowSchemaInfo* schema_info = nullptr;
    bool ok = schemas_context.ColumnRefResolved(relation_name_, column_name_,
                                                &schema_info);
    CHECK_TRUE(ok, "Fail to resolve column ", GetExprString());

    codec::RowDecoder decoder(*schema_info->schema_);
    codec::ColInfo col_info;
    CHECK_TRUE(decoder.ResolveColumn(column_name_, &col_info),
               "Fail to resolve column ", GetExprString());

    node::DataType dtype;
    CHECK_TRUE(vm::SchemaType2DataType(col_info.type, &dtype),
               "Fail to convert type: ", col_info.type);

    auto nm = ctx->node_manager();
    SetOutputType(nm->MakeTypeNode(dtype));
    return Status::OK();
}

}  // namespace node
}  // namespace fesql
