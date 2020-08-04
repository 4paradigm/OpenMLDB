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
    return Status(common::kCodegenError,
                  "ColumnRef should be transformed out before infer pass");
}

Status ConstNode::InferAttr(ExprAnalysisContext* ctx) {
    SetOutputType(ctx->node_manager()->MakeTypeNode(data_type_));
    SetNullable(false);
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
    auto input_type = GetRow()->GetOutputType();
    if (input_type->base() == node::kTuple) {
        try {
            int idx = std::stoi(this->GetColumnName());
            CHECK_TRUE(0 <= idx && idx < input_type->GetGenericSize(),
                       "Tuple idx out of range: ", idx);
            SetOutputType(input_type->GetGenericType(idx));
            SetNullable(input_type->IsGenericNullable(idx));
        } catch (std::invalid_argument err) {
            return Status(common::kCodegenError,
                          "Invalid Tuple index: " + this->GetColumnName());
        }

    } else if (input_type->base() == node::kRow) {
        auto row_type = dynamic_cast<const RowTypeNode*>(input_type);
        vm::SchemasContext schemas_context(row_type->schema_source());

        const vm::RowSchemaInfo* schema_info = nullptr;
        bool ok = schemas_context.ColumnRefResolved(relation_name_,
                                                    column_name_, &schema_info);
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
    } else {
        return Status(common::kCodegenError,
                      "Get field's input is neither tuple nor row");
    }
    return Status::OK();
}

}  // namespace node
}  // namespace fesql
