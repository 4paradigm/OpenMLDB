/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_CODEGEN_EXPR_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_EXPR_IR_BUILDER_H_

#include <map>
#include <memory>
#include <string>
#include <vector>
#include "base/fe_status.h"
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/predicate_expr_ir_builder.h"
#include "codegen/row_ir_builder.h"
#include "codegen/scope_var.h"
#include "codegen/variable_ir_builder.h"
#include "codegen/window_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "node/type_node.h"
#include "passes/resolve_fn_and_attrs.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace codegen {

using hybridse::base::Status;

class ExprIRBuilder {
 public:
    explicit ExprIRBuilder(CodeGenContext* ctx);
    ~ExprIRBuilder();

    Status Build(const ::hybridse::node::ExprNode* node, NativeValue* output);

    Status BuildAsUdf(const node::ExprNode* expr, const std::string& name,
                      const std::vector<NativeValue>& args,
                      NativeValue* output);

    Status BuildWindow(NativeValue* output);

    inline void set_frame(node::ExprNode* frame_arg,
                          const node::FrameNode* frame) {
        this->frame_arg_ = frame_arg;
        this->frame_ = frame;
    }

 private:
    Status BuildConstExpr(const ::hybridse::node::ConstNode* node,
                          NativeValue* output);
    Status BuildParameterExpr(const ::hybridse::node::ParameterExpr* node,
                              NativeValue* output);
    Status BuildColumnRef(const ::hybridse::node::ColumnRefNode* node,
                          NativeValue* output);


    Status BuildCallFn(const ::hybridse::node::CallExprNode* fn,
                       NativeValue* output);

    Status BuildCallFnLegacy(const ::hybridse::node::CallExprNode* call_fn,
                           NativeValue* output);

    Status BuildCastExpr(const ::hybridse::node::CastExprNode* node,
                         NativeValue* output);

    Status BuildBinaryExpr(const ::hybridse::node::BinaryExpr* node,
                           NativeValue* output);

    Status BuildUnaryExpr(const ::hybridse::node::UnaryExpr* node,
                          NativeValue* output);

    Status BuildStructExpr(const ::hybridse::node::StructExpr* node,
                           NativeValue* output);

    Status BuildGetFieldExpr(const ::hybridse::node::GetFieldExpr* node,
                             NativeValue* output);

    Status BuildCaseExpr(const ::hybridse::node::CaseWhenExprNode* node,
                         NativeValue* output);

    Status BuildCondExpr(const ::hybridse::node::CondExpr* node,
                         NativeValue* output);

    Status BuildBetweenExpr(const ::hybridse::node::BetweenExpr* node, NativeValue* output);

    Status BuildInExpr(const ::hybridse::node::InExpr* node, NativeValue* output);

    Status BuildExprList(const ::hybridse::node::ExprListNode* node, NativeValue* output);

    Status BuildLikeExprAsUdf(const ::hybridse::node::BinaryExpr* node, const std::string& name, const NativeValue& lhs,
                              const NativeValue& rhs, NativeValue* output);

    Status BuildEscapeExpr(const ::hybridse::node::EscapedExpr* node, NativeValue* output);

    Status BuildRLikeExprAsUdf(const ::hybridse::node::BinaryExpr* node, const std::string& name,
                               const NativeValue& lhs, const NativeValue& rhs, NativeValue* output);

    Status ExtractSliceFromRow(const NativeValue& input_value, const int schema_idx, ::llvm::Value** slice_ptr,
                               ::llvm::Value** slice_size);
    Status GetFunction(const std::string& col, const std::vector<const node::TypeNode*>& generic_types,
                      ::llvm::Function** output);

    Status BuildArrayExpr(const ::hybridse::node::ArrayExpr* node, NativeValue* output);

 private:
    CodeGenContext* ctx_;

    // window frame node
    const node::FrameNode* frame_ = nullptr;

    // ExprIdNode definition of frame
    node::ExprNode* frame_arg_ = nullptr;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_EXPR_IR_BUILDER_H_
