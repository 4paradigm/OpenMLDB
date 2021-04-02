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

#ifndef SRC_CODEGEN_EXPR_IR_BUILDER_H_
#define SRC_CODEGEN_EXPR_IR_BUILDER_H_

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

    Status BuildColumnRef(const ::hybridse::node::ColumnRefNode* node,
                          NativeValue* output);

    Status BuildCallFn(const ::hybridse::node::CallExprNode* fn,
                       NativeValue* output);

    bool BuildCallFnLegacy(const ::hybridse::node::CallExprNode* call_fn,
                           NativeValue* output,
                           ::hybridse::base::Status& status);  // NOLINT

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

    ::llvm::Function* GetFuncion(
        const std::string& col,
        const std::vector<const node::TypeNode*>& generic_types,
        base::Status& status);  // NOLINT

 private:
    CodeGenContext* ctx_;
    const node::FrameNode* frame_ = nullptr;
    node::ExprNode* frame_arg_ = nullptr;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // SRC_CODEGEN_EXPR_IR_BUILDER_H_
