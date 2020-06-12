/*
 * expr_ir_builder.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "codegen/expr_ir_builder.h"
#include <string>
#include <utility>
#include <vector>
#include "codegen/buf_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/list_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/window_ir_builder.h"
#include "glog/logging.h"
#include "proto/fe_common.pb.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace codegen {

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block),
      sv_(scope_var),
      frame_(nullptr),
      row_mode_(true),
      variable_ir_builder_(block, scope_var),
      arithmetic_ir_builder_(block),
      predicate_ir_builder_(block),
      module_(block->getModule()),
      schemas_context_(nullptr) {}

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var,
                             const vm::SchemasContext* schemas_context,
                             const bool row_mode, ::llvm::Module* module)
    : block_(block),
      sv_(scope_var),
      frame_(nullptr),
      row_mode_(row_mode),
      variable_ir_builder_(block, scope_var),
      arithmetic_ir_builder_(block),
      predicate_ir_builder_(block),
      module_(module),
      schemas_context_(schemas_context) {
    for (auto info : schemas_context->row_schema_info_list_) {
        row_ir_builder_list_.push_back(std::unique_ptr<RowDecodeIRBuilder>(
            new BufNativeIRBuilder(*info.schema_, block, sv_)));
    }
    window_ir_builder_ =
        std::unique_ptr<WindowDecodeIRBuilder>(new MemoryWindowDecodeIRBuilder(
            schemas_context->row_schema_info_list_, block));
}
ExprIRBuilder::~ExprIRBuilder() {}

// TODO(chenjing): 修改GetFunction, 直接根据参数生成signature
::llvm::Function* ExprIRBuilder::GetFuncion(
    const std::string& name, const std::vector<node::TypeNode>& generic_types,
    base::Status& status) {
    std::string fn_name = name;
    if (!generic_types.empty()) {
        for (node::TypeNode type_node : generic_types) {
            fn_name.append("_").append(type_node.GetName());
        }
    }

    ::llvm::Function* fn = module_->getFunction(::llvm::StringRef(fn_name));
    if (nullptr == fn) {
        status.code = common::kCallMethodError;
        status.msg = "fail to find func with name " + fn_name;
        LOG(WARNING) << status.msg;
        return fn;
    }
    return fn;
}

bool ExprIRBuilder::Build(const ::fesql::node::ExprNode* node,
                          NativeValue* output,
                          ::fesql::base::Status& status) {  // NOLINT
    if (node == NULL || output == NULL) {
        status.msg = "node or output is null";
        status.code = common::kCodegenError;
        return false;
    }
    DLOG(INFO) << "expr node type "
               << fesql::node::ExprTypeName(node->GetExprType());
    ::llvm::IRBuilder<> builder(block_);
    switch (node->GetExprType()) {
        case ::fesql::node::kExprColumnRef: {
            const ::fesql::node::ColumnRefNode* n =
                (const ::fesql::node::ColumnRefNode*)node;
            return BuildColumnRef(n, output, status);
        }
        case ::fesql::node::kExprCall: {
            const ::fesql::node::CallExprNode* fn =
                (const ::fesql::node::CallExprNode*)node;
            return BuildCallFn(fn, output, status);
        }
        case ::fesql::node::kExprPrimary: {
            ::fesql::node::ConstNode* const_node =
                (::fesql::node::ConstNode*)node;

            switch (const_node->GetDataType()) {
                case ::fesql::node::kInt16:
                    *output = NativeValue::Create(
                        builder.getInt16(const_node->GetSmallInt()));
                    return true;
                case ::fesql::node::kInt32:
                    *output = NativeValue::Create(
                        builder.getInt32(const_node->GetInt()));
                    return true;
                case ::fesql::node::kInt64:
                    *output = NativeValue::Create(
                        builder.getInt64(const_node->GetLong()));
                    return true;
                case ::fesql::node::kFloat: {
                    llvm::Value* raw_float = nullptr;
                    if (GetConstFloat(block_->getContext(),
                                      const_node->GetFloat(), &raw_float)) {
                        *output = NativeValue::Create(raw_float);
                        return true;
                    } else {
                        return false;
                    }
                }
                case ::fesql::node::kDouble: {
                    llvm::Value* raw_double = nullptr;
                    if (GetConstDouble(block_->getContext(),
                                       const_node->GetDouble(), &raw_double)) {
                        *output = NativeValue::Create(raw_double);
                        return true;
                    } else {
                        return false;
                    }
                }
                case ::fesql::node::kVarchar: {
                    std::string val(const_node->GetStr(),
                                    strlen(const_node->GetStr()));
                    llvm::Value* raw_str = nullptr;
                    if (GetConstFeString(val, block_, &raw_str)) {
                        *output = NativeValue::Create(raw_str);
                        return true;
                    } else {
                        return false;
                    }
                }
                default: {
                    status.msg =
                        "fail to codegen primary expression for type: " +
                        node::DataTypeName(const_node->GetDataType());
                    status.code = common::kCodegenError;
                    return false;
                }
            }
        }
        case ::fesql::node::kExprId: {
            ::fesql::node::ExprIdNode* id_node =
                (::fesql::node::ExprIdNode*)node;
            DLOG(INFO) << "id node name " << id_node->GetName();
            NativeValue val;
            if (!variable_ir_builder_.LoadValue(id_node->GetName(), &val,
                                                status) ||
                val.GetRaw() == nullptr) {
                status.msg = "fail to find var " + id_node->GetName();
                status.code = common::kCodegenError;
                LOG(WARNING) << status.msg;
                return false;
            }
            *output = val;
            return true;
        }
        case ::fesql::node::kExprBinary: {
            return BuildBinaryExpr((::fesql::node::BinaryExpr*)node, output,
                                   status);
        }
        case ::fesql::node::kExprUnary: {
            return Build(node->children_[0], output, status);
        }
        case ::fesql::node::kExprStruct: {
            return BuildStructExpr((fesql::node::StructExpr*)node, output,
                                   status);
        }
        default: {
            LOG(WARNING) << "Expression Type "
                         << node::ExprTypeName(node->GetExprType())
                         << " not supported";
            return false;
        }
    }
}

bool ExprIRBuilder::BuildCallFn(const ::fesql::node::CallExprNode* call_fn,
                                NativeValue* output,
                                ::fesql::base::Status& status) {  // NOLINT
    // TODO(chenjing): return status;
    if (call_fn == NULL || output == NULL) {
        status.code = common::kNullPointer;
        status.msg = "call fn or output is null";
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::StringRef name(call_fn->GetFunctionName());

    bool is_udaf = IsUADF(call_fn->GetFunctionName());
    std::vector<::llvm::Value*> llvm_args;
    const fesql::node::ExprListNode* args = call_fn->GetArgs();
    std::vector<::fesql::node::ExprNode*>::const_iterator it =
        args->children_.cbegin();
    std::vector<::fesql::node::TypeNode> generics_types;

    bool old_mode = row_mode_;
    row_mode_ = !is_udaf;

    for (; it != args->children_.cend(); ++it) {
        const ::fesql::node::ExprNode* arg = dynamic_cast<node::ExprNode*>(*it);
        NativeValue llvm_arg_wrapper;
        // TODO(chenjing): remove out_name
        if (Build(arg, &llvm_arg_wrapper, status)) {
            ::fesql::node::TypeNode value_type;
            if (false == GetFullType(llvm_arg_wrapper.GetType(), &value_type)) {
                status.msg = "fail to handle arg type ";
                status.code = common::kCodegenError;
                return false;
            }
            // TODO(chenjing): 直接使用list TypeNode
            // handle list type
            // 泛型类型还需要优化，目前是hard
            // code识别list或者迭代器类型，然后取generic type
            if (fesql::node::kList == value_type.base_ ||
                fesql::node::kIterator == value_type.base_) {
                generics_types.push_back(value_type);
            }
            ::llvm::Value* llvm_arg = llvm_arg_wrapper.GetValue(&builder);
            llvm_args.push_back(llvm_arg);
        } else {
            LOG(WARNING) << "fail to build arg for " << status.msg;
            std::ostringstream oss;
            oss << "faild to build args: " << *arg;
            status.msg = oss.str();
            status.code = common::kCodegenError;
            return false;
        }
    }

    row_mode_ = old_mode;

    ::llvm::Function* fn =
        GetFuncion(call_fn->GetFunctionName(), generics_types, status);

    if (common::kOk != status.code) {
        return false;
    }

    if (args->children_.size() == fn->arg_size()) {
        ::llvm::ArrayRef<::llvm::Value*> array_ref(llvm_args);
        *output = NativeValue::Create(
            builder.CreateCall(fn->getFunctionType(), fn, array_ref));
        return true;
    } else if (args->children_.size() == fn->arg_size() - 1) {
        auto it = fn->arg_end();
        it--;
        ::llvm::Argument* last_arg = &*it;
        if (!TypeIRBuilder::IsStructPtr(last_arg->getType())) {
            status.msg = ("Incorrect arguments passed");
            status.code = (common::kCallMethodError);
            LOG(WARNING) << status.msg;
            return false;
        }
        ::llvm::Type* struct_type =
            reinterpret_cast<::llvm::PointerType*>(last_arg->getType())
                ->getElementType();
        ::llvm::Value* struct_value = builder.CreateAlloca(struct_type);
        llvm_args.push_back(struct_value);
        ::llvm::Value* ret =
            builder.CreateCall(fn->getFunctionType(), fn,
                               ::llvm::ArrayRef<::llvm::Value*>(llvm_args));
        if (nullptr == ret) {
            status.code = common::kCallMethodError;
            status.msg = "Fail to Call Function";
            LOG(WARNING) << status.msg;
            return false;
        }
        *output = NativeValue::Create(struct_value);
        return true;

    } else {
        status.msg = ("Incorrect arguments passed");
        status.code = (common::kCallMethodError);
        LOG(WARNING) << status.msg;
        return false;
    }
    return false;
}

// Build Struct Expr IR:
// TODO(chenjing): support method memeber
// @param node
// @param output
// @return
bool ExprIRBuilder::BuildStructExpr(const ::fesql::node::StructExpr* node,
                                    NativeValue* output,
                                    base::Status& status) {  // NOLINT
    std::vector<::llvm::Type*> members;
    if (nullptr != node->GetFileds() && !node->GetFileds()->children.empty()) {
        for (auto each : node->GetFileds()->children) {
            node::FnParaNode* field = dynamic_cast<node::FnParaNode*>(each);
            ::llvm::Type* type = nullptr;
            if (ConvertFeSQLType2LLVMType(field->GetParaType(), module_,
                                          &type)) {
                members.push_back(type);
            } else {
                std::ostringstream oss;
                oss << "Invalid struct with unacceptable field type: "
                    << (field->GetParaType());
                status.msg = oss.str();
                status.code = common::kCodegenError;
                return false;
            }
        }
    }
    ::llvm::StringRef name(node->GetName());
    ::llvm::StructType* llvm_struct =
        ::llvm::StructType::create(module_->getContext(), name);
    ::llvm::ArrayRef<::llvm::Type*> array_ref(members);
    llvm_struct->setBody(array_ref);
    *output = NativeValue::Create((::llvm::Value*)llvm_struct);
    return true;
}

bool ExprIRBuilder::BuildColumnRef(const ::fesql::node::ColumnRefNode* node,
                                   NativeValue* output,
                                   base::Status& status) {  // NOLINT
    if (node == NULL || output == NULL) {
        status.msg = "column ref node is null";
        status.code = common::kCodegenError;
        return false;
    }

    if (row_mode_) {
        return BuildColumnItem(node->GetRelationName(), node->GetColumnName(),
                               output, status);
    } else {
        llvm::Value* iter = nullptr;
        if (BuildColumnIterator(node->GetRelationName(), node->GetColumnName(),
                                &iter, status)) {
            *output = NativeValue::Create(iter);
            return true;
        } else {
            return false;
        }
    }
}

bool ExprIRBuilder::BuildColumnItem(const std::string& relation_name,
                                    const std::string& col, NativeValue* output,
                                    base::Status& status) {
    const RowSchemaInfo* info;
    if (!FindRowSchemaInfo(relation_name, col, &info)) {
        status.msg =
            "fail to find row context with " + relation_name + "." + col;
        status.code = common::kCodegenError;
        return false;
    }

    NativeValue value;
    DLOG(INFO) << "get table column " << col;
    // not found
    bool ok = variable_ir_builder_.LoadColumnItem(info->table_name_, col,
                                                  &value, status);
    if (ok) {
        *output = value;
        return true;
    }

    ::llvm::Value* row_ptr = NULL;
    if (!variable_ir_builder_.LoadArrayIndex("@row_ptrs", info->idx_, &row_ptr,
                                             status) ||
        row_ptr == NULL) {
        std::ostringstream oss;
        oss << "fail to find row_ptrs"
            << "[" << info->idx_ << "]" << status.msg;
        status.msg = oss.str();
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::Value* row_size = NULL;
    if (!variable_ir_builder_.LoadArrayIndex("@row_sizes", info->idx_,
                                             &row_size, status) ||
        row_size == NULL) {
        std::ostringstream oss;
        oss << "fail to find row_sizes"
            << "[" << info->idx_ << "]"
            << ": " << status.msg;
        status.msg = oss.str();
        LOG(WARNING) << status.msg;
        return false;
    }

    // TODO(wangtaize) buf ir builder add build get field ptr
    ok = row_ir_builder_list_[info->idx_]->BuildGetField(col, row_ptr, row_size,
                                                         &value);
    if (!ok || value.GetRaw() == nullptr) {
        status.msg = "fail to find column " + col;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    ok = variable_ir_builder_.StoreColumnItem(info->table_name_, col, value,
                                              status);
    if (ok) {
        *output = value;
        status.msg = "ok";
        status.code = common::kOk;
        return true;
    } else {
        return false;
    }
}

// Get inner window with given frame
bool ExprIRBuilder::BuildWindow(const node::FrameNode* frame_node,
                                ::llvm::Value** output,
                                ::fesql::base::Status& status) {  // NOLINT
    ::llvm::IRBuilder<> builder(block_);
    NativeValue window_ptr_value;
    const std::string frame_str =
        nullptr == frame_node ? "" : frame_node->GetExprString();
    // Load Inner Window If Exist
    bool ok =
        variable_ir_builder_.LoadWindow(frame_str, &window_ptr_value, status);
    ::llvm::Value* window_ptr = window_ptr_value.GetValue(&builder);

    if (!ok || nullptr == window_ptr) {
        // Load Big Window, throw error if big window not exist
        ok = variable_ir_builder_.LoadWindow("", &window_ptr_value, status);
        if (!ok || nullptr == window_ptr_value.GetValue(&builder)) {
            status.msg = "fail to find window " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }

        // Build Inner Window based on Big Window and frame info
        ok = window_ir_builder_->BuildInnerRangeList(
            window_ptr_value.GetValue(&builder), frame_node->GetRangeEnd(),
            frame_node->GetRangeStart(), &window_ptr);
    } else {
        *output = window_ptr;
        return true;
    }

    if (!ok || nullptr == window_ptr) {
        status.msg = "fail to build inner window " + frame_str;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (!variable_ir_builder_.StoreWindow(frame_str, window_ptr, status)) {
        LOG(WARNING) << "fail to store window " << frame_str << ": "
                     << status.msg;
        return false;
    }
    *output = window_ptr;
    return true;
}
// Get col with given col name, set list struct pointer into output
// param col
// param output
// return
bool ExprIRBuilder::BuildColumnIterator(const std::string& relation_name,
                                        const std::string& col,
                                        ::llvm::Value** output,
                                        base::Status& status) {  // NOLINT
    const RowSchemaInfo* info;
    if (!FindRowSchemaInfo(relation_name, col, &info)) {
        status.msg = "fail to find context with " + relation_name + "." + col;
        status.code = common::kCodegenError;
        return false;
    }

    ::llvm::Value* value = NULL;
    const std::string frame_str =
        nullptr == frame_ ? "" : frame_->GetExprString();
    DLOG(INFO) << "get table column " << col;
    // not found
    bool ok = variable_ir_builder_.LoadColumnRef(info->table_name_, col,
                                                 frame_str, &value, status);
    if (ok) {
        *output = value;
        return true;
    }

    ::llvm::Value* window_ptr = NULL;
    if (!BuildWindow(frame_, &window_ptr, status)) {
        LOG(WARNING) << "fail to build window";
        return false;
    }

    DLOG(INFO) << "get table column " << col;
    // NOT reuse for iterator
    ok = window_ir_builder_->BuildGetCol(col, window_ptr, info->idx_, &value);

    if (!ok || value == NULL) {
        status.msg = "fail to find column " + col;
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ok = variable_ir_builder_.StoreColumnRef(info->table_name_, col, frame_str,
                                             value, status);
    if (ok) {
        *output = value;
        status.msg = "ok";
        status.code = common::kOk;
        return true;
    } else {
        LOG(WARNING) << "fail to store col for " << status.msg;
        return false;
    }
}

bool ExprIRBuilder::BuildUnaryExpr(const ::fesql::node::UnaryExpr* node,
                                   NativeValue* output,
                                   base::Status& status) {  // NOLINT
    if (node == NULL || output == NULL) {
        status.code = common::kCodegenError;
        status.msg = "input node or output is null";
        LOG(WARNING) << status.msg;
        return false;
    }

    if (node->children_.size() != 1) {
        status.code = common::kCodegenError;
        status.msg = "invalid unary expr node ";
        LOG(WARNING) << status.msg;
        return false;
    }

    DLOG(INFO) << "build unary"
               << ::fesql::node::ExprTypeName(node->GetExprType());
    NativeValue left;
    bool ok = Build(node->children_[0], &left, status);
    if (!ok) {
        status.code = common::kCodegenError;
        status.msg = "fail to build unary child";
        LOG(WARNING) << status.msg;
        return false;
    }
    LOG(WARNING) << "can't support unary yet";
    return false;
}

bool ExprIRBuilder::BuildBinaryExpr(const ::fesql::node::BinaryExpr* node,
                                    NativeValue* output, base::Status& status) {
    if (node == NULL || output == NULL) {
        status.msg = "input node or output is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (node->children_.size() != 2) {
        status.code = common::kCodegenError;
        status.msg = "invalid binary expr node ";
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    DLOG(INFO) << "build binary "
               << ::fesql::node::ExprOpTypeName(node->GetOp());
    NativeValue left_wrapper;
    bool ok = Build(node->children_[0], &left_wrapper, status);
    if (!ok) {
        LOG(WARNING) << "fail to build left node: " << status.msg;
        return false;
    }
    ::llvm::Value* left = left_wrapper.GetValue(&builder);

    NativeValue right_wrapper;
    ok = Build(node->children_[1], &right_wrapper, status);
    if (!ok) {
        LOG(WARNING) << "fail to build right node" << status.msg;
        return false;
    }
    ::llvm::Value* right = right_wrapper.GetValue(&builder);

    // TODO(wangtaize) type check
    llvm::Value* raw = nullptr;
    switch (node->GetOp()) {
        case ::fesql::node::kFnOpAdd: {
            ok = arithmetic_ir_builder_.BuildAddExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpMulti: {
            ok = arithmetic_ir_builder_.BuildMultiExpr(left, right, &raw,
                                                       status);
            break;
        }
        case ::fesql::node::kFnOpFDiv: {
            ok =
                arithmetic_ir_builder_.BuildFDivExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpMinus: {
            ok = arithmetic_ir_builder_.BuildSubExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpMod: {
            ok = arithmetic_ir_builder_.BuildModExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpAnd: {
            ok = predicate_ir_builder_.BuildAndExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpOr: {
            ok = predicate_ir_builder_.BuildOrExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpEq: {
            ok = predicate_ir_builder_.BuildEqExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpNeq: {
            ok = predicate_ir_builder_.BuildNeqExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpGt: {
            ok = predicate_ir_builder_.BuildGtExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpGe: {
            ok = predicate_ir_builder_.BuildGeExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpLt: {
            ok = predicate_ir_builder_.BuildLtExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpLe: {
            ok = predicate_ir_builder_.BuildLeExpr(left, right, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpAt: {
            fesql::node::DataType left_type;
            if (false == GetBaseType(left->getType(), &left_type)) {
                status.code = common::kCodegenError;
                status.msg =
                    "fail to codegen var[pos] expression: var type invalid";
            }
            switch (left_type) {
                case fesql::node::kList: {
                    ::llvm::Value* at_value = nullptr;
                    ListIRBuilder list_ir_builder(block_, sv_);
                    if (false == list_ir_builder.BuildAt(left, right, &at_value,
                                                         status)) {
                        return false;
                    }
                    *output = NativeValue::Create(at_value);
                    return true;
                }
                default: {
                    status.code = common::kCodegenError;
                    status.msg =
                        "fail to codegen var[pos] expression: var type "
                        "can't "
                        "support []";
                    return false;
                }
            }
        }
        default: {
            ok = false;
            status.msg = "invalid op " + ExprOpTypeName(node->GetOp());
            status.code = ::fesql::common::kCodegenError;
            LOG(WARNING) << status.msg;
        }
    }

    if (!ok || raw == nullptr) {
        LOG(WARNING) << "fail to codegen binary expression: " << status.msg;
        return false;
    }
    *output = NativeValue::Create(raw);
    return true;
}
bool ExprIRBuilder::IsUADF(std::string function_name) {
    if (module_->getFunctionList().empty()) {
        return false;
    }

    for (auto iter = module_->getFunctionList().begin();
         iter != module_->getFunctionList().end(); iter++) {
        if (iter->getName().startswith_lower(function_name + "_list")) {
            return true;
        }
    }
    return false;
}
bool ExprIRBuilder::FindRowSchemaInfo(const std::string& relation_name,
                                      const std::string& col_name,
                                      const RowSchemaInfo** info) {
    if (nullptr == schemas_context_) {
        LOG(WARNING)
            << "fail to find row schema info with null schemas context";
        return false;
    }
    return schemas_context_->ColumnRefResolved(relation_name, col_name, info);
}

}  // namespace codegen
}  // namespace fesql
