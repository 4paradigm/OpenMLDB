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
#include "codegen/context.h"
#include "codegen/date_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/list_ir_builder.h"
#include "codegen/struct_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/udf_ir_builder.h"
#include "codegen/window_ir_builder.h"
#include "glog/logging.h"
#include "proto/fe_common.pb.h"
#include "udf/default_udf_library.h"
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
    if (schemas_context != nullptr) {
        for (auto info : schemas_context->row_schema_info_list_) {
            row_ir_builder_list_.push_back(std::unique_ptr<RowDecodeIRBuilder>(
                new BufNativeIRBuilder(*info.schema_, block, sv_)));
        }
        window_ir_builder_ = std::unique_ptr<WindowDecodeIRBuilder>(
            new MemoryWindowDecodeIRBuilder(
                schemas_context->row_schema_info_list_, block));
    } else {
        window_ir_builder_ = std::unique_ptr<WindowDecodeIRBuilder>(
            new MemoryWindowDecodeIRBuilder(std::vector<vm::RowSchemaInfo>(),
                                            block));
    }
}
ExprIRBuilder::~ExprIRBuilder() {}

// TODO(chenjing): 修改GetFunction, 直接根据参数生成signature
::llvm::Function* ExprIRBuilder::GetFuncion(
    const std::string& name, const std::vector<node::TypeNode>& args_types,
    base::Status& status) {
    std::string fn_name = name;
    if (!args_types.empty()) {
        for (node::TypeNode type_node : args_types) {
            fn_name.append(".").append(type_node.GetName());
        }
    }

    ::llvm::Function* fn = module_->getFunction(::llvm::StringRef(fn_name));
    if (nullptr != fn) {
        return fn;
    }

    if (!args_types.empty() && !args_types[0].generics_.empty()) {
        switch (args_types[0].generics_[0]->base_) {
            case node::kTimestamp:
            case node::kVarchar:
            case node::kDate: {
                fn_name.append(".").append(
                    args_types[0].generics_[0]->GetName());
                fn = module_->getFunction(::llvm::StringRef(fn_name));
                break;
            }
            default: {
            }
        }
    }
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
            status = BuildCallFn(fn, output);
            return status.isOK();
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
                case ::fesql::node::kDate: {
                    auto date_int = builder.getInt32(const_node->GetLong());
                    DateIRBuilder date_builder(module_);
                    ::llvm::Value* date = nullptr;
                    if (date_builder.NewDate(block_, date_int, &date)) {
                        *output = NativeValue::Create(date);
                        return true;
                    } else {
                        return false;
                    }
                }
                case ::fesql::node::kTimestamp: {
                    auto ts_int = builder.getInt64(const_node->GetLong());
                    TimestampIRBuilder date_builder(module_);
                    ::llvm::Value* ts = nullptr;
                    if (date_builder.NewTimestamp(block_, ts_int, &ts)) {
                        *output = NativeValue::Create(ts);
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
            DLOG(INFO) << "id node spec " << id_node->GetExprString();
            if (!id_node->IsResolved()) {
                status.msg = "Detect unresolved expr id: " + id_node->GetName();
                status.code = common::kCodegenError;
                LOG(WARNING) << status.msg;
                return false;
            }
            NativeValue val;
            if (!variable_ir_builder_.LoadValue(id_node->GetExprString(), &val,
                                                status)) {
                status.msg = "fail to find var " + id_node->GetExprString();
                status.code = common::kCodegenError;
                LOG(WARNING) << status.msg;
                return false;
            }
            *output = val;
            return true;
        }
        case ::fesql::node::kExprCast: {
            return BuildCastExpr((::fesql::node::CastExprNode*)node, output,
                                 status);
        }
        case ::fesql::node::kExprBinary: {
            return BuildBinaryExpr((::fesql::node::BinaryExpr*)node, output,
                                   status);
        }
        case ::fesql::node::kExprUnary: {
            return BuildUnaryExpr(
                dynamic_cast<const ::fesql::node::UnaryExpr*>(node), output,
                status);
        }
        case ::fesql::node::kExprStruct: {
            return BuildStructExpr((fesql::node::StructExpr*)node, output,
                                   status);
        }
        case ::fesql::node::kExprGetField: {
            status = BuildGetFieldExpr(
                dynamic_cast<const ::fesql::node::GetFieldExpr*>(node), output);
            if (!status.isOK()) {
                LOG(WARNING) << "Build get field failed: " << status.msg;
                return false;
            }
            return true;
        }
        case ::fesql::node::kExprCond: {
            status = BuildCondExpr(
                dynamic_cast<const ::fesql::node::CondExpr*>(node), output);
            if (!status.isOK()) {
                LOG(WARNING) << "Build cond expr failed: " << status.msg;
                return false;
            }
            return true;
        }
        default: {
            LOG(WARNING) << "Expression Type "
                         << node::ExprTypeName(node->GetExprType())
                         << " not supported";
            return false;
        }
    }
}

Status ExprIRBuilder::BuildCallFn(const ::fesql::node::CallExprNode* call,
                                  NativeValue* output) {
    Status status;
    const node::FnDefNode* fn_def = call->GetFnDef();
    switch (fn_def->GetType()) {
        case node::kExternalFnDef: {
            auto extern_fn =
                dynamic_cast<const node::ExternalFnDefNode*>(fn_def);
            if (!extern_fn->IsResolved()) {
                BuildCallFnLegacy(call, output, status);
                return status;
            }
            break;
        }
        default:
            break;
    }

    ::llvm::IRBuilder<> builder(block_);
    std::vector<NativeValue> arg_values;
    std::vector<const node::TypeNode*> arg_types;

    // TODO(xxx): remove this
    bool is_udaf = false;
    if (call->GetChildNum() > 0) {
        auto first_node_type = call->GetChild(0)->GetOutputType();
        if (first_node_type != nullptr &&
            first_node_type->base_ == node::kList) {
            is_udaf = true;
        }
    }
    ExprIRBuilder sub_builder(block_, sv_, schemas_context_, !is_udaf, module_);
    sub_builder.set_frame(this->frame_);
    for (size_t i = 0; i < call->GetChildNum(); ++i) {
        node::ExprNode* arg_expr = call->GetChild(i);
        NativeValue arg_value;
        CHECK_TRUE(sub_builder.Build(arg_expr, &arg_value, status),
                   "Build argument ", arg_expr->GetExprString(),
                   " failed: ", status.msg);
        arg_values.push_back(arg_value);
        arg_types.push_back(arg_expr->GetOutputType());
    }

    UDFIRBuilder udf_builder(block_, sv_, schemas_context_, module_);
    return udf_builder.BuildCall(fn_def, arg_types, arg_values, output);
}

bool ExprIRBuilder::BuildCallFnLegacy(
    const ::fesql::node::CallExprNode* call_fn, NativeValue* output,
    ::fesql::base::Status& status) {  // NOLINT

    // TODO(chenjing): return status;
    if (call_fn == NULL || output == NULL) {
        status.code = common::kNullPointer;
        status.msg = "call fn or output is null";
        LOG(WARNING) << status.msg;
        return false;
    }
    auto named_fn =
        dynamic_cast<const node::ExternalFnDefNode*>(call_fn->GetFnDef());
    std::string function_name = named_fn->function_name();

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::StringRef name(function_name);

    bool is_udaf = IsUADF(function_name);
    std::vector<::llvm::Value*> llvm_args;
    std::vector<::fesql::node::ExprNode*>::const_iterator it =
        call_fn->children_.cbegin();
    std::vector<::fesql::node::TypeNode> generics_types;
    std::vector<::fesql::node::TypeNode> args_types;

    bool old_mode = row_mode_;
    row_mode_ = !is_udaf;

    for (; it != call_fn->children_.cend(); ++it) {
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
            args_types.push_back(value_type);
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
            oss << "faild to build args: " << *call_fn;
            status.msg = oss.str();
            status.code = common::kCodegenError;
            return false;
        }
    }

    row_mode_ = old_mode;

    ::llvm::Function* fn = GetFuncion(function_name, args_types, status);

    if (common::kOk != status.code) {
        return false;
    }

    if (call_fn->children_.size() == fn->arg_size()) {
        ::llvm::ArrayRef<::llvm::Value*> array_ref(llvm_args);
        *output = NativeValue::Create(
            builder.CreateCall(fn->getFunctionType(), fn, array_ref));
        return true;
    } else if (call_fn->children_.size() == fn->arg_size() - 1) {
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

    auto& llvm_ctx = module_->getContext();
    auto ptr_ty = llvm::Type::getInt8Ty(llvm_ctx)->getPointerTo();
    auto int64_ty = llvm::Type::getInt64Ty(llvm_ctx);
    auto int32_ty = llvm::Type::getInt32Ty(llvm_ctx);
    ::llvm::IRBuilder<> builder(block_);

    const vm::RowSchemaInfo* schema_info = nullptr;
    ok = schemas_context_->ColumnRefResolved(relation_name, col, &schema_info);
    if (!ok) {
        LOG(WARNING) << "Fail to resolve column " << col;
        return false;
    }

    NativeValue input_value;
    if (!sv_->FindVar("@row_ptr", &input_value)) {
        LOG(WARNING) << "Fail to find @row_ptr";
        return false;
    }
    auto row_ptr = input_value.GetValue(&builder);

    auto slice_idx = builder.getInt64(schema_info->idx_);
    auto get_slice_func = module_->getOrInsertFunction(
        "fesql_storage_get_row_slice",
        ::llvm::FunctionType::get(ptr_ty, {ptr_ty, int64_ty}, false));
    auto get_slice_size_func = module_->getOrInsertFunction(
        "fesql_storage_get_row_slice_size",
        ::llvm::FunctionType::get(int64_ty, {ptr_ty, int64_ty}, false));
    ::llvm::Value* slice_ptr =
        builder.CreateCall(get_slice_func, {row_ptr, slice_idx});
    ::llvm::Value* slice_size = builder.CreateIntCast(
        builder.CreateCall(get_slice_size_func, {row_ptr, slice_idx}), int32_ty,
        false);

    BufNativeIRBuilder buf_builder(*schema_info->schema_, block_, sv_);
    if (!buf_builder.BuildGetField(col, slice_ptr, slice_size, output)) {
        return false;
    }

    // TODO(wangtaize) buf ir builder add build get field ptr
    ok = row_ir_builder_list_[schema_info->idx_]->BuildGetField(
        col, slice_ptr, slice_size, &value);
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
bool ExprIRBuilder::BuildWindow(NativeValue* output,
                                ::fesql::base::Status& status) {  // NOLINT
    ::llvm::IRBuilder<> builder(block_);
    NativeValue window_ptr_value;
    const std::string frame_str =
        nullptr == frame_ ? "" : frame_->GetExprString();
    // Load Inner Window If Exist
    bool ok =
        variable_ir_builder_.LoadWindow(frame_str, &window_ptr_value, status);
    ::llvm::Value* window_ptr = nullptr;
    if (!window_ptr_value.IsConstNull()) {
        window_ptr = window_ptr_value.GetValue(&builder);
    }

    if (!ok || nullptr == window_ptr) {
        // Load Big Window, throw error if big window not exist
        ok = variable_ir_builder_.LoadWindow("", &window_ptr_value, status);
        if (!ok || nullptr == window_ptr_value.GetValue(&builder)) {
            status.msg = "fail to find window " + status.msg;
            LOG(WARNING) << status.msg;
            return false;
        }

        // Build Inner Window based on Big Window and frame info
        // ListRef* { int8_t* } -> int8_t**
        ::llvm::Value* list_ref_ptr = window_ptr_value.GetValue(&builder);
        list_ref_ptr = builder.CreatePointerCast(
            list_ref_ptr, builder.getInt8PtrTy()->getPointerTo());
        ::llvm::Value* list_ptr = builder.CreateLoad(list_ref_ptr);

        if (frame_->frame_range() != nullptr) {
            ok = window_ir_builder_->BuildInnerRangeList(
                list_ptr, frame_->GetHistoryRangeEnd(),
                frame_->GetHistoryRangeStart(), &window_ptr);
        } else if (frame_->frame_rows() != nullptr) {
            ok = window_ir_builder_->BuildInnerRowsList(
                list_ptr, -1 * frame_->GetHistoryRowsEnd(),
                -1 * frame_->GetHistoryRowsStart(), &window_ptr);
        }

        // int8_t** -> ListRef* { int8_t* }
        ::llvm::Value* inner_list_ref_ptr =
            builder.CreateAlloca(window_ptr->getType());
        builder.CreateStore(window_ptr, inner_list_ref_ptr);
        window_ptr = builder.CreatePointerCast(inner_list_ref_ptr,
                                               window_ptr->getType());

    } else {
        *output = NativeValue::Create(window_ptr);
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
    } else {
        LOG(INFO) << "store window " << frame_str;
    }
    *output = NativeValue::Create(window_ptr);
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

    NativeValue window;
    if (!BuildWindow(&window, status)) {
        LOG(WARNING) << "fail to build window";
        return false;
    }

    DLOG(INFO) << "get table column " << col;
    // NOT reuse for iterator
    ok = window_ir_builder_->BuildGetCol(col, window.GetRaw(), info->idx_,
                                         &value);

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
        status.msg = "input node or output is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }

    if (node->children_.size() != 1) {
        status.code = common::kCodegenError;
        status.msg = "invalid binary expr node ";
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    DLOG(INFO) << "build unary "
               << ::fesql::node::ExprOpTypeName(node->GetOp());
    NativeValue left_wrapper;
    bool ok = Build(node->children_[0], &left_wrapper, status);
    if (!ok) {
        LOG(WARNING) << "fail to build left node: " << status.msg;
        return false;
    }
    ::llvm::Value* left = left_wrapper.GetValue(&builder);
    llvm::Value* raw = nullptr;
    switch (node->GetOp()) {
        case ::fesql::node::kFnOpNot: {
            ok = predicate_ir_builder_.BuildNotExpr(left, &raw, status);
            break;
        }
        case ::fesql::node::kFnOpMinus: {
            ok = arithmetic_ir_builder_.BuildSubExpr(builder.getInt16(0), left,
                                                     &raw, status);
            break;
        }
        case ::fesql::node::kFnOpBracket: {
            raw = left;
            break;
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

bool ExprIRBuilder::BuildCastExpr(const ::fesql::node::CastExprNode* node,
                                  NativeValue* output, base::Status& status) {
    if (node == NULL || output == NULL) {
        status.msg = "input node or output is null";
        status.code = common::kCodegenError;
        LOG(WARNING) << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    DLOG(INFO) << "build cast expr: " << node::ExprString(node);
    NativeValue left_wrapper;
    bool ok = Build(node->expr(), &left_wrapper, status);
    if (!ok) {
        LOG(WARNING) << "fail to build left node: " << status.msg;
        return false;
    }
    ::llvm::Value* left = left_wrapper.GetValue(&builder);

    CastExprIRBuilder cast_builder(block_);
    ::llvm::Type* cast_type = NULL;
    if (!GetLLVMType(block_->getModule(), node->cast_type_, &cast_type)) {
        status.code = common::kCodegenError;
        status.msg = "fail to cast expr: dist type invalid";
        LOG(WARNING) << status.msg;
        return false;
    }

    ::llvm::Value* dist = NULL;
    if (TypeIRBuilder::IsStructPtr(cast_type)) {
        if (!StructTypeIRBuilder::StructCastFrom(block_, left, cast_type,
                                                 &dist)) {
            status.code = common::kCodegenError;
            status.msg = "fail to cast expr";
            LOG(WARNING) << status.msg;
            return false;
        } else {
            *output = NativeValue::Create(dist);
            return true;
        }
    }
    if (!cast_builder.UnSafeCast(left, cast_type, &dist, status)) {
        return false;
    }
    *output = NativeValue::Create(dist);
    return true;
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
        case ::fesql::node::kFnOpDiv: {
            ok =
                arithmetic_ir_builder_.BuildSDivExpr(left, right, &raw, status);
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
        case ::fesql::node::kFnOpXor: {
            ok = predicate_ir_builder_.BuildXorExpr(left, right, &raw, status);
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

Status ExprIRBuilder::BuildGetFieldExpr(const ::fesql::node::GetFieldExpr* node,
                                        NativeValue* output) {
    // build input
    ::llvm::IRBuilder<> builder(block_);
    Status status;
    NativeValue input_value;
    CHECK_TRUE(this->Build(node->GetRow(), &input_value, status), status.msg);

    auto input_type = node->GetRow()->GetOutputType();
    if (input_type->base() == node::kTuple) {
        CHECK_TRUE(input_value.IsTuple() && input_value.GetFieldNum() ==
                                                input_type->GetGenericSize(),
                   "Illegal input for kTuple, expect ", input_type->GetName());
        try {
            size_t idx = std::stoi(node->GetColumnName());
            CHECK_TRUE(0 <= idx && idx < input_value.GetFieldNum(),
                       "Tuple idx out of range: ", idx);
            *output = input_value.GetField(idx);
        } catch (std::invalid_argument err) {
            return Status(common::kCodegenError,
                          "Invalid Tuple index: " + node->GetColumnName());
        }

    } else if (input_type->base() == node::kRow) {
        auto& llvm_ctx = module_->getContext();
        auto ptr_ty = llvm::Type::getInt8Ty(llvm_ctx)->getPointerTo();
        auto int64_ty = llvm::Type::getInt64Ty(llvm_ctx);
        auto int32_ty = llvm::Type::getInt32Ty(llvm_ctx);

        auto row_type = dynamic_cast<const node::RowTypeNode*>(
            node->GetRow()->GetOutputType());
        vm::SchemasContext schemas_context(row_type->schema_source());

        const vm::RowSchemaInfo* schema_info = nullptr;
        bool ok = schemas_context.ColumnRefResolved(
            node->GetRelationName(), node->GetColumnName(), &schema_info);
        CHECK_TRUE(ok, "Fail to resolve column ", node->GetExprString(),
                   row_type);
        auto row_ptr = input_value.GetValue(&builder);

        auto slice_idx = builder.getInt64(schema_info->idx_);
        auto get_slice_func = module_->getOrInsertFunction(
            "fesql_storage_get_row_slice",
            ::llvm::FunctionType::get(ptr_ty, {ptr_ty, int64_ty}, false));
        auto get_slice_size_func = module_->getOrInsertFunction(
            "fesql_storage_get_row_slice_size",
            ::llvm::FunctionType::get(int64_ty, {ptr_ty, int64_ty}, false));
        ::llvm::Value* slice_ptr =
            builder.CreateCall(get_slice_func, {row_ptr, slice_idx});
        ::llvm::Value* slice_size = builder.CreateIntCast(
            builder.CreateCall(get_slice_size_func, {row_ptr, slice_idx}),
            int32_ty, false);

        BufNativeIRBuilder buf_builder(*schema_info->schema_, block_, sv_);
        CHECK_TRUE(buf_builder.BuildGetField(node->GetColumnName(), slice_ptr,
                                             slice_size, output));
    } else {
        return Status(common::kCodegenError,
                      "Get field's input is neither tuple nor row");
    }
    return Status::OK();
}

Status ExprIRBuilder::BuildCondExpr(const ::fesql::node::CondExpr* node,
                                    NativeValue* output) {
    // build condition
    ::llvm::IRBuilder<> builder(block_);
    Status status;
    NativeValue cond_value;
    CHECK_TRUE(this->Build(node->GetCondition(), &cond_value, status),
               status.msg);

    // build left
    NativeValue left_value;
    CHECK_TRUE(this->Build(node->GetLeft(), &left_value, status), status.msg);

    // build right
    NativeValue right_value;
    CHECK_TRUE(this->Build(node->GetRight(), &right_value, status), status.msg);

    auto raw_cond = cond_value.GetValue(&builder);
    if (cond_value.HasFlag()) {
        raw_cond = builder.CreateAnd(
            raw_cond, builder.CreateNot(cond_value.GetIsNull(&builder)));
    }

    if (left_value.IsTuple()) {
        CHECK_TRUE(right_value.IsTuple() &&
                   left_value.GetFieldNum() == right_value.GetFieldNum());
        std::vector<NativeValue> result_tuple;
        for (size_t i = 0; i < left_value.GetFieldNum(); ++i) {
            NativeValue sub_left = left_value.GetField(i);
            NativeValue sub_right = right_value.GetField(i);
            ::llvm::Value* raw_value =
                builder.CreateSelect(raw_cond, sub_left.GetValue(&builder),
                                     sub_right.GetValue(&builder));

            bool output_nullable = sub_left.HasFlag() || sub_right.HasFlag();
            if (output_nullable) {
                ::llvm::Value* output_is_null =
                    builder.CreateSelect(raw_cond, sub_left.GetIsNull(&builder),
                                         sub_right.GetIsNull(&builder));
                result_tuple.push_back(*output = NativeValue::CreateWithFlag(
                                           raw_value, output_is_null));
            } else {
                result_tuple.push_back(NativeValue::Create(raw_value));
            }
        }
        *output = NativeValue::CreateTuple(result_tuple);
    } else {
        ::llvm::Value* raw_value =
            builder.CreateSelect(raw_cond, left_value.GetValue(&builder),
                                 right_value.GetValue(&builder));

        bool output_nullable = left_value.HasFlag() || right_value.HasFlag();
        if (output_nullable) {
            ::llvm::Value* output_is_null =
                builder.CreateSelect(raw_cond, left_value.GetIsNull(&builder),
                                     right_value.GetIsNull(&builder));
            *output = NativeValue::CreateWithFlag(raw_value, output_is_null);
        } else {
            *output = NativeValue::Create(raw_value);
        }
    }
    return Status::OK();
}

bool ExprIRBuilder::IsUADF(std::string function_name) {
    if (module_->getFunctionList().empty()) {
        return false;
    }

    for (auto iter = module_->getFunctionList().begin();
         iter != module_->getFunctionList().end(); iter++) {
        if (iter->getName().startswith_lower(function_name + ".list")) {
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
