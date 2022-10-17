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

#include "codegen/expr_ir_builder.h"

#include <string>
#include <utility>
#include <vector>

#include "base/numeric.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/cond_select_ir_builder.h"
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

namespace hybridse {
namespace codegen {

using ::hybridse::common::kCodegenError;

ExprIRBuilder::ExprIRBuilder(CodeGenContext* ctx) : ctx_(ctx) {}

ExprIRBuilder::~ExprIRBuilder() {}

// TODO(chenjing): 修改GetFunction, 直接根据参数生成signature
base::Status ExprIRBuilder::GetFunction(const std::string& name, const std::vector<const node::TypeNode*>& args_types,
                                       ::llvm::Function** output) {
    std::string fn_name = name;
    if (!args_types.empty()) {
        for (const node::TypeNode* type_node : args_types) {
            fn_name.append(".").append(type_node->GetName());
        }
    }

    auto module = ctx_->GetModule();
    ::llvm::Function* fn = module->getFunction(::llvm::StringRef(fn_name));
    if (nullptr != fn) {
        *output = fn;
        return base::Status::OK();
    }

    if (!args_types.empty() && !args_types[0]->generics_.empty()) {
        switch (args_types[0]->generics_[0]->base_) {
            case node::kTimestamp:
            case node::kVarchar:
            case node::kDate: {
                fn_name.append(".").append(
                    args_types[0]->generics_[0]->GetName());
                fn = module->getFunction(::llvm::StringRef(fn_name));
                break;
            }
            default: {
            }
        }
    }
    *output = fn;
    CHECK_TRUE(nullptr != fn, common::kCodegenCallFunctionError, "Fail to find function named ", fn_name)
    return base::Status::OK();
}

Status ExprIRBuilder::Build(const ::hybridse::node::ExprNode* node,
                            NativeValue* output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kCodegenError,
               "Node or output is null");
    std::string cache_key = "@expr(#" + std::to_string(node->node_id()) + ")";
    if (frame_ != nullptr) {
        cache_key.append(" over " + frame_->GetExprString());
    }
    if (ctx_->GetCurrentScope()->sv()->FindVar(cache_key, output)) {
        return Status::OK();
    }
    if (node->GetOutputType() == nullptr) {
        LOG(WARNING) << node->GetExprString() << " not fully resolved";
    }
    switch (node->GetExprType()) {
        case ::hybridse::node::kExprColumnRef: {
            const ::hybridse::node::ColumnRefNode* n =
                dynamic_cast<const ::hybridse::node::ColumnRefNode*>(node);
            CHECK_STATUS(BuildColumnRef(n, output));
            break;
        }
        case ::hybridse::node::kExprCall: {
            const ::hybridse::node::CallExprNode* fn =
                dynamic_cast<const ::hybridse::node::CallExprNode*>(node);
            CHECK_STATUS(BuildCallFn(fn, output));
            break;
        }
        case ::hybridse::node::kExprParameter: {
            auto parameter_expr = dynamic_cast<const ::hybridse::node::ParameterExpr*>(node);
            CHECK_STATUS(BuildParameterExpr(parameter_expr, output));
            break;
        }
        case ::hybridse::node::kExprPrimary: {
            auto const_node =
                dynamic_cast<const ::hybridse::node::ConstNode*>(node);
            CHECK_STATUS(BuildConstExpr(const_node, output));
            break;
        }
        case ::hybridse::node::kExprId: {
            auto id_node =
                dynamic_cast<const ::hybridse::node::ExprIdNode*>(node);
            DLOG(INFO) << "id node spec " << id_node->GetExprString();
            CHECK_TRUE(id_node->IsResolved(), kCodegenError,
                       "Detect unresolved expr id: " + id_node->GetName());
            NativeValue val;
            VariableIRBuilder variable_ir_builder(
                ctx_->GetCurrentBlock(), ctx_->GetCurrentScope()->sv());
            Status status;
            CHECK_TRUE(variable_ir_builder.LoadValue(id_node->GetExprString(),
                                                     &val, status),
                       kCodegenError, "Fail to find var ",
                       id_node->GetExprString());
            *output = val;
            break;
        }
        case ::hybridse::node::kExprCast: {
            CHECK_STATUS(
                BuildCastExpr((::hybridse::node::CastExprNode*)node, output));
            break;
        }
        case ::hybridse::node::kExprBinary: {
            CHECK_STATUS(
                BuildBinaryExpr((::hybridse::node::BinaryExpr*)node, output));
            break;
        }
        case ::hybridse::node::kExprUnary: {
            CHECK_STATUS(BuildUnaryExpr(
                dynamic_cast<const ::hybridse::node::UnaryExpr*>(node),
                output));
            break;
        }
        case ::hybridse::node::kExprStruct: {
            CHECK_STATUS(
                BuildStructExpr((hybridse::node::StructExpr*)node, output));
            break;
        }
        case ::hybridse::node::kExprGetField: {
            CHECK_STATUS(BuildGetFieldExpr(
                dynamic_cast<const ::hybridse::node::GetFieldExpr*>(node),
                output));
            break;
        }
        case ::hybridse::node::kExprCond: {
            CHECK_STATUS(BuildCondExpr(
                dynamic_cast<const ::hybridse::node::CondExpr*>(node), output));
            break;
        }
        case ::hybridse::node::kExprCase: {
            CHECK_STATUS(BuildCaseExpr(
                dynamic_cast<const ::hybridse::node::CaseWhenExprNode*>(node),
                output));
            break;
        }
        case ::hybridse::node::kExprBetween: {
            CHECK_STATUS(BuildBetweenExpr(
                dynamic_cast<const ::hybridse::node::BetweenExpr*>(node),
                output));
            break;
        }
        case ::hybridse::node::kExprIn: {
            CHECK_STATUS(BuildInExpr(
                dynamic_cast<const ::hybridse::node::InExpr*>(node),
                output));
            break;
        }
        case ::hybridse::node::kExprList: {
            CHECK_STATUS(BuildExprList(dynamic_cast<const ::hybridse::node::ExprListNode*>(node), output));
            break;
        }
        case ::hybridse::node::kExprEscaped: {
            CHECK_STATUS(BuildEscapeExpr(dynamic_cast<const ::hybridse::node::EscapedExpr*>(node), output));
            break;
        }
        default: {
            return Status(kCodegenError,
                          "Expression Type " +
                              node::ExprTypeName(node->GetExprType()) +
                              " not supported");
        }
    }
    ctx_->GetCurrentScope()->sv()->AddVar(cache_key, *output);
    return Status::OK();
}

Status ExprIRBuilder::BuildConstExpr(
    const ::hybridse::node::ConstNode* const_node, NativeValue* output) {
    ::llvm::IRBuilder<> builder(ctx_->GetCurrentBlock());
    switch (const_node->GetDataType()) {
        case ::hybridse::node::kNull: {
            *output = NativeValue::CreateNull(
                llvm::Type::getTokenTy(builder.getContext()));
            break;
        }
        case ::hybridse::node::kBool: {
            *output = NativeValue::Create(
                builder.getInt1(const_node->GetBool() ? 1 : 0));
            break;
        }
        case ::hybridse::node::kInt16: {
            *output = NativeValue::Create(
                builder.getInt16(const_node->GetSmallInt()));
            break;
        }
        case ::hybridse::node::kInt32: {
            *output =
                NativeValue::Create(builder.getInt32(const_node->GetInt()));
            break;
        }
        case ::hybridse::node::kInt64: {
            *output =
                NativeValue::Create(builder.getInt64(const_node->GetLong()));
            break;
        }
        case ::hybridse::node::kFloat: {
            llvm::Value* raw_float = nullptr;
            CHECK_TRUE(GetConstFloat(ctx_->GetLLVMContext(),
                                     const_node->GetFloat(), &raw_float),
                       kCodegenError);

            *output = NativeValue::Create(raw_float);
            break;
        }
        case ::hybridse::node::kDouble: {
            llvm::Value* raw_double = nullptr;
            CHECK_TRUE(GetConstDouble(ctx_->GetLLVMContext(),
                                      const_node->GetDouble(), &raw_double),
                       kCodegenError);

            *output = NativeValue::Create(raw_double);
            break;
        }
        case ::hybridse::node::kVarchar: {
            std::string val(const_node->GetStr(), strlen(const_node->GetStr()));
            llvm::Value* raw_str = nullptr;
            CHECK_TRUE(GetConstFeString(val, ctx_->GetCurrentBlock(), &raw_str),
                       kCodegenError);
            *output = NativeValue::Create(raw_str);
            break;
        }
        case ::hybridse::node::kDate: {
            auto date_int = builder.getInt32(const_node->GetLong());
            DateIRBuilder date_builder(ctx_->GetModule());
            ::llvm::Value* date = nullptr;
            CHECK_TRUE(
                date_builder.NewDate(ctx_->GetCurrentBlock(), date_int, &date),
                kCodegenError);
            *output = NativeValue::Create(date);
            break;
        }
        case ::hybridse::node::kTimestamp: {
            auto ts_int = builder.getInt64(const_node->GetLong());
            TimestampIRBuilder date_builder(ctx_->GetModule());
            ::llvm::Value* ts = nullptr;
            CHECK_TRUE(
                date_builder.NewTimestamp(ctx_->GetCurrentBlock(), ts_int, &ts),
                kCodegenError);
            *output = NativeValue::Create(ts);
            break;
        }
        default: {
            return Status(kCodegenError,
                          "Fail to codegen primary expression for type: " +
                              node::DataTypeName(const_node->GetDataType()));
        }
    }
    return Status::OK();
}

Status ExprIRBuilder::BuildCallFn(const ::hybridse::node::CallExprNode* call,
                                  NativeValue* output) {
    const node::FnDefNode* fn_def = call->GetFnDef();
    if (fn_def->GetType() == node::kExternalFnDef) {
        auto extern_fn = dynamic_cast<const node::ExternalFnDefNode*>(fn_def);
        if (!extern_fn->IsResolved()) {
            CHECK_STATUS(BuildCallFnLegacy(call, output))
        }
    }

    std::vector<NativeValue> arg_values;
    std::vector<const node::TypeNode*> arg_types;
    ExprIRBuilder sub_builder(ctx_);
    sub_builder.set_frame(this->frame_arg_, this->frame_);
    for (size_t i = 0; i < call->GetChildNum(); ++i) {
        node::ExprNode* arg_expr = call->GetChild(i);
        NativeValue arg_value;
        CHECK_STATUS(sub_builder.Build(arg_expr, &arg_value), "Build argument ",
                     arg_expr->GetExprString(), " failed");
        arg_values.push_back(arg_value);
        arg_types.push_back(arg_expr->GetOutputType());
    }
    UdfIRBuilder udf_builder(ctx_, frame_arg_, frame_);
    CHECK_STATUS(udf_builder.BuildCall(fn_def, arg_types, arg_values, output));
    return base::Status::OK();
}

Status ExprIRBuilder::BuildCallFnLegacy(
    const ::hybridse::node::CallExprNode* call_fn, NativeValue* output) {

    // TODO(chenjing): return status;
    CHECK_TRUE(call_fn != NULL, common::kCodegenError, "BuildCallFnLegacy#call_fn is null")
    CHECK_TRUE(output != NULL, common::kCodegenError, "BuildCallFnLegacy#output is null")
    auto named_fn =
        dynamic_cast<const node::ExternalFnDefNode*>(call_fn->GetFnDef());
    std::string function_name = named_fn->function_name();

    ::llvm::BasicBlock* block = ctx_->GetCurrentBlock();
    ::llvm::IRBuilder<> builder(block);
    ::llvm::StringRef name(function_name);

    std::vector<::llvm::Value*> llvm_args;
    std::vector<::hybridse::node::ExprNode*>::const_iterator it =
        call_fn->children_.cbegin();
    std::vector<const ::hybridse::node::TypeNode*> generics_types;
    std::vector<const ::hybridse::node::TypeNode*> args_types;

    for (; it != call_fn->children_.cend(); ++it) {
        const ::hybridse::node::ExprNode* arg =
            dynamic_cast<node::ExprNode*>(*it);
        NativeValue llvm_arg_wrapper;
        // TODO(chenjing): remove out_name
        CHECK_STATUS(Build(arg, &llvm_arg_wrapper), "Fail to build arguments")
        const ::hybridse::node::TypeNode* value_type = nullptr;
        CHECK_TRUE(GetFullType(ctx_->node_manager(), llvm_arg_wrapper.GetType(), &value_type), kCodegenError,
                   "Fail to handler argument type")

        args_types.push_back(value_type);
        // TODO(chenjing): 直接使用list TypeNode
        // handle list type
        // 泛型类型还需要优化，目前是hard
        // code识别list或者迭代器类型，然后取generic type
        if (hybridse::node::kList == value_type->base() ||
            hybridse::node::kIterator == value_type->base()) {
            generics_types.push_back(value_type);
        }
        ::llvm::Value* llvm_arg = llvm_arg_wrapper.GetValue(&builder);
        llvm_args.push_back(llvm_arg);
    }

    ::llvm::Function* fn = nullptr;
    CHECK_STATUS(GetFunction(function_name, args_types, &fn));
    if (call_fn->children_.size() == fn->arg_size()) {
        ::llvm::ArrayRef<::llvm::Value*> array_ref(llvm_args);
        *output = NativeValue::Create(
            builder.CreateCall(fn->getFunctionType(), fn, array_ref));
        return base::Status::OK();
    } else if (call_fn->children_.size() == fn->arg_size() - 1) {
        auto it = fn->arg_end();
        it--;
        ::llvm::Argument* last_arg = &*it;
        CHECK_TRUE(TypeIRBuilder::IsStructPtr(last_arg->getType()), common::kCodegenCallFunctionError,
                   "Incorrect arguments passed")
        ::llvm::Type* struct_type = reinterpret_cast<::llvm::PointerType*>(last_arg->getType())
                ->getElementType();
        ::llvm::Value* struct_value =
            CreateAllocaAtHead(&builder, struct_type, "struct_alloca");
        llvm_args.push_back(struct_value);
        ::llvm::Value* ret =
            builder.CreateCall(fn->getFunctionType(), fn,
                               ::llvm::ArrayRef<::llvm::Value*>(llvm_args));
        CHECK_TRUE(nullptr != ret, common::kCodegenCallFunctionError, "Fail to codegen Call Function")

        *output = NativeValue::Create(struct_value);
        return base::Status::OK();

    } else {
        FAIL_STATUS(common::kCodegenCallFunctionError, "Incorrect arguments passed")
    }
    return base::Status::OK();
}

// Build Struct Expr IR:
// TODO(chenjing): support method memeber
// @param node
// @param output
// @return
Status ExprIRBuilder::BuildStructExpr(const ::hybridse::node::StructExpr* node,
                                      NativeValue* output) {
    std::vector<::llvm::Type*> members;
    if (nullptr != node->GetFileds() && !node->GetFileds()->children.empty()) {
        for (auto each : node->GetFileds()->children) {
            node::FnParaNode* field = dynamic_cast<node::FnParaNode*>(each);
            ::llvm::Type* type = nullptr;
            CHECK_TRUE(ConvertHybridSeType2LlvmType(field->GetParaType(),
                                                    ctx_->GetModule(), &type),
                       kCodegenError,
                       "Invalid struct with unacceptable field type: " +
                           field->GetParaType()->GetName());
            members.push_back(type);
        }
    }
    ::llvm::StringRef name(node->GetName());
    ::llvm::StructType* llvm_struct =
        ::llvm::StructType::create(ctx_->GetLLVMContext(), name);
    ::llvm::ArrayRef<::llvm::Type*> array_ref(members);
    llvm_struct->setBody(array_ref);
    *output = NativeValue::Create(reinterpret_cast<::llvm::Value*>(llvm_struct));
    return Status::OK();
}

// Get inner window with given frame
Status ExprIRBuilder::BuildWindow(NativeValue* output) {  // NOLINT
    ::llvm::IRBuilder<> builder(ctx_->GetCurrentBlock());
    NativeValue window_ptr_value;
    const std::string frame_str = nullptr == frame_ ? "" : frame_->GetExprString();
    // Load Inner Window If Exist
    VariableIRBuilder variable_ir_builder(ctx_->GetCurrentBlock(), ctx_->GetCurrentScope()->sv());
    Status status;
    bool ok = variable_ir_builder.LoadWindow(frame_str, &window_ptr_value, status);
    ::llvm::Value* window_ptr = nullptr;
    if (!window_ptr_value.IsConstNull()) {
        window_ptr = window_ptr_value.GetValue(&builder);
    }

    if (ok && window_ptr != nullptr) {
        *output = NativeValue::Create(window_ptr);
        return Status::OK();
    }

    // Load Big Window, throw error if big window not exist
    ok = variable_ir_builder.LoadWindow("", &window_ptr_value, status);
    CHECK_TRUE(ok && nullptr != window_ptr_value.GetValue(&builder), kCodegenError,
               "Fail to find window " + status.str());

    // Build Inner Window based on Big Window and frame info
    // ListRef* { int8_t* } -> int8_t**
    ::llvm::Value* list_ref_ptr = window_ptr_value.GetValue(&builder);
    list_ref_ptr = builder.CreatePointerCast(list_ref_ptr, builder.getInt8PtrTy()->getPointerTo());
    ::llvm::Value* list_ptr = builder.CreateLoad(list_ref_ptr);

    MemoryWindowDecodeIRBuilder window_ir_builder(ctx_->schemas_context(), ctx_->GetCurrentBlock());
    if (frame_->frame_range() != nullptr) {
        NativeValue row_key_value;
        ok = variable_ir_builder.LoadRowKey(&row_key_value, status);
        ::llvm::Value* row_key = nullptr;
        if (!row_key_value.IsConstNull()) {
            row_key = row_key_value.GetValue(&builder);
        }
        CHECK_TRUE(ok && nullptr != row_key, kCodegenError, "Fail to build inner range window: row key is null");
        if (frame_->exclude_current_row_) {
            ok = window_ir_builder.BuildInnerRowsRangeList(list_ptr, row_key, 1,
                                                           frame_->GetHistoryRangeStart(), &window_ptr);
        } else {
            ok = window_ir_builder.BuildInnerRangeList(list_ptr, row_key, frame_->GetHistoryRangeEnd(),
                                                       frame_->GetHistoryRangeStart(), &window_ptr);
        }
    } else if (frame_->frame_rows() != nullptr) {
        ok = window_ir_builder.BuildInnerRowsList(list_ptr, ::hybridse::base::safe_inverse(frame_->GetHistoryRowsEnd()),
                                                  ::hybridse::base::safe_inverse(frame_->GetHistoryRowsStart()),
                                                  &window_ptr);
    }

    // int8_t** -> ListRef* { int8_t* }
    ::llvm::Value* inner_list_ref_ptr = CreateAllocaAtHead(&builder, window_ptr->getType(), "sub_window_alloca");
    builder.CreateStore(window_ptr, inner_list_ref_ptr);
    window_ptr = builder.CreatePointerCast(inner_list_ref_ptr, window_ptr->getType());

    CHECK_TRUE(ok && nullptr != window_ptr, kCodegenError, "Fail to build inner window " + frame_str);

    CHECK_TRUE(variable_ir_builder.StoreWindow(frame_str, window_ptr, status), kCodegenError, "Fail to store window ",
               frame_str, ": ", status.msg);
    DLOG(INFO) << "store window " << frame_str;

    *output = NativeValue::Create(window_ptr);
    return Status::OK();
}

// Get paramter item from parameter row
// param parameter
// param output
// return
Status ExprIRBuilder::BuildParameterExpr(const ::hybridse::node::ParameterExpr* parameter, NativeValue* output) {
    CHECK_TRUE(nullptr != ctx_->parameter_types() && !ctx_->parameter_types()->empty(), kCodegenError,
               "Fail to build parameter expression when parameter types is null or empty")
    CHECK_TRUE(parameter->position() > 0 && parameter->position() <= ctx_->parameter_types()->size(), kCodegenError,
               "Fail to build paramater expression when parameter position ", parameter->position(), " out of range")
    // Load Inner Window If Exist
    VariableIRBuilder variable_ir_builder(ctx_->GetCurrentBlock(), ctx_->GetCurrentScope()->sv());
    base::Status status;
    NativeValue parameter_row;
    CHECK_TRUE(variable_ir_builder.LoadParameter(&parameter_row, status), kCodegenError, status.msg);
    ::llvm::Value* slice_ptr = nullptr;
    ::llvm::Value* slice_size = nullptr;
    // Since parameter row has only one slice, the schema idx should be 0
    size_t schema_idx = 0;
    CHECK_STATUS(ExtractSliceFromRow(parameter_row, schema_idx, &slice_ptr, &slice_size))
    BufNativeIRBuilder buf_builder(
        schema_idx, ctx_->parameter_row_format(),
        ctx_->GetCurrentBlock(), ctx_->GetCurrentScope()->sv());
    CHECK_TRUE(
        buf_builder.BuildGetField(parameter->position()-1, slice_ptr, slice_size, output),
        kCodegenError, "Fail to get ", parameter->position(), "th parameter value")
    return base::Status::OK();
}
// Get col with given col name, set list struct pointer into output
// param col
// param output
// return
Status ExprIRBuilder::BuildColumnRef(
    const ::hybridse::node::ColumnRefNode* node, NativeValue* output) {
    const std::string relation_name = node->GetRelationName();
    const std::string col = node->GetColumnName();

    size_t schema_idx;
    size_t col_idx;
    CHECK_STATUS(ctx_->schemas_context()->ResolveColumnRefIndex(
                     node, &schema_idx, &col_idx),
                 "Fail to find context with " + node->GetExprString());

    ::llvm::Value* value = NULL;
    const std::string frame_str =
        nullptr == frame_ ? "" : frame_->GetExprString();
    DLOG(INFO) << "get table column " << col;
    // not found
    VariableIRBuilder variable_ir_builder(ctx_->GetCurrentBlock(),
                                          ctx_->GetCurrentScope()->sv());
    Status status;
    bool ok = variable_ir_builder.LoadColumnRef(relation_name, col, frame_str,
                                                &value, status);
    if (ok) {
        *output = NativeValue::Create(value);
        return Status::OK();
    }

    NativeValue window;
    CHECK_STATUS(BuildWindow(&window), "Fail to build window");

    DLOG(INFO) << "get table column " << col;
    // NOT reuse for iterator
    MemoryWindowDecodeIRBuilder window_ir_builder(ctx_->schemas_context(),
                                                  ctx_->GetCurrentBlock());
    ok = window_ir_builder.BuildGetCol(schema_idx, col_idx, window.GetRaw(),
                                       &value);
    CHECK_TRUE(ok && value != nullptr, kCodegenError,
               "fail to find column " + col);

    ok = variable_ir_builder.StoreColumnRef(relation_name, col, frame_str,
                                            value, status);
    CHECK_TRUE(ok, kCodegenError, "fail to store col for ", status.str());
    *output = NativeValue::Create(value);
    return Status::OK();
}

Status ExprIRBuilder::BuildUnaryExpr(const ::hybridse::node::UnaryExpr* node,
                                     NativeValue* output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kCodegenError,
               "Input node or output is null");
    CHECK_TRUE(node->GetChildNum() == 1, kCodegenError,
               "Invalid unary expr node");

    DLOG(INFO) << "build unary "
               << ::hybridse::node::ExprOpTypeName(node->GetOp());
    NativeValue left;
    CHECK_STATUS(Build(node->GetChild(0), &left), "Fail to build left node");

    PredicateIRBuilder predicate_ir_builder(ctx_->GetCurrentBlock());
    ArithmeticIRBuilder arithmetic_ir_builder(ctx_->GetCurrentBlock());
    switch (node->GetOp()) {
        case ::hybridse::node::kFnOpNot: {
            CHECK_STATUS(predicate_ir_builder.BuildNotExpr(left, output));
            break;
        }
        case ::hybridse::node::kFnOpMinus: {
            ::llvm::IRBuilder<> builder(ctx_->GetCurrentBlock());
            if (node->GetOutputType()->base() == node::kBool) {
                *output = left;
            } else {
                CHECK_STATUS(arithmetic_ir_builder.BuildSubExpr(
                    NativeValue::Create(builder.getInt16(0)), left, output));
            }
            break;
        }
        case ::hybridse::node::kFnOpBitwiseNot: {
            CHECK_STATUS(arithmetic_ir_builder.BuildBitwiseNotExpr(left, output));
            break;
        }
        case ::hybridse::node::kFnOpBracket: {
            *output = left;
            break;
        }
        case ::hybridse::node::kFnOpIsNull: {
            CHECK_STATUS(predicate_ir_builder.BuildIsNullExpr(left, output));
            break;
        }
        case ::hybridse::node::kFnOpNonNull: {
            // just ignore any null flag
            *output = NativeValue::Create(left.GetValue(ctx_));
            break;
        }
        default: {
            return Status(kCodegenError,
                          "Invalid op " + ExprOpTypeName(node->GetOp()));
        }
    }

    // check llvm type and inferred type
    if (node->GetOutputType() == nullptr) {
        LOG(WARNING) << "Unary op type not inferred";
    } else {
        ::llvm::Type* expect_llvm_ty = nullptr;
        GetLlvmType(ctx_->GetModule(), node->GetOutputType(), &expect_llvm_ty);
        if (expect_llvm_ty != output->GetType()) {
            LOG(WARNING) << "Inconsistent return llvm type: "
                         << GetLlvmObjectString(output->GetType())
                         << ", expect " << GetLlvmObjectString(expect_llvm_ty);
        }
    }
    return Status::OK();
}

Status ExprIRBuilder::BuildCastExpr(const ::hybridse::node::CastExprNode* node,
                                    NativeValue* output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kCodegenError,
               "Input node or output is null");

    DLOG(INFO) << "build cast expr: " << node::ExprString(node);
    NativeValue left;
    CHECK_STATUS(Build(node->expr(), &left), "Fail to build left node");

    CastExprIRBuilder cast_builder(ctx_->GetCurrentBlock());
    ::llvm::Type* cast_type = NULL;
    CHECK_TRUE(GetLlvmType(ctx_->GetModule(), node->cast_type_, &cast_type),
               kCodegenError, "Fail to cast expr: dist type invalid");

    if (cast_builder.IsSafeCast(left.GetType(), cast_type)) {
        return cast_builder.SafeCast(left, cast_type, output);
    } else {
        return cast_builder.UnSafeCast(left, cast_type, output);
    }
}

Status ExprIRBuilder::BuildBinaryExpr(const ::hybridse::node::BinaryExpr* node,
                                      NativeValue* output) {
    CHECK_TRUE(node != nullptr && output != nullptr, kCodegenError,
               "Input node or output is null");
    CHECK_TRUE(node->GetChildNum() == 2, kCodegenError,
               "Invalid binary expr node");

    DLOG(INFO) << "build binary "
               << ::hybridse::node::ExprOpTypeName(node->GetOp());
    NativeValue left;
    CHECK_STATUS(Build(node->children_[0], &left), "Fail to build left node");

    NativeValue right;
    CHECK_STATUS(Build(node->children_[1], &right), "Fail to build right node");

    ArithmeticIRBuilder arithmetic_ir_builder(ctx_->GetCurrentBlock());
    PredicateIRBuilder predicate_ir_builder(ctx_->GetCurrentBlock());

    switch (node->GetOp()) {
        case ::hybridse::node::kFnOpAdd: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildAddExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpMulti: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildMultiExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpFDiv: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildFDivExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpDiv: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildSDivExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpMinus: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildSubExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpMod: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildModExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpBitwiseAnd: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildBitwiseAndExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpBitwiseOr: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildBitwiseOrExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpBitwiseXor: {
            CHECK_STATUS(
                arithmetic_ir_builder.BuildBitwiseXorExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpAnd: {
            CHECK_STATUS(
                predicate_ir_builder.BuildAndExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpOr: {
            CHECK_STATUS(predicate_ir_builder.BuildOrExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpXor: {
            CHECK_STATUS(
                predicate_ir_builder.BuildXorExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpEq: {
            CHECK_STATUS(predicate_ir_builder.BuildEqExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpNeq: {
            CHECK_STATUS(
                predicate_ir_builder.BuildNeqExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpGt: {
            CHECK_STATUS(predicate_ir_builder.BuildGtExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpGe: {
            CHECK_STATUS(predicate_ir_builder.BuildGeExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpLt: {
            CHECK_STATUS(predicate_ir_builder.BuildLtExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpLe: {
            CHECK_STATUS(predicate_ir_builder.BuildLeExpr(left, right, output));
            break;
        }
        case ::hybridse::node::kFnOpAt: {
            CHECK_STATUS(BuildAsUdf(node, "at", {left, right}, output));
            break;
        }
        case ::hybridse::node::kFnOpLike: {
            CHECK_STATUS(BuildLikeExprAsUdf(node, "like_match", left, right, output))
            break;
        }
        case ::hybridse::node::kFnOpILike: {
            CHECK_STATUS(BuildLikeExprAsUdf(node, "ilike_match", left, right, output))
            break;
        }
        case ::hybridse::node::kFnOpRLike: {
            CHECK_STATUS(BuildRLikeExprAsUdf(node, "regexp_like", left, right, output))
            break;
        }
        default: {
            return Status(kCodegenError,
                          "Invalid op " + ExprOpTypeName(node->GetOp()));
        }
    }

    // check llvm type and inferred type
    if (node->GetOutputType() == nullptr) {
        LOG(WARNING) << "Binary op type not inferred";
    } else {
        ::llvm::Type* expect_llvm_ty = nullptr;
        GetLlvmType(ctx_->GetModule(), node->GetOutputType(), &expect_llvm_ty);
        if (expect_llvm_ty != output->GetType()) {
            LOG(WARNING) << "Inconsistent return llvm type: "
                         << GetLlvmObjectString(output->GetType())
                         << ", expect " << GetLlvmObjectString(expect_llvm_ty);
        }
    }
    return Status::OK();
}

Status ExprIRBuilder::BuildAsUdf(const node::ExprNode* expr,
                                 const std::string& name,
                                 const std::vector<NativeValue>& args,
                                 NativeValue* output) {
    CHECK_TRUE(args.size() == expr->GetChildNum(), kCodegenError);
    auto library = udf::DefaultUdfLibrary::get();

    std::vector<node::ExprNode*> proxy_args;
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        auto child = expr->GetChild(i);
        auto arg = ctx_->node_manager()->MakeExprIdNode("proxy_arg_" +
                                                        std::to_string(i));
        arg->SetOutputType(child->GetOutputType());
        arg->SetNullable(child->nullable());
        proxy_args.push_back(arg);
    }
    node::ExprNode* transformed = nullptr;
    CHECK_STATUS(library->Transform(name, proxy_args, ctx_->node_manager(),
                                    &transformed));

    node::ExprNode* target_expr = nullptr;
    node::ExprAnalysisContext analysis_ctx(ctx_->node_manager(), library,
                                           ctx_->schemas_context(), nullptr);
    passes::ResolveFnAndAttrs resolver(&analysis_ctx);
    CHECK_STATUS(resolver.VisitExpr(transformed, &target_expr));

    // Insert a transient binding scope between current scope and parent
    // Thus temporal binding of udf proxy arg can be dropped after build
    ScopeVar* cur_sv = ctx_->GetCurrentScope()->sv();
    ScopeVar proxy_sv_scope(cur_sv->parent());
    for (size_t i = 0; i < args.size(); ++i) {
        proxy_sv_scope.AddVar(proxy_args[i]->GetExprString(), args[i]);
    }
    cur_sv->SetParent(&proxy_sv_scope);

    Status status = Build(target_expr, output);

    cur_sv->SetParent(proxy_sv_scope.parent());
    return status;
}

Status ExprIRBuilder::BuildLikeExprAsUdf(const ::hybridse::node::BinaryExpr* expr,
                                         const std::string& name,
                                         const NativeValue& lhs,
                                         const NativeValue& rhs,
                                         NativeValue* output) {
    auto library = udf::DefaultUdfLibrary::get();

    std::vector<node::ExprNode*> proxy_args;
    const auto nm = ctx_->node_manager();

    // target node
    const auto target_node = expr->GetChild(0);
    auto arg_0 = nm->MakeExprIdNode("proxy_arg_0");
    arg_0->SetOutputType(target_node->GetOutputType());
    arg_0->SetNullable(target_node->nullable());
    proxy_args.push_back(arg_0);

    // pattern node
    auto arg_1 = nm->MakeExprIdNode("proxy_arg_1");
    const auto pattern_node = expr->GetChild(1);
    const auto type_node = pattern_node->GetOutputType();
    if (type_node->IsTuple()) {
        arg_1->SetOutputType(type_node->GetGenericType(0));
        arg_1->SetNullable(type_node->IsGenericNullable(0));
        proxy_args.push_back(arg_1);

        auto arg_2 = nm->MakeExprIdNode("proxy_arg_2");
        arg_2->SetOutputType(type_node->GetGenericType(1));
        arg_2->SetNullable(type_node->IsGenericNullable(1));
        proxy_args.push_back(arg_2);
    } else {
        arg_1->SetOutputType(pattern_node->GetOutputType());
        arg_1->SetNullable(pattern_node->nullable());
        proxy_args.push_back(arg_1);
    }

    node::ExprNode* transformed = nullptr;
    CHECK_STATUS(library->Transform(name, proxy_args, ctx_->node_manager(),
                                    &transformed));
    node::ExprNode* target_expr = nullptr;
    node::ExprAnalysisContext analysis_ctx(ctx_->node_manager(), library,
                                           ctx_->schemas_context(), nullptr);
    passes::ResolveFnAndAttrs resolver(&analysis_ctx);
    CHECK_STATUS(resolver.VisitExpr(transformed, &target_expr));

    // Insert a transient binding scope between current scope and parent
    // Thus temporal binding of udf proxy arg can be dropped after build
    ScopeVar* cur_sv = ctx_->GetCurrentScope()->sv();
    ScopeVar proxy_sv_scope(cur_sv->parent());
    proxy_sv_scope.AddVar(proxy_args[0]->GetExprString(), lhs);
    if (rhs.IsTuple()) {
        proxy_sv_scope.AddVar(proxy_args[1]->GetExprString(), rhs.GetField(0));
        proxy_sv_scope.AddVar(proxy_args[2]->GetExprString(), rhs.GetField(1));
    } else {
        proxy_sv_scope.AddVar(proxy_args[1]->GetExprString(), rhs);
    }

    cur_sv->SetParent(&proxy_sv_scope);

    Status status = Build(target_expr, output);

    cur_sv->SetParent(proxy_sv_scope.parent());
    return Status::OK();
}

Status ExprIRBuilder::BuildRLikeExprAsUdf(const ::hybridse::node::BinaryExpr* expr,
                                         const std::string& name,
                                         const NativeValue& lhs,
                                         const NativeValue& rhs,
                                         NativeValue* output) {
    auto library = udf::DefaultUdfLibrary::get();

    std::vector<node::ExprNode*> proxy_args;
    const auto nm = ctx_->node_manager();

    // target node
    const auto target_node = expr->GetChild(0);
    auto arg_0 = nm->MakeExprIdNode("proxy_arg_0");
    arg_0->SetOutputType(target_node->GetOutputType());
    arg_0->SetNullable(target_node->nullable());
    proxy_args.push_back(arg_0);

    // pattern node
    auto arg_1 = nm->MakeExprIdNode("proxy_arg_1");
    const auto pattern_node = expr->GetChild(1);
    const auto type_node = pattern_node->GetOutputType();
    if (type_node->IsTuple()) {
        arg_1->SetOutputType(type_node->GetGenericType(0));
        arg_1->SetNullable(type_node->IsGenericNullable(0));
        proxy_args.push_back(arg_1);

        auto arg_2 = nm->MakeExprIdNode("proxy_arg_2");
        arg_2->SetOutputType(type_node->GetGenericType(1));
        arg_2->SetNullable(type_node->IsGenericNullable(1));
        proxy_args.push_back(arg_2);
    } else {
        arg_1->SetOutputType(pattern_node->GetOutputType());
        arg_1->SetNullable(pattern_node->nullable());
        proxy_args.push_back(arg_1);
    }

    node::ExprNode* transformed = nullptr;
    CHECK_STATUS(library->Transform(name, proxy_args, ctx_->node_manager(),
                                    &transformed));
    node::ExprNode* target_expr = nullptr;
    node::ExprAnalysisContext analysis_ctx(ctx_->node_manager(), library,
                                           ctx_->schemas_context(), nullptr);
    passes::ResolveFnAndAttrs resolver(&analysis_ctx);
    CHECK_STATUS(resolver.VisitExpr(transformed, &target_expr));

    // Insert a transient binding scope between current scope and parent
    // Thus temporal binding of udf proxy arg can be dropped after build
    ScopeVar* cur_sv = ctx_->GetCurrentScope()->sv();
    ScopeVar proxy_sv_scope(cur_sv->parent());
    proxy_sv_scope.AddVar(proxy_args[0]->GetExprString(), lhs);
    if (rhs.IsTuple()) {
        proxy_sv_scope.AddVar(proxy_args[1]->GetExprString(), rhs.GetField(0));
        proxy_sv_scope.AddVar(proxy_args[2]->GetExprString(), rhs.GetField(1));
    } else {
        proxy_sv_scope.AddVar(proxy_args[1]->GetExprString(), rhs);
    }

    cur_sv->SetParent(&proxy_sv_scope);

    Status status = Build(target_expr, output);

    cur_sv->SetParent(proxy_sv_scope.parent());
    return Status::OK();
}

Status ExprIRBuilder::BuildGetFieldExpr(
    const ::hybridse::node::GetFieldExpr* node, NativeValue* output) {
    // build input
    Status status;
    NativeValue input_value;
    CHECK_STATUS(this->Build(node->GetRow(), &input_value));

    auto input_type = node->GetRow()->GetOutputType();
    if (input_type->base() == node::kTuple) {
        CHECK_TRUE(input_value.IsTuple() && input_value.GetFieldNum() ==
                                                input_type->GetGenericSize(),
                   kCodegenError, "Illegal input for kTuple, expect ",
                   input_type->GetName());
        try {
            size_t idx = node->GetColumnID();
            CHECK_TRUE(0 <= idx && idx < input_value.GetFieldNum(),
                       kCodegenError, "Tuple idx out of range: ", idx);
            *output = input_value.GetField(idx);
        } catch (std::invalid_argument& err) {
            return Status(kCodegenError,
                          "Invalid Tuple index: " + node->GetColumnName());
        }

    } else if (input_type->base() == node::kRow) {
        auto row_type = dynamic_cast<const node::RowTypeNode*>(
            node->GetRow()->GetOutputType());
        const auto schemas_context = row_type->schemas_ctx();

        size_t schema_idx;
        size_t col_idx;
        CHECK_STATUS(schemas_context->ResolveColumnIndexByID(
                         node->GetColumnID(), &schema_idx, &col_idx),
                     "Fail to resolve column ", node->GetExprString(), " from ",
                     row_type->GetName());
        ::llvm::Value* slice_ptr = nullptr;
        ::llvm::Value* slice_size = nullptr;
        CHECK_STATUS(ExtractSliceFromRow(input_value, schema_idx, &slice_ptr, &slice_size))
        BufNativeIRBuilder buf_builder(
            schema_idx, schemas_context->GetRowFormat(),
            ctx_->GetCurrentBlock(), ctx_->GetCurrentScope()->sv());
        CHECK_TRUE(
            buf_builder.BuildGetField(col_idx, slice_ptr, slice_size, output),
            kCodegenError);
    } else {
        return Status(common::kCodegenError,
                      "Get field's input is neither tuple nor row");
    }
    return Status::OK();
}

Status ExprIRBuilder::BuildCaseExpr(
    const ::hybridse::node::CaseWhenExprNode* node, NativeValue* output) {
    CHECK_TRUE(nullptr != node && nullptr != node->when_expr_list() &&
                   node->when_expr_list()->GetChildNum() > 0,
               kCodegenError);
    node::NodeManager* nm = ctx_->node_manager();
    node::ExprNode* expr =
        nullptr == node->else_expr() ? nm->MakeConstNode() : node->else_expr();
    for (auto iter = node->when_expr_list()->children_.rbegin();
         iter != node->when_expr_list()->children_.rend(); iter++) {
        auto when_expr = dynamic_cast<::hybridse::node::WhenExprNode*>(*iter);
        expr = nm->MakeCondExpr(when_expr->when_expr(), when_expr->then_expr(),
                                expr);
    }
    return BuildCondExpr(dynamic_cast<::hybridse::node::CondExpr*>(expr),
                         output);
}

Status ExprIRBuilder::BuildCondExpr(const ::hybridse::node::CondExpr* node,
                                    NativeValue* output) {
    // build condition
    NativeValue cond_value;
    CHECK_STATUS(this->Build(node->GetCondition(), &cond_value));

    // build left
    NativeValue left_value;
    CHECK_STATUS(this->Build(node->GetLeft(), &left_value));

    // build right
    NativeValue right_value;
    CHECK_STATUS(this->Build(node->GetRight(), &right_value));

    CondSelectIRBuilder cond_select_builder;
    return cond_select_builder.Select(ctx_->GetCurrentBlock(), cond_value,
                                      left_value, right_value, output);
}

Status ExprIRBuilder::BuildBetweenExpr(const ::hybridse::node::BetweenExpr* node, NativeValue* output) {
    CHECK_TRUE(node != nullptr && node->GetChildNum() == 3, kCodegenError, "invalid between expr node");

    NativeValue lhs_value;
    CHECK_STATUS(Build(node->GetLhs(), &lhs_value), "failed to build between lhs expr");

    NativeValue low_value;
    CHECK_STATUS(Build(node->GetLow(), &low_value), "failed to build between low expr");

    NativeValue high_value;
    CHECK_STATUS(Build(node->GetHigh(), &high_value), "failed to build between high expr");

    PredicateIRBuilder predicate_ir_builder(ctx_->GetCurrentBlock());
    return predicate_ir_builder.BuildBetweenExpr(lhs_value, low_value, high_value, node->is_not_between(), output);
}

Status ExprIRBuilder::BuildInExpr(const ::hybridse::node::InExpr* node, NativeValue* output) {
    CHECK_TRUE(node != nullptr, kCodegenError, "Invalid in expr node");

    NativeValue lhs_value;
    CHECK_STATUS(Build(node->GetLhs(), &lhs_value), "failed to build lhs in InExpr");

    NativeValue expr_value_list;
    CHECK_STATUS(Build(node->GetInList(), &expr_value_list));

    PredicateIRBuilder predicate_ir_builder(ctx_->GetCurrentBlock());
    CHECK_STATUS(predicate_ir_builder.BuildInExpr(lhs_value, expr_value_list, node->IsNot(), output));

    return Status::OK();
}

Status ExprIRBuilder::BuildExprList(const ::hybridse::node::ExprListNode* node, NativeValue* output) {
    std::vector<NativeValue> expr_value_list;
    for (const auto& ele : node->children_) {
        NativeValue ele_value;
        CHECK_STATUS(Build(ele, &ele_value));
        expr_value_list.push_back(std::move(ele_value));
    }
    *output = NativeValue::CreateTuple(std::move(expr_value_list));
    return Status::OK();
}

Status ExprIRBuilder::BuildEscapeExpr(const ::hybridse::node::EscapedExpr* node, NativeValue* output) {
    std::vector<NativeValue> expr_value_list(2, NativeValue());
    CHECK_STATUS(Build(node->GetPattern(), &expr_value_list[0]));
    CHECK_STATUS(Build(node->GetEscape(), &expr_value_list[1]));
    *output = NativeValue::CreateTuple(std::move(expr_value_list));
    return Status::OK();
}

Status ExprIRBuilder::ExtractSliceFromRow(const NativeValue& input_value, const int schema_idx, llvm::Value** slice_ptr,
                                          llvm::Value** slice_size) {
    auto& llvm_ctx = ctx_->GetLLVMContext();
    auto ptr_ty = llvm::Type::getInt8Ty(llvm_ctx)->getPointerTo();
    auto int64_ty = llvm::Type::getInt64Ty(llvm_ctx);
    auto int32_ty = llvm::Type::getInt32Ty(llvm_ctx);

    auto row_ptr = input_value.GetValue(ctx_);

    ::llvm::Module* module = ctx_->GetModule();
    ::llvm::IRBuilder<> builder(ctx_->GetCurrentBlock());

    size_t slice_idx = schema_idx;
    if (ctx_->schemas_context()->GetRowFormat() != nullptr) {
        // TODO(tobe): check schema contest and make sure it is built for unit tests
        slice_idx = ctx_->schemas_context()->GetRowFormat()->GetSliceId(schema_idx);
    }

    auto slice_idx_value = builder.getInt64(slice_idx);
    auto get_slice_func = module->getOrInsertFunction(
        "hybridse_storage_get_row_slice",
        ::llvm::FunctionType::get(ptr_ty, {ptr_ty, int64_ty}, false));
    auto get_slice_size_func = module->getOrInsertFunction(
        "hybridse_storage_get_row_slice_size",
        ::llvm::FunctionType::get(int64_ty, {ptr_ty, int64_ty}, false));
    *slice_ptr =
        builder.CreateCall(get_slice_func, {row_ptr, slice_idx_value});
    *slice_size = builder.CreateIntCast(
        builder.CreateCall(get_slice_size_func, {row_ptr, slice_idx_value}),
        int32_ty, false);
    return Status::OK();
}
}  // namespace codegen
}  // namespace hybridse
