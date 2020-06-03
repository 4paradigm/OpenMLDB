/*
 * fn_let_ir_builder_optsum.cc
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
#include <stdlib.h>

#include "codegen/fn_let_ir_builder.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "gflags/gflags.h"

DECLARE_bool(enable_column_sum_opt);
namespace fesql {
namespace codegen {


bool RowFnLetIRBuilder::EnableColumnSumOpt() const {
    if (FLAGS_enable_column_sum_opt) {
        LOG(INFO) << "Multi column sum opt is enabled";
        return true;
    }
    return false;
}

bool RowFnLetIRBuilder::IsColumnSum(const fesql::node::ExprNode* expr,
                                    fesql::node::ColumnRefNode** col,
                                    fesql::type::Type* sum_type) {
    switch (expr->expr_type_) {
        case node::kExprCall: {
            auto call = dynamic_cast<const node::CallExprNode*>(expr);
            if (!call->GetIsAgg() || call->GetFunctionName() != "sum") {
                break;
            }
            auto args = call->GetArgs();
            if (args->GetChildNum() != 1) {
                break;
            }
            auto input_expr = args->GetChild(0);
            if (input_expr->expr_type_ != node::kExprColumnRef) {
                break;
            }
            *col = dynamic_cast<node::ColumnRefNode*>(
                const_cast<node::ExprNode*>(input_expr));
            const std::string& rel_name = (*col)->GetRelationName();
            const std::string& col_name = (*col)->GetColumnName();
            const RowSchemaInfo* info;
            if (!schema_context_.ColumnRefResolved(
                    rel_name, col_name, &info)) {
                LOG(ERROR) << "fail to resolve column "
                    << rel_name + "." + col_name;
                return false;
            }
            codec::RowDecoder decoder(*info->schema_);
            uint32_t offset;
            type::Type col_type;
            if (!decoder.GetPrimayFieldOffsetType(
                    col_name, &offset, &col_type)) {
                LOG(ERROR) << "fail to resolve column " <<
                    rel_name + "." + col_name;
                return false;
            }
            *sum_type = col_type;
            return true;
        }
        default: break;
    }
    return false;
}


bool RowFnLetIRBuilder::BuildMultiColumnSum(
    const std::string& base_funcname,
    const std::vector<std::pair<::fesql::node::ColumnRefNode*, uint32_t>>& cols,
    ExprIRBuilder* expr_ir_builder,
    VariableIRBuilder* variable_ir_builder,
    ::llvm::BasicBlock* cur_block,
    const std::string& output_ptr_name,
    vm::Schema* output_schema) {

    ::llvm::LLVMContext& llvm_ctx = module_->getContext();
    ::llvm::IRBuilder<> builder(llvm_ctx);
    auto void_ty = llvm::Type::getVoidTy(llvm_ctx);
    auto int64_ty = llvm::Type::getInt64Ty(llvm_ctx);

    base::Status status;
    llvm::Value* window_ptr = nullptr;
    bool ok = variable_ir_builder->LoadValue("window_ptr", &window_ptr, status);
    if (!ok || window_ptr == nullptr) {
        LOG(ERROR) << "fail to find window_ptr: " + status.msg;
        return false;
    }
    llvm::Value* output_buf = nullptr;
    ok = variable_ir_builder->LoadValue(output_ptr_name, &output_buf, status);
    if (!ok) {
        LOG(ERROR) << "fail to get output row ptr";
        return false;
    }

    std::vector<codec::RowDecoder> decoders;
    for (auto& info : schema_context_.row_schema_info_list_) {
        decoders.push_back(codec::RowDecoder(*info.schema_));
    }

    // resolve col info
    std::vector<llvm::Type*> col_sum_types;
    std::vector<size_t> col_slice_idxs;
    for (auto pair : cols) {
        const std::string& relation_name = pair.first->GetRelationName();
        const std::string& col_name = pair.first->GetColumnName();
        const RowSchemaInfo* info;
        if (!schema_context_.ColumnRefResolved(
                relation_name, col_name, &info)) {
            LOG(ERROR) << "fail to resolve column " <<
                relation_name + "." + col_name;
            return false;
        }
        auto& decoder = decoders[info->idx_];

        uint32_t offset;
        type::Type col_type;
        if (!decoder.GetPrimayFieldOffsetType(col_name, &offset, &col_type)) {
            LOG(ERROR) << "fail to resolve column " <<
                relation_name + "." + col_name;
            return false;
        }

        node::DataType node_col_type;
        if (!SchemaType2DataType(col_type, &node_col_type)) {
            LOG(ERROR) << "unrecognized data type " <<
                fesql::type::Type_Name(col_type);
            return false;
        }

        ::llvm::Type* sum_type;
        switch (node_col_type) {
            case ::fesql::node::kInt16:
                sum_type = ::llvm::Type::getInt16Ty(llvm_ctx); break;
            case ::fesql::node::kInt32:
                sum_type = ::llvm::Type::getInt32Ty(llvm_ctx); break;
            case ::fesql::node::kInt64:
                sum_type = ::llvm::Type::getInt64Ty(llvm_ctx); break;
            case ::fesql::node::kFloat:
                sum_type = ::llvm::Type::getFloatTy(llvm_ctx); break;
            case ::fesql::node::kDouble:
                sum_type = ::llvm::Type::getDoubleTy(llvm_ctx); break;
            default: {
                LOG(ERROR) << "do not support sum on non-numeric type";
                return false;
            }
        }
        col_sum_types.push_back(sum_type);
        col_slice_idxs.push_back(info->idx_);
        LOG(INFO) << "column sum on " << col_name << " offset=" << offset;
    }

    std::string fn_name = base_funcname + "_multi_column_sum__";
    auto ptr_ty = llvm::Type::getInt8Ty(llvm_ctx)->getPointerTo();
    ::llvm::FunctionType *fnt = ::llvm::FunctionType::get(
        llvm::Type::getVoidTy(llvm_ctx),
        {ptr_ty, ptr_ty}, false);
    ::llvm::Function* fn = ::llvm::Function::Create(fnt,
        llvm::Function::ExternalLinkage, fn_name, module_);
    builder.SetInsertPoint(cur_block);
    builder.CreateCall(module_->getOrInsertFunction(fn_name, fnt),
        {window_ptr, builder.CreateLoad(output_buf)});

    ::llvm::BasicBlock* head_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "head", fn);
    ::llvm::BasicBlock* enter_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "enter_iter", fn);
    ::llvm::BasicBlock* body_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "iter_body", fn);
    ::llvm::BasicBlock* exit_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "exit_iter", fn);

    // gen head
    builder.SetInsertPoint(head_block);
    std::vector<::llvm::Value*> accum_values;
    for (::llvm::Type* sum_type : col_sum_types) {
        auto accum = builder.CreateAlloca(sum_type);
        if (sum_type->isIntegerTy()) {
            builder.CreateStore(
                ::llvm::ConstantInt::get(sum_type, 0, true), accum);
        } else {
            builder.CreateStore(
                ::llvm::ConstantFP::get(sum_type, 0.0), accum);
        }
        accum_values.push_back(accum);
    }
    ::llvm::Value* input_arg = fn->arg_begin();
    ::llvm::Value* output_arg = fn->arg_begin() + 1;

    // on stack unique pointer
    size_t iter_bytes = sizeof(std::unique_ptr<codec::RowIterator>);
    ::llvm::Value* iter_ptr = builder.CreateAlloca(
        ::llvm::Type::getInt8Ty(llvm_ctx),
        ::llvm::ConstantInt::get(int64_ty, iter_bytes, true));
    auto get_iter_func = module_->getOrInsertFunction(
        "fesql_storage_get_row_iter", void_ty, ptr_ty, ptr_ty);
    builder.CreateCall(get_iter_func, {input_arg, iter_ptr});
    builder.CreateBr(enter_block);

    // gen iter begin
    builder.SetInsertPoint(enter_block);
    auto bool_ty = llvm::Type::getInt1Ty(llvm_ctx);
    auto has_next_func = module_->getOrInsertFunction(
        "fesql_storage_row_iter_has_next",
        ::llvm::FunctionType::get(bool_ty, {ptr_ty}, false));
    ::llvm::Value* has_next = builder.CreateCall(has_next_func, iter_ptr);
    builder.CreateCondBr(has_next, body_block, exit_block);

    // gen iter body
    builder.SetInsertPoint(body_block);
    auto get_slice_func = module_->getOrInsertFunction(
        "fesql_storage_row_iter_get_cur_slice",
        ::llvm::FunctionType::get(ptr_ty, {ptr_ty, int64_ty}, false));
    auto get_slice_size_func = module_->getOrInsertFunction(
        "fesql_storage_row_iter_get_cur_slice_size",
        ::llvm::FunctionType::get(int64_ty, {ptr_ty, int64_ty}, false));
    std::unordered_map<size_t,
        std::pair<::llvm::Value*, ::llvm::Value*>> used_slices;
    // compute current row's slices
    for (size_t i = 0; i < cols.size(); ++i) {
        size_t slice_idx = col_slice_idxs[i];
        auto iter = used_slices.find(slice_idx);
        if (iter == used_slices.end()) {
            ::llvm::Value* idx_value = llvm::ConstantInt::get(
                int64_ty, slice_idx, true);
            ::llvm::Value* buf_ptr = builder.CreateCall(
                get_slice_func, {iter_ptr, idx_value});
            ::llvm::Value* buf_size = builder.CreateCall(
                get_slice_size_func, {iter_ptr, idx_value});
            used_slices[slice_idx] = {buf_ptr, buf_size};
        }
    }
    // compute row field fetches
    std::vector<llvm::Value*> cur_row_fields;
    for (size_t i = 0; i < cols.size(); ++i) {
        size_t slice_idx = col_slice_idxs[i];
        auto& slice_info = used_slices[slice_idx];

        ScopeVar dummy_scope_var;
        BufNativeIRBuilder buf_builder(
            *schema_context_.row_schema_info_list_[slice_idx].schema_,
            body_block, &dummy_scope_var);
        llvm::Value* field_value = nullptr;
        if (!buf_builder.BuildGetField(cols[i].first->GetColumnName(),
                                       slice_info.first,
                                       slice_info.second,
                                       &field_value)) {
            LOG(ERROR) << "fail to gen fetch column";
            return false;
        }
        cur_row_fields.push_back(field_value);
    }
    // compute accumulation
    for (size_t i = 0; i < cols.size(); ++i) {
        ::llvm::Value* accum = accum_values[i];
        ::llvm::Value* field_value = cur_row_fields[i];
        ::llvm::Value* accum_value = builder.CreateLoad(accum);

        ::llvm::Value* add;
        if (col_sum_types[i]->isIntegerTy()) {
            add = builder.CreateAdd(accum_value, field_value);
        } else {
            add = builder.CreateFAdd(accum_value, field_value);
        }
        builder.CreateStore(add, accum);
    }
    auto next_func = module_->getOrInsertFunction(
        "fesql_storage_row_iter_next",
        ::llvm::FunctionType::get(void_ty, {ptr_ty}, false));
    builder.CreateCall(next_func, {iter_ptr});
    builder.CreateBr(enter_block);

    // gen iter end
    builder.SetInsertPoint(exit_block);
    auto delete_iter_func = module_->getOrInsertFunction(
        "fesql_storage_row_iter_delete",
        ::llvm::FunctionType::get(void_ty, {ptr_ty}, false));
    builder.CreateCall(delete_iter_func, {iter_ptr});

    // store results to output row
    std::map<uint32_t, ::llvm::Value*> dummy_map;
    BufNativeEncoderIRBuilder output_encoder(
        &dummy_map, *output_schema, exit_block);
    for (size_t i = 0; i < cols.size(); ++i) {
        ::llvm::Value* accum = accum_values[i];
        accum = builder.CreateLoad(accum);
        size_t output_idx = cols[i].second;
        output_encoder.BuildEncodePrimaryField(
            output_arg, output_idx, accum);
    }
    builder.CreateRetVoid();
    return true;
}

}  // namespace codegen
}  // namespace fesql
