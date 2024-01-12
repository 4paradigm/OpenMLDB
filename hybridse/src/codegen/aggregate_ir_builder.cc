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
#include "codegen/aggregate_ir_builder.h"

#include <algorithm>
#include <limits>
#include <map>
#include <memory>

#include "absl/container/flat_hash_set.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/variable_ir_builder.h"
#include "glog/logging.h"
#include "node/node_manager.h"

namespace hybridse {
namespace codegen {

static auto CreateAggFuncMap() {
    absl::flat_hash_set<absl::string_view> res = {"sum", "avg", "count", "min", "max"};
    return res;
}

static auto& GetAggFuncMap() {
    static const absl::flat_hash_set<absl::string_view>& res = *new auto(CreateAggFuncMap());
    return res;
}

AggregateIRBuilder::AggregateIRBuilder(const vm::SchemasContext* sc, ::llvm::Module* module,
                                       const node::FrameNode* frame_node, uint32_t id)
    : schema_context_(sc), module_(module), frame_node_(frame_node), id_(id) {}

bool AggregateIRBuilder::IsAggFuncName(absl::string_view fname) const {
    auto& map = GetAggFuncMap();
    return map.find(fname) != map.end();
}

bool AggregateIRBuilder::CollectAggColumn(const hybridse::node::ExprNode* expr,
                                          size_t output_idx,
                                          hybridse::type::Type* res_agg_type) {
    switch (expr->expr_type_) {
        case node::kExprCall: {
            auto call = dynamic_cast<const node::CallExprNode*>(expr);
            std::string agg_func_name = "";
            switch (call->GetFnDef()->GetType()) {
                case node::kExternalFnDef: {
                    agg_func_name =
                        dynamic_cast<const node::ExternalFnDefNode*>(
                            call->GetFnDef())
                            ->function_name();
                    break;
                }
                case node::kUdafDef: {
                    agg_func_name =
                        dynamic_cast<const node::UdafDefNode*>(call->GetFnDef())
                            ->GetName();
                    break;
                }
                default:
                    break;
            }
            boost::to_lower(agg_func_name);
            if (!IsAggFuncName(agg_func_name)) {
                break;
            }
            if (call->GetChildNum() != 1) {
                break;
            }
            auto input_expr = call->GetChild(0);
            if (input_expr->expr_type_ != node::kExprColumnRef) {
                break;
            }
            auto col = dynamic_cast<node::ColumnRefNode*>(
                const_cast<node::ExprNode*>(input_expr));
            const std::string& rel_name = col->GetRelationName();
            const std::string& col_name = col->GetColumnName();

            size_t schema_idx;
            size_t col_idx;
            Status status = schema_context_->ResolveColumnRefIndex(
                col, &schema_idx, &col_idx);
            if (!status.isOK()) {
                DLOG(ERROR) << status;
                return false;
            }
            const codec::ColInfo& col_info = *schema_context_->GetRowFormat()->GetColumnInfo(schema_idx, col_idx);
            uint32_t offset = col_info.offset;

            // resolve llvm agg type
            node::NodeManager nm;
            auto s = ColumnSchema2Type(col_info.schema, &nm);
            if (!s.ok()) {
                // legacy udf resolve context, this only happens for base types
                LOG(ERROR) << s.status();
                return false;
            }
            auto* type = s.value();
            if (!type->IsBaseType()) {
                LOG(INFO) << "skip CollectAggColumn for non-base types";
                return false;
            }
            if (GetOutputLlvmType(module_->getContext(), agg_func_name, type->base()) == nullptr) {
                return false;
            }
            if (agg_func_name == "count") {
                *res_agg_type = ::hybridse::type::kInt64;
            } else if (agg_func_name == "avg") {
                *res_agg_type = ::hybridse::type::kDouble;
            } else {
                *res_agg_type = col_info.schema.base_type();
            }

            std::string col_key = absl::StrCat(rel_name, ".", col_name);
            auto res = agg_col_infos_.try_emplace(col_key, col, type->base(), schema_idx, col_idx, offset);
            res.first->second.AddAgg(agg_func_name, output_idx);
            return true;
        }
        default:
            break;
    }
    return false;
}

class StatisticalAggGenerator {
 public:
    StatisticalAggGenerator(node::DataType col_type,
                            const std::vector<std::string>& col_keys)
        : col_type_(col_type),
          col_num_(col_keys.size()),
          col_keys_(col_keys),
          sum_idxs_(col_num_),
          avg_idxs_(col_num_),
          count_idxs_(col_num_),
          min_idxs_(col_num_),
          max_idxs_(col_num_),
          sum_states_(col_num_, nullptr),
          avg_states_(col_num_, nullptr),
          min_states_(col_num_, nullptr),
          max_states_(col_num_, nullptr),
          count_state_(col_num_, nullptr) {}

    ::llvm::Value* GenSumInitState(::llvm::IRBuilder<>* builder) {
        ::llvm::LLVMContext& llvm_ctx = builder->getContext();
        ::llvm::Type* llvm_ty =
            AggregateIRBuilder::GetOutputLlvmType(llvm_ctx, "sum", col_type_);
        ::llvm::Value* accum = CreateAllocaAtHead(builder, llvm_ty, "sum");
        if (llvm_ty->isIntegerTy()) {
            builder->CreateStore(::llvm::ConstantInt::get(llvm_ty, 0, true),
                                 accum);
        } else {
            builder->CreateStore(::llvm::ConstantFP::get(llvm_ty, 0.0), accum);
        }
        return accum;
    }

    ::llvm::Value* GenAvgInitState(::llvm::IRBuilder<>* builder) {
        ::llvm::LLVMContext& llvm_ctx = builder->getContext();
        ::llvm::Type* llvm_ty =
            AggregateIRBuilder::GetOutputLlvmType(llvm_ctx, "avg", col_type_);
        ::llvm::Value* accum = CreateAllocaAtHead(builder, llvm_ty, "avg");
        builder->CreateStore(::llvm::ConstantFP::get(llvm_ty, 0.0), accum);
        return accum;
    }

    ::llvm::Value* GenCountInitState(::llvm::IRBuilder<>* builder) {
        ::llvm::LLVMContext& llvm_ctx = builder->getContext();
        ::llvm::Type* int64_ty = ::llvm::Type::getInt64Ty(llvm_ctx);
        ::llvm::Value* cnt = CreateAllocaAtHead(builder, int64_ty, "cnt");
        builder->CreateStore(::llvm::ConstantInt::get(int64_ty, 0, true), cnt);
        return cnt;
    }

    ::llvm::Value* GenMinInitState(::llvm::IRBuilder<>* builder) {
        ::llvm::LLVMContext& llvm_ctx = builder->getContext();
        ::llvm::Type* llvm_ty =
            AggregateIRBuilder::GetOutputLlvmType(llvm_ctx, "min", col_type_);
        ::llvm::Value* accum = CreateAllocaAtHead(builder, llvm_ty, "min");
        ::llvm::Value* min;
        if (llvm_ty == ::llvm::Type::getInt16Ty(llvm_ctx)) {
            min = ::llvm::ConstantInt::get(
                llvm_ty, std::numeric_limits<int16_t>::max(), true);
        } else if (llvm_ty == ::llvm::Type::getInt32Ty(llvm_ctx)) {
            min = ::llvm::ConstantInt::get(
                llvm_ty, std::numeric_limits<int32_t>::max(), true);
        } else if (llvm_ty == ::llvm::Type::getInt64Ty(llvm_ctx)) {
            min = ::llvm::ConstantInt::get(
                llvm_ty, std::numeric_limits<int64_t>::max(), true);
        } else if (llvm_ty == ::llvm::Type::getFloatTy(llvm_ctx)) {
            min = ::llvm::ConstantFP::get(llvm_ty,
                                          std::numeric_limits<float>::max());
        } else {
            min = ::llvm::ConstantFP::get(llvm_ty,
                                          std::numeric_limits<double>::max());
        }
        builder->CreateStore(min, accum);
        return accum;
    }

    ::llvm::Value* GenMaxInitState(::llvm::IRBuilder<>* builder) {
        ::llvm::LLVMContext& llvm_ctx = builder->getContext();
        ::llvm::Type* llvm_ty =
            AggregateIRBuilder::GetOutputLlvmType(llvm_ctx, "max", col_type_);
        ::llvm::Value* accum = CreateAllocaAtHead(builder, llvm_ty, "max");
        ::llvm::Value* max;
        if (llvm_ty == ::llvm::Type::getInt16Ty(llvm_ctx)) {
            max = ::llvm::ConstantInt::get(
                llvm_ty, std::numeric_limits<int16_t>::lowest(), true);
        } else if (llvm_ty == ::llvm::Type::getInt32Ty(llvm_ctx)) {
            max = ::llvm::ConstantInt::get(
                llvm_ty, std::numeric_limits<int32_t>::lowest(), true);
        } else if (llvm_ty == ::llvm::Type::getInt64Ty(llvm_ctx)) {
            max = ::llvm::ConstantInt::get(
                llvm_ty, std::numeric_limits<int64_t>::lowest(), true);
        } else if (llvm_ty == ::llvm::Type::getFloatTy(llvm_ctx)) {
            max = ::llvm::ConstantFP::get(llvm_ty,
                                          std::numeric_limits<float>::lowest());
        } else {
            max = ::llvm::ConstantFP::get(
                llvm_ty, std::numeric_limits<double>::lowest());
        }
        builder->CreateStore(max, accum);
        return accum;
    }

    void GenInitState(::llvm::IRBuilder<>* builder) {
        for (size_t i = 0; i < col_num_; ++i) {
            // any of min/max/avg/sum/count need the count info
            count_state_[i] = GenCountInitState(builder);

            if (!sum_idxs_[i].empty()) {
                sum_states_[i] = GenSumInitState(builder);
            }
            if (!avg_idxs_[i].empty()) {
                if (col_type_ == ::hybridse::node::kDouble) {
                    if (sum_states_[i] == nullptr) {
                        sum_states_[i] = GenSumInitState(builder);
                    }
                } else {
                    avg_states_[i] = GenAvgInitState(builder);
                }
            }
            if (!min_idxs_[i].empty()) {
                min_states_[i] = GenMinInitState(builder);
            }
            if (!max_idxs_[i].empty()) {
                max_states_[i] = GenMaxInitState(builder);
            }
        }
    }

    void GenSumUpdate(size_t i, ::llvm::Value* input, ::llvm::Value* is_null,
                      ::llvm::IRBuilder<>* builder) {
        ::llvm::Value* accum = builder->CreateLoad(sum_states_[i]);
        ::llvm::Value* add;
        if (input->getType()->isIntegerTy()) {
            add = builder->CreateAdd(accum, input);
        } else {
            add = builder->CreateFAdd(accum, input);
        }
        add = builder->CreateSelect(is_null, accum, add);
        builder->CreateStore(add, sum_states_[i]);
    }

    void GenAvgUpdate(size_t i, ::llvm::Value* input, ::llvm::Value* is_null,
                      ::llvm::IRBuilder<>* builder) {
        ::llvm::Value* accum = builder->CreateLoad(avg_states_[i]);
        if (input->getType()->isIntegerTy()) {
            input = builder->CreateSIToFP(input, accum->getType());
        } else {
            input = builder->CreateFPCast(input, accum->getType());
        }
        ::llvm::Value* sum = builder->CreateFAdd(accum, input);
        sum = builder->CreateSelect(is_null, accum, sum);
        builder->CreateStore(sum, avg_states_[i]);
    }

    void GenCountUpdate(size_t i, ::llvm::Value* is_null, ::llvm::IRBuilder<>* builder) {
        ::llvm::Value* one = ::llvm::ConstantInt::get(
            reinterpret_cast<::llvm::PointerType*>(count_state_[i]->getType())->getElementType(), 1, true);
        ::llvm::Value* cnt = builder->CreateLoad(count_state_[i]);
        ::llvm::Value* new_cnt = builder->CreateAdd(cnt, one);
        new_cnt = builder->CreateSelect(is_null, cnt, new_cnt);
        builder->CreateStore(new_cnt, count_state_[i]);
    }

    void GenMinUpdate(size_t i, ::llvm::Value* input, ::llvm::Value* is_null,
                      ::llvm::IRBuilder<>* builder) {
        ::llvm::Value* accum = builder->CreateLoad(min_states_[i]);
        ::llvm::Type* min_ty = accum->getType();
        ::llvm::Value* cmp;
        if (min_ty->isIntegerTy()) {
            cmp = builder->CreateICmpSLT(accum, input);
        } else {
            cmp = builder->CreateFCmpOLT(accum, input);
        }
        ::llvm::Value* min = builder->CreateSelect(cmp, accum, input);
        min = builder->CreateSelect(is_null, accum, min);
        builder->CreateStore(min, min_states_[i]);
    }

    void GenMaxUpdate(size_t i, ::llvm::Value* input, ::llvm::Value* is_null,
                      ::llvm::IRBuilder<>* builder) {
        ::llvm::Value* accum = builder->CreateLoad(max_states_[i]);
        ::llvm::Type* max_ty = accum->getType();
        ::llvm::Value* cmp;
        if (max_ty->isIntegerTy()) {
            cmp = builder->CreateICmpSLT(accum, input);
        } else {
            cmp = builder->CreateFCmpOLT(accum, input);
        }
        ::llvm::Value* max = builder->CreateSelect(cmp, input, accum);
        max = builder->CreateSelect(is_null, accum, max);
        builder->CreateStore(max, max_states_[i]);
    }

    void GenUpdate(::llvm::IRBuilder<>* builder,
                   const std::vector<::llvm::Value*>& inputs,
                   const std::vector<::llvm::Value*>& is_null) {
        for (size_t i = 0; i < col_num_; ++i) {
            GenCountUpdate(i, is_null[i], builder);
            if (!sum_idxs_[i].empty() || (!avg_idxs_[i].empty() && avg_states_[i] == nullptr)) {
                GenSumUpdate(i, inputs[i], is_null[i], builder);
            }
            if (!avg_idxs_[i].empty() && avg_states_[i] != nullptr) {
                GenAvgUpdate(i, inputs[i], is_null[i], builder);
            }
            if (!min_idxs_[i].empty()) {
                GenMinUpdate(i, inputs[i], is_null[i], builder);
            }
            if (!max_idxs_[i].empty()) {
                GenMaxUpdate(i, inputs[i], is_null[i], builder);
            }
        }
    }

    void GenOutputs(::llvm::IRBuilder<>* builder, std::vector<std::pair<size_t, NativeValue>>* outputs) {
        for (size_t i = 0; i < col_num_; ++i) {
            // cnt always exists
            ::llvm::Value* cnt = builder->CreateLoad(count_state_[i]);
            ::llvm::Value* is_empty = builder->CreateICmpEQ(cnt, builder->getInt64(0));

            if (!sum_idxs_[i].empty()) {
                ::llvm::Value* accum = builder->CreateLoad(sum_states_[i]);
                for (int idx : sum_idxs_[i]) {
                    outputs->emplace_back(idx, NativeValue::CreateWithFlag(accum, is_empty));
                }
            }

            if (!avg_idxs_[i].empty()) {
                ::llvm::Type* avg_ty = AggregateIRBuilder::GetOutputLlvmType(builder->getContext(), "avg", col_type_);
                ::llvm::Value* sum;
                if (avg_states_[i] == nullptr) {
                    sum = builder->CreateLoad(sum_states_[i]);
                } else {
                    sum = builder->CreateLoad(avg_states_[i]);
                }
                ::llvm::Value* raw_avg = builder->CreateFDiv(sum, builder->CreateSIToFP(cnt, avg_ty));
                for (int idx : avg_idxs_[i]) {
                    outputs->emplace_back(idx, NativeValue::CreateWithFlag(raw_avg, is_empty));
                }
            }

            if (!count_idxs_[i].empty()) {
                for (int idx : count_idxs_[i]) {
                    outputs->emplace_back(idx, NativeValue::Create(cnt));
                }
            }

            if (!min_idxs_[i].empty()) {
                ::llvm::Value* accum = builder->CreateLoad(min_states_[i]);
                for (int idx : min_idxs_[i]) {
                    outputs->emplace_back(idx, NativeValue::CreateWithFlag(accum, is_empty));
                }
            }

            if (!max_idxs_[i].empty()) {
                ::llvm::Value* accum = builder->CreateLoad(max_states_[i]);
                for (int idx : max_idxs_[i]) {
                    outputs->emplace_back(idx, NativeValue::CreateWithFlag(accum, is_empty));
                }
            }
        }
    }

    void RegisterSum(size_t pos, size_t out_idx) {
        sum_idxs_[pos].push_back(out_idx);
    }

    void RegisterAvg(size_t pos, size_t out_idx) {
        avg_idxs_[pos].push_back(out_idx);
    }

    void RegisterCount(size_t pos, size_t out_idx) {
        count_idxs_[pos].push_back(out_idx);
    }

    void RegisterMin(size_t pos, size_t out_idx) {
        min_idxs_[pos].push_back(out_idx);
    }

    void RegisterMax(size_t pos, size_t out_idx) {
        max_idxs_[pos].push_back(out_idx);
    }

    const std::vector<std::string>& GetColKeys() const { return col_keys_; }

 private:
    node::DataType col_type_;
    size_t col_num_;
    std::vector<std::string> col_keys_;

    std::vector<std::vector<int>> sum_idxs_;
    std::vector<std::vector<int>> avg_idxs_;
    std::vector<std::vector<int>> count_idxs_;
    std::vector<std::vector<int>> min_idxs_;
    std::vector<std::vector<int>> max_idxs_;

    // accumulation states
    std::vector<::llvm::Value*> sum_states_;
    std::vector<::llvm::Value*> avg_states_;
    std::vector<::llvm::Value*> min_states_;
    std::vector<::llvm::Value*> max_states_;
    // the count of non-null values of corresponding column
    // represent the result of `count(col)`
    // and help sum/avg/min/max to determine if `NULL` should returned
    std::vector<::llvm::Value*> count_state_;
};

llvm::Type* AggregateIRBuilder::GetOutputLlvmType(::llvm::LLVMContext& llvm_ctx, const std::string& fname,
                                                  const node::DataType& node_type) {
    if (fname == "count") {
        return ::llvm::Type::getInt64Ty(llvm_ctx);
    } else if (fname == "avg") {
        return ::llvm::Type::getDoubleTy(llvm_ctx);
    }

    ::llvm::Type* llvm_ty = nullptr;
    switch (node_type) {
        case ::hybridse::node::kInt16:
            llvm_ty = ::llvm::Type::getInt16Ty(llvm_ctx);
            break;
        case ::hybridse::node::kInt32:
            llvm_ty = ::llvm::Type::getInt32Ty(llvm_ctx);
            break;
        case ::hybridse::node::kInt64:
            llvm_ty = ::llvm::Type::getInt64Ty(llvm_ctx);
            break;
        case ::hybridse::node::kFloat:
            llvm_ty = ::llvm::Type::getFloatTy(llvm_ctx);
            break;
        case ::hybridse::node::kDouble:
            llvm_ty = ::llvm::Type::getDoubleTy(llvm_ctx);
            break;
        default: {
            LOG(ERROR) << "Unknown data type: " << DataTypeName(node_type);
            return nullptr;
        }
    }
    return llvm_ty;
}

size_t GetTypeByteSize(node::DataType dtype) {
    switch (dtype) {
        case ::hybridse::node::kInt16:
            return 2;
        case ::hybridse::node::kInt32:
            return 4;
        case ::hybridse::node::kInt64:
            return 8;
        case ::hybridse::node::kFloat:
            return 4;
        case ::hybridse::node::kDouble:
            return 8;
        default:
            return 0;
    }
}

base::Status ScheduleAggGenerators(
    std::unordered_map<std::string, AggColumnInfo>& agg_col_infos,  // NOLINT
    std::vector<StatisticalAggGenerator>* res) {
    // collect and sort used input columns
    std::vector<std::string> col_keys;
    for (auto& pair : agg_col_infos) {
        col_keys.emplace_back(pair.first);
    }
    std::sort(col_keys.begin(), col_keys.end(),
              [&agg_col_infos](const std::string& l, const std::string& r) {
                  return agg_col_infos[l].offset < agg_col_infos[r].offset;
              });

    // schedule agg op generators
    // try best to find contiguous input cols of same type
    size_t idx = 0;
    int64_t cur_offset = -1;
    node::DataType cur_col_type = node::kNull;
    std::vector<std::string> agg_gen_col_seq;

    while (!agg_gen_col_seq.empty() || idx < col_keys.size()) {
        AggColumnInfo* info = nullptr;

        bool finish_seq = false;
        if (idx >= col_keys.size()) {
            finish_seq = true;
        } else {
            info = &agg_col_infos[col_keys[idx]];
            if (agg_gen_col_seq.size() >= 4) {
                finish_seq = true;
            } else if (cur_offset >= 0) {
                size_t bytes = GetTypeByteSize(cur_col_type);
                if (info->col_type != cur_col_type ||
                    info->offset - cur_offset != bytes) {
                    finish_seq = true;
                }
            }
        }

        if (finish_seq) {
            // create generator from contiguous seq
            StatisticalAggGenerator agg_gen(cur_col_type, agg_gen_col_seq);
            for (size_t i = 0; i < agg_gen_col_seq.size(); ++i) {
                auto& geninfo = agg_col_infos[agg_gen_col_seq[i]];
                DLOG(INFO) << geninfo.DebugString();
                for (size_t j = 0; j < geninfo.GetOutputNum(); j++) {
                    auto& fname = geninfo.agg_funcs[j];
                    size_t out_idx = geninfo.output_idxs[j];

                    if (fname == "sum") {
                        agg_gen.RegisterSum(i, out_idx);
                    } else if (fname == "min") {
                        agg_gen.RegisterMin(i, out_idx);
                    } else if (fname == "max") {
                        agg_gen.RegisterMax(i, out_idx);
                    } else if (fname == "count") {
                        agg_gen.RegisterCount(i, out_idx);
                    } else if (fname == "avg") {
                        agg_gen.RegisterAvg(i, out_idx);
                    } else {
                        FAIL_STATUS(common::kCodegenUdafError, "Unknown agg function name: ", fname)
                    }
                }
            }
            res->emplace_back(agg_gen);
            agg_gen_col_seq.clear();
        }

        if (info != nullptr) {
            agg_gen_col_seq.emplace_back(col_keys[idx]);
            cur_offset = info->offset;
            cur_col_type = info->col_type;
        }
        idx += 1;
    }
    return base::Status::OK();
}

base::Status AggregateIRBuilder::BuildMulti(const std::string& base_funcname,
                                            ExprIRBuilder* expr_ir_builder,
                                            VariableIRBuilder* variable_ir_builder,
                                            ::llvm::BasicBlock* cur_block,
                                            const std::string& output_ptr_name,
                                            const vm::Schema& output_schema) {
    ::llvm::LLVMContext& llvm_ctx = module_->getContext();
    ::llvm::IRBuilder<> builder(llvm_ctx);
    auto void_ty = llvm::Type::getVoidTy(llvm_ctx);
    auto int64_ty = llvm::Type::getInt64Ty(llvm_ctx);
    expr_ir_builder->set_frame(nullptr, frame_node_);
    NativeValue window_ptr;
    CHECK_STATUS(expr_ir_builder->BuildWindow(&window_ptr))
    CHECK_TRUE(nullptr != window_ptr.GetRaw(), common::kCodegenError, "Window ptr is null")

    base::Status status;
    NativeValue output_buf_wrapper;
    CHECK_TRUE(variable_ir_builder->LoadValue(output_ptr_name, &output_buf_wrapper, status),
               common::kCodegenLoadValueError, "fail to get output row ptr")
    ::llvm::Value* output_buf = output_buf_wrapper.GetValue(&builder);

    std::string fn_name = absl::StrCat(base_funcname, "_multi_column_agg_", id_, "__");
    auto ptr_ty = llvm::Type::getInt8Ty(llvm_ctx)->getPointerTo();
    ::llvm::FunctionType* fnt = ::llvm::FunctionType::get(
        llvm::Type::getVoidTy(llvm_ctx), {ptr_ty, ptr_ty}, false);
    ::llvm::Function* fn = ::llvm::Function::Create(
        fnt, llvm::Function::ExternalLinkage, fn_name, module_);
    builder.SetInsertPoint(cur_block);
    builder.CreateCall(
        module_->getOrInsertFunction(fn_name, fnt),
        {window_ptr.GetValue(&builder), builder.CreateLoad(output_buf)});

    ::llvm::BasicBlock* head_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "head", fn);
    ::llvm::BasicBlock* enter_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "enter_iter", fn);
    ::llvm::BasicBlock* body_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "iter_body", fn);
    ::llvm::BasicBlock* exit_block =
        ::llvm::BasicBlock::Create(llvm_ctx, "exit_iter", fn);

    std::vector<StatisticalAggGenerator> generators;
    CHECK_STATUS(ScheduleAggGenerators(agg_col_infos_, &generators), common::kCodegenUdafError,
                 "Schedule agg ops failed")

    // gen head
    builder.SetInsertPoint(head_block);
    for (auto& agg_generator : generators) {
        agg_generator.GenInitState(&builder);
    }

    ::llvm::Value* input_arg = fn->arg_begin();
    ::llvm::Value* output_arg = fn->arg_begin() + 1;

    // on stack unique pointer
    size_t iter_bytes = sizeof(std::unique_ptr<codec::RowIterator>);
    ::llvm::Value* iter_ptr = CreateAllocaAtHead(
        &builder, ::llvm::Type::getInt8Ty(llvm_ctx), "row_iter",
        ::llvm::ConstantInt::get(int64_ty, iter_bytes, true));
    auto get_iter_func = module_->getOrInsertFunction(
        "hybridse_storage_get_row_iter", void_ty, ptr_ty, ptr_ty);
    builder.CreateCall(get_iter_func, {input_arg, iter_ptr});
    builder.CreateBr(enter_block);

    // gen iter begin
    builder.SetInsertPoint(enter_block);
    auto bool_ty = llvm::Type::getInt1Ty(llvm_ctx);
    auto has_next_func = module_->getOrInsertFunction(
        "hybridse_storage_row_iter_has_next",
        ::llvm::FunctionType::get(bool_ty, {ptr_ty}, false));
    ::llvm::Value* has_next = builder.CreateCall(has_next_func, iter_ptr);
    builder.CreateCondBr(has_next, body_block, exit_block);

    // gen iter body
    builder.SetInsertPoint(body_block);
    auto get_slice_func = module_->getOrInsertFunction(
        "hybridse_storage_row_iter_get_cur_slice",
        ::llvm::FunctionType::get(ptr_ty, {ptr_ty, int64_ty}, false));
    auto get_slice_size_func = module_->getOrInsertFunction(
        "hybridse_storage_row_iter_get_cur_slice_size",
        ::llvm::FunctionType::get(int64_ty, {ptr_ty, int64_ty}, false));
    std::unordered_map<size_t, std::pair<::llvm::Value*, ::llvm::Value*>>
        used_slices;

    // compute current row's slices
    for (auto& pair : agg_col_infos_) {
        size_t schema_idx = pair.second.schema_idx;

        size_t slice_idx = schema_idx;
        // TODO(tobe): Check row format before getting
        if (schema_context_->GetRowFormat() != nullptr) {
            slice_idx = schema_context_->GetRowFormat()->GetSliceId(schema_idx);
        }

        auto iter = used_slices.find(slice_idx);
        if (iter == used_slices.end()) {
            ::llvm::Value* idx_value =
                llvm::ConstantInt::get(int64_ty, slice_idx, true);

            // FIXME(ace): the two jit functions are deprecating
            //
            // It must be the order of GetSliceSize then GetSlice, or dangling reference happens
            ::llvm::Value* buf_size = builder.CreateCall(get_slice_size_func, {iter_ptr, idx_value});
            ::llvm::Value* buf_ptr = builder.CreateCall(get_slice_func, {iter_ptr, idx_value});
            used_slices[slice_idx] = {buf_ptr, buf_size};
        }
    }

    // compute row field fetches
    std::unordered_map<std::string, NativeValue> cur_row_fields_dict;
    for (auto& pair : agg_col_infos_) {
        auto& info = pair.second;
        std::string col_key = info.GetColKey();
        if (cur_row_fields_dict.find(col_key) == cur_row_fields_dict.end()) {
            size_t schema_idx = info.schema_idx;
            size_t slice_idx = schema_idx;
            // TODO(tobe): Check row format before getting
            if (schema_context_->GetRowFormat() != nullptr) {
                slice_idx = schema_context_->GetRowFormat()->GetSliceId(schema_idx);
            }

            auto& slice_info = used_slices[slice_idx];

            ScopeVar dummy_scope_var;
            BufNativeIRBuilder buf_builder(
                schema_idx, schema_context_->GetRowFormat(),
                body_block, &dummy_scope_var);
            NativeValue field_value;
            CHECK_TRUE(buf_builder.BuildGetField(info.col_idx, slice_info.first, slice_info.second, &field_value),
                       common::kCodegenGetFieldError, "fail to gen fetch column")
            cur_row_fields_dict[col_key] = field_value;
        }
    }

    // compute accumulation
    for (auto& agg_generator : generators) {
        std::vector<::llvm::Value*> fields;
        std::vector<::llvm::Value*> fields_is_null;
        for (auto& key : agg_generator.GetColKeys()) {
            auto iter = cur_row_fields_dict.find(key);
            CHECK_TRUE(iter != cur_row_fields_dict.end(), common::kCodegenUdafError, "Fail to find row field of ", key)
            auto& field_value = iter->second;
            fields.push_back(field_value.GetValue(&builder));
            fields_is_null.push_back(field_value.GetIsNull(&builder));
        }
        agg_generator.GenUpdate(&builder, fields, fields_is_null);
    }
    auto next_func = module_->getOrInsertFunction(
        "hybridse_storage_row_iter_next",
        ::llvm::FunctionType::get(void_ty, {ptr_ty}, false));
    builder.CreateCall(next_func, {iter_ptr});
    builder.CreateBr(enter_block);

    // gen iter end
    builder.SetInsertPoint(exit_block);
    auto delete_iter_func = module_->getOrInsertFunction(
        "hybridse_storage_row_iter_delete",
        ::llvm::FunctionType::get(void_ty, {ptr_ty}, false));
    builder.CreateCall(delete_iter_func, {iter_ptr});

    // store results to output row
    std::map<uint32_t, NativeValue> dummy_map;
    BufNativeEncoderIRBuilder output_encoder(&dummy_map, &output_schema,
                                             exit_block);
    for (auto& agg_generator : generators) {
        std::vector<std::pair<size_t, NativeValue>> outputs;
        agg_generator.GenOutputs(&builder, &outputs);
        for (auto pair : outputs) {
            output_encoder.BuildEncodePrimaryField(output_arg, pair.first,
                                                   pair.second);
        }
    }
    builder.CreateRetVoid();
    return base::Status::OK();
}

}  // namespace codegen
}  // namespace hybridse
