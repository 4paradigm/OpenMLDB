/**
 * Copyright (c) 2024 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/insert_row_builder.h"

#include <map>
#include <string>
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "base/fe_status.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/context.h"
#include "codegen/expr_ir_builder.h"
#include "node/node_manager.h"
#include "passes/resolve_fn_and_attrs.h"
#include "udf/default_udf_library.h"
#include "vm/engine.h"
#include "vm/jit_wrapper.h"

namespace hybridse {
namespace codegen {

InsertRowBuilder::InsertRowBuilder(const codec::Schema* schema) : schema_(schema) {}

absl::Status InsertRowBuilder::Init() {
    ::hybridse::vm::Engine::InitializeGlobalLLVM();

    jit_ = std::unique_ptr<vm::HybridSeJitWrapper>(vm::HybridSeJitWrapper::Create());
    if (!jit_->Init()) {
        jit_ = nullptr;
        return absl::InternalError("fail to init jit");
    }
    if (!vm::HybridSeJitWrapper::InitJitSymbols(jit_.get())) {
        jit_ = nullptr;
        return absl::InternalError("fail to init jit symbols");
    }
    return absl::OkStatus();
}

absl::StatusOr<std::shared_ptr<int8_t>> InsertRowBuilder::ComputeRow(const node::ExprListNode* values) {
    EnsureInitialized();
    return ComputeRow(values->children_);
}

absl::StatusOr<std::shared_ptr<int8_t>> InsertRowBuilder::ComputeRow(absl::Span<node::ExprNode* const> values) {
    EnsureInitialized();

    std::unique_ptr<llvm::LLVMContext> llvm_ctx = llvm::make_unique<llvm::LLVMContext>();
    std::unique_ptr<llvm::Module> llvm_module = llvm::make_unique<llvm::Module>("insert_row_builder", *llvm_ctx);
    vm::SchemasContext empty_sc;
    node::NodeManager nm;
    codec::Schema empty_param_types;
    CodeGenContext dump_ctx(llvm_module.get(), &empty_sc, &empty_param_types, &nm);

    auto library = udf::DefaultUdfLibrary::get();
    node::ExprAnalysisContext expr_ctx(&nm, library, &empty_sc, &empty_param_types);
    passes::ResolveFnAndAttrs resolver(&expr_ctx);

    std::vector<node::ExprNode*> transformed;
    for (auto& expr : values) {
        node::ExprNode* out = nullptr;
        CHECK_STATUS_TO_ABSL(resolver.VisitExpr(expr, &out));
        transformed.push_back(out);
    }

    std::string fn_name = absl::StrCat("gen_insert_row_", fn_counter_++);
    auto fs = BuildFn(&dump_ctx, fn_name, transformed);
    CHECK_ABSL_STATUSOR(fs);

    llvm::Function* fn = fs.value();

    if (!jit_->OptModule(llvm_module.get())) {
        return absl::InternalError("fail to optimize module");
    }

    if (!jit_->AddModule(std::move(llvm_module), std::move(llvm_ctx))) {
        return absl::InternalError("add llvm module failed");
    }

    auto c_fn = jit_->FindFunction(fn->getName());
    void (*encode)(int8_t**) = reinterpret_cast<void (*)(int8_t**)>(const_cast<int8_t*>(c_fn));

    int8_t* insert_row = nullptr;
    encode(&insert_row);

    auto managed_row = std::shared_ptr<int8_t>(insert_row, std::free);

    return managed_row;
}

absl::StatusOr<llvm::Function*> InsertRowBuilder::BuildFn(CodeGenContext* ctx, llvm::StringRef fn_name,
                                                          absl::Span<node::ExprNode* const> values) {
    llvm::Function* fn = ctx->GetModule()->getFunction(fn_name);
    if (fn == nullptr) {
        auto builder = ctx->GetBuilder();
        llvm::FunctionType* fnt = llvm::FunctionType::get(builder->getVoidTy(),
                                                          {
                                                              builder->getInt8PtrTy()->getPointerTo(),
                                                          },
                                                          false);

        fn = llvm::Function::Create(fnt, llvm::GlobalValue::ExternalLinkage, fn_name, ctx->GetModule());
        FunctionScopeGuard fg(fn, ctx);

        llvm::Value* row_ptr_ptr = fn->arg_begin();

        ExprIRBuilder expr_builder(ctx);

        std::map<uint32_t, NativeValue> columns;
        for (uint32_t i = 0; i < values.size(); ++i) {
            auto expr = values[i];

            NativeValue out;
            auto s = expr_builder.Build(expr, &out);
            CHECK_STATUS_TO_ABSL(s);

            columns[i] = out;
        }

        BufNativeEncoderIRBuilder encode_builder(ctx, &columns, schema_);
        CHECK_STATUS_TO_ABSL(encode_builder.Init());

        encode_builder.BuildEncode(row_ptr_ptr);

        builder->CreateRetVoid();
    }

    return fn;
}

// build the function that transform a single insert row values into encoded row
absl::StatusOr<llvm::Function*> InsertRowBuilder::BuildEncodeFn() { return absl::OkStatus(); }
}  // namespace codegen
}  // namespace hybridse
