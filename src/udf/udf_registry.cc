/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_registry.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_registry.h"

#include <memory>
#include <sstream>

#include "passes/resolve_fn_and_attrs.h"

namespace fesql {
namespace udf {

Status CompositeRegistry::Transform(UDFResolveContext* ctx,
                                    node::ExprNode** result) {
    if (sub_.size() == 1) {
        return sub_[0]->Transform(ctx, result);
    }
    std::string errs;
    std::vector<node::ExprNode*> candidates;
    for (auto registry : sub_) {
        node::ExprNode* cand = nullptr;
        auto status = registry->Transform(ctx, &cand);
        if (status.isOK()) {
            candidates.push_back(cand);
            break;
        } else {
            errs.append("\n --> ").append(status.msg);
        }
    }
    CHECK_TRUE(!candidates.empty(),
               "Fail to transform udf with underlying errors: ", errs);
    *result = candidates[0];
    return Status::OK();
}

Status CompositeRegistry::ResolveFunction(UDFResolveContext* ctx,
                                          node::FnDefNode** result) {
    std::string errs;
    std::vector<node::FnDefNode*> candidates;
    for (auto registry : sub_) {
        node::FnDefNode* cand = nullptr;
        auto fn_reg = std::dynamic_pointer_cast<UDFRegistry>(registry);
        if (fn_reg == nullptr) {
            errs.append("\n --> not functional registry");
            continue;
        }
        auto status = fn_reg->ResolveFunction(ctx, &cand);
        if (status.isOK()) {
            candidates.push_back(cand);
            break;
        } else {
            errs.append("\n --> ").append(status.msg);
        }
    }
    CHECK_TRUE(!candidates.empty(),
               "Fail to resolve udf with underlying errors: ", errs);
    *result = candidates[0];
    return Status::OK();
}

Status ExprUDFRegistry::ResolveFunction(UDFResolveContext* ctx,
                                        node::FnDefNode** result) {
    // find generator with specified input argument types
    int variadic_pos = -1;
    std::shared_ptr<ExprUDFGenBase> gen_ptr;
    std::string signature;
    CHECK_STATUS(reg_table_.Find(ctx, &gen_ptr, &signature, &variadic_pos),
                 "Fail to resolve fn name \"", name(), "\"");

    DLOG(INFO) << "Resolve expression udf \"" << name() << "\" -> " << name()
               << "(" << signature << ")";

    // construct fn def node:
    // def fn(arg0, arg1, ...argN):
    //     return gen_impl(arg0, arg1, ...argN)
    auto nm = ctx->node_manager();
    std::vector<node::ExprIdNode*> func_params;
    std::vector<const node::TypeNode*> arg_types;
    std::vector<node::ExprNode*> func_params_to_gen;
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        std::string arg_name = "arg_" + std::to_string(i);
        auto arg_type = ctx->arg_type(i);
        CHECK_TRUE(arg_type != nullptr, "ExprUDF's ", i,
                   "th argument type is null, maybe error occurs in type infer "
                   "process");
        arg_types.push_back(arg_type);

        auto arg_expr =
            nm->MakeExprIdNode(arg_name, node::ExprIdNode::GetNewId());
        func_params.emplace_back(arg_expr);
        func_params_to_gen.emplace_back(arg_expr);
        arg_expr->SetOutputType(arg_type);
    }

    auto ret_expr = gen_ptr->gen(ctx, func_params_to_gen);
    CHECK_TRUE(ret_expr != nullptr && !ctx->HasError(),
               "Fail to create expr udf: ", ctx->GetError());

    auto lambda = nm->MakeLambdaNode(func_params, ret_expr);
    *result = lambda;
    return Status::OK();
}

Status ExprUDFRegistry::Register(
    const std::vector<std::string>& args,
    std::shared_ptr<ExprUDFGenBase> gen_impl_func) {
    return reg_table_.Register(args, gen_impl_func);
}

Status LLVMUDFRegistry::ResolveFunction(UDFResolveContext* ctx,
                                        node::FnDefNode** result) {
    // find generator with specified input argument types
    int variadic_pos = -1;
    std::shared_ptr<LLVMUDFGenBase> gen_ptr;
    std::string signature;
    CHECK_STATUS(reg_table_.Find(ctx, &gen_ptr, &signature, &variadic_pos),
                 "Fail to resolve fn name \"", name(), "\"");

    DLOG(INFO) << "Resolve llvm codegen udf \"" << name() << "\" -> " << name()
               << "(" << signature << ")";

    std::vector<const node::TypeNode*> arg_types;
    std::vector<int> arg_nullable;
    std::vector<const ExprAttrNode*> arg_attrs;
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        auto arg_type = ctx->arg_type(i);
        bool nullable = ctx->arg_nullable(i);
        CHECK_TRUE(arg_type != nullptr, i,
                   "th argument node type is unknown: ", name());
        arg_types.push_back(arg_type);
        arg_nullable.push_back(nullable);
        arg_attrs.push_back(new ExprAttrNode(arg_type, nullable));
    }
    ExprAttrNode out_attr(nullptr, true);
    auto status = gen_ptr->infer(ctx, arg_attrs, &out_attr);
    for (auto ptr : arg_attrs) {
        delete const_cast<ExprAttrNode*>(ptr);
    }
    CHECK_STATUS(status, "Infer llvm output attr failed: ", status.msg);

    auto return_type = out_attr.type();
    bool return_nullable = out_attr.nullable();
    CHECK_TRUE(return_type != nullptr && !ctx->HasError(),
               "Infer node return type failed: ", ctx->GetError());

    auto udf_def = dynamic_cast<node::UDFByCodeGenDefNode*>(
        ctx->node_manager()->MakeUDFByCodeGenDefNode(
            arg_types, arg_nullable, return_type, return_nullable));
    udf_def->SetGenImpl(gen_ptr);
    *result = udf_def;
    return Status::OK();
}

Status LLVMUDFRegistry::Register(
    const std::vector<std::string>& args,
    std::shared_ptr<LLVMUDFGenBase> gen_impl_func) {
    return reg_table_.Register(args, gen_impl_func);
}

Status ExternalFuncRegistry::ResolveFunction(UDFResolveContext* ctx,
                                             node::FnDefNode** result) {
    // find generator with specified input argument types
    int variadic_pos = -1;
    node::ExternalFnDefNode* external_def = nullptr;
    std::string signature;
    auto status =
        reg_table_.Find(ctx, &external_def, &signature, &variadic_pos);
    if (!status.isOK()) {
        DLOG(WARNING) << "Fail to resolve fn name \"" << name() << "\"";
        return status;
    }

    CHECK_TRUE(external_def->ret_type() != nullptr,
               "No return type specified for ", external_def->function_name());
    DLOG(INFO) << "Resolve udf \"" << name() << "\" -> "
               << external_def->function_name() << "(" << signature << ")";
    *result = external_def;
    return Status::OK();
}

Status ExternalFuncRegistry::Register(const std::vector<std::string>& args,
                                      node::ExternalFnDefNode* func) {
    return reg_table_.Register(args, func);
}

Status SimpleUDFRegistry::ResolveFunction(UDFResolveContext* ctx,
                                          node::FnDefNode** result) {
    int variadic_pos = -1;
    std::string signature;
    node::UDFDefNode* udf_def = nullptr;
    CHECK_STATUS(reg_table_.Find(ctx, &udf_def, &signature, &variadic_pos));
    *result = udf_def;
    return Status::OK();
}

Status SimpleUDFRegistry::Register(const std::vector<std::string>& args,
                                   node::UDFDefNode* udf_def) {
    return reg_table_.Register(args, udf_def);
}

Status UDAFRegistry::ResolveFunction(UDFResolveContext* ctx,
                                     node::FnDefNode** result) {
    auto nm = ctx->node_manager();
    int variadic_pos = -1;
    std::string signature;
    UDAFDefGen udaf_gen;
    CHECK_STATUS(reg_table_.Find(ctx, &udaf_gen, &signature, &variadic_pos));

    // gen init
    node::ExprNode* init_expr = nullptr;
    if (udaf_gen.init_gen != nullptr) {
        init_expr = udaf_gen.init_gen->gen(ctx, {});
        CHECK_TRUE(init_expr != nullptr);
    }

    // gen update
    node::FnDefNode* update_func = nullptr;
    std::vector<const node::TypeNode*> list_types;
    std::vector<node::ExprNode*> update_args;
    auto state_arg = nm->MakeExprIdNode("state", node::ExprIdNode::GetNewId());
    state_arg->SetOutputType(udaf_gen.state_type);
    state_arg->SetNullable(udaf_gen.state_nullable);
    update_args.push_back(state_arg);
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        auto elem_arg = nm->MakeExprIdNode("elem_" + std::to_string(i),
                                           node::ExprIdNode::GetNewId());
        auto list_type = ctx->arg_type(i);
        CHECK_TRUE(list_type != nullptr && list_type->base() == node::kList);
        elem_arg->SetOutputType(list_type->GetGenericType(0));
        elem_arg->SetNullable(list_type->IsGenericNullable(0));
        update_args.push_back(elem_arg);
        list_types.push_back(list_type);
    }
    UDFResolveContext update_ctx(update_args, nm, ctx->library());
    CHECK_TRUE(udaf_gen.update_gen != nullptr);
    CHECK_STATUS(
        udaf_gen.update_gen->ResolveFunction(&update_ctx, &update_func),
        "Resolve update function of ", name(), " failed");

    // gen merge
    node::FnDefNode* merge_func = nullptr;
    if (udaf_gen.merge_gen != nullptr) {
        UDFResolveContext merge_ctx({state_arg, state_arg}, nm, ctx->library());
        CHECK_STATUS(
            udaf_gen.merge_gen->ResolveFunction(&merge_ctx, &merge_func),
            "Resolve merge function of ", name(), " failed");
    }

    // gen output
    node::FnDefNode* output_func = nullptr;
    if (udaf_gen.output_gen != nullptr) {
        UDFResolveContext output_ctx({state_arg}, nm, ctx->library());
        CHECK_STATUS(
            udaf_gen.output_gen->ResolveFunction(&output_ctx, &output_func),
            "Resolve output function of ", name(), " failed");
    }
    *result = nm->MakeUDAFDefNode(name(), list_types, init_expr, update_func,
                                  merge_func, output_func);
    return Status::OK();
}

Status UDAFRegistry::Register(const std::vector<std::string>& args,
                              const UDAFDefGen& udaf_gen) {
    return reg_table_.Register(args, udaf_gen);
}

}  // namespace udf
}  // namespace fesql
