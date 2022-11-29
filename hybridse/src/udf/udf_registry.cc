/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "udf/udf_registry.h"

#include <memory>
#include <sstream>

#include "passes/resolve_fn_and_attrs.h"
#include "udf/openmldb_udf.h"

using ::hybridse::common::kCodegenError;

namespace hybridse {
namespace udf {

Status ArgSignatureTable::Find(UdfResolveContext* ctx, std::shared_ptr<UdfRegistry>* res, std::string* signature,
            int* variadic_pos) {
    std::vector<const node::TypeNode*> arg_types;
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        arg_types.push_back(ctx->arg_type(i));
    }
    return Find(arg_types, res, signature, variadic_pos);
}

Status ArgSignatureTable::Find(const std::vector<const node::TypeNode*>& arg_types, std::shared_ptr<UdfRegistry>* res,
            std::string* signature, int* variadic_pos) {
    std::stringstream ss;
    for (size_t i = 0; i < arg_types.size(); ++i) {
        auto type_node = arg_types[i];
        if (type_node == nullptr) {
            ss << "?";
        } else {
            ss << type_node->GetName();
        }
        if (i < arg_types.size() - 1) {
            ss << ", ";
        }
    }

    // There are four match conditions:
    // (1) explicit match without placeholders
    // (2) explicit match with placeholders
    // (3) variadic match without placeholders
    // (4) variadic match with placeholders
    // The priority is (1) > (2) > (3) > (4)
    typename TableType::iterator placeholder_match_iter = table_.end();
    typename TableType::iterator variadic_placeholder_match_iter =
        table_.end();
    typename TableType::iterator variadic_match_iter = table_.end();
    int variadic_match_pos = -1;
    int variadic_placeholder_match_pos = -1;

    for (auto iter = table_.begin(); iter != table_.end(); ++iter) {
        auto& def_item = iter->second;
        auto& def_arg_types = def_item.arg_types;
        if (def_item.is_variadic) {
            // variadic match
            bool match = true;
            bool placeholder_match = false;
            int non_variadic_arg_num = def_arg_types.size();
            if (arg_types.size() <
                static_cast<size_t>(non_variadic_arg_num)) {
                continue;
            }
            for (int j = 0; j < non_variadic_arg_num; ++j) {
                if (def_arg_types[j] == nullptr) {  // any arg
                    placeholder_match = true;
                    match = false;
                } else if (!node::TypeEquals(def_arg_types[j],
                                             arg_types[j])) {
                    placeholder_match = false;
                    match = false;
                    break;
                }
            }
            if (match) {
                if (variadic_match_pos < non_variadic_arg_num) {
                    placeholder_match_iter = iter;
                    variadic_match_pos = non_variadic_arg_num;
                }
            } else if (placeholder_match) {
                if (variadic_placeholder_match_pos < non_variadic_arg_num) {
                    variadic_placeholder_match_iter = iter;
                    variadic_placeholder_match_pos = non_variadic_arg_num;
                }
            }

        } else if (arg_types.size() == def_arg_types.size()) {
            // explicit match
            bool match = true;
            bool placeholder_match = false;
            for (size_t j = 0; j < arg_types.size(); ++j) {
                if (def_arg_types[j] == nullptr) {
                    placeholder_match = true;
                    match = false;
                } else if (!node::TypeEquals(def_arg_types[j],
                                             arg_types[j])) {
                    placeholder_match = false;
                    match = false;
                    break;
                }
            }
            if (match) {
                *variadic_pos = -1;
                *signature = iter->first;
                *res = def_item.value;
                return Status::OK();
            } else if (placeholder_match) {
                placeholder_match_iter = iter;
            }
        }
    }

    if (placeholder_match_iter != table_.end()) {
        *variadic_pos = -1;
        *signature = placeholder_match_iter->first;
        *res = placeholder_match_iter->second.value;
        return Status::OK();
    } else if (variadic_match_iter != table_.end()) {
        *variadic_pos = variadic_match_pos;
        *signature = variadic_match_iter->first;
        *res = variadic_match_iter->second.value;
        return Status::OK();
    } else if (variadic_placeholder_match_iter != table_.end()) {
        *variadic_pos = variadic_placeholder_match_pos;
        *signature = variadic_placeholder_match_iter->first;
        *res = variadic_placeholder_match_iter->second.value;
        return Status::OK();
    } else {
        return Status(common::kCodegenError,
                      "Resolve udf signature failure: <" + ss.str() + ">");
    }
}

Status ArgSignatureTable::Register(const std::vector<const node::TypeNode*>& args,
                bool is_variadic, const std::shared_ptr<UdfRegistry>& t) {
    std::stringstream ss;
    for (size_t i = 0; i < args.size(); ++i) {
        if (args[i] == nullptr) {
            ss << "?";
        } else {
            ss << args[i]->GetName();
        }
        if (i < args.size() - 1) {
            ss << ", ";
        }
    }
    std::string key = ss.str();
    auto iter = table_.find(key);
    CHECK_TRUE(iter == table_.end(), common::kCodegenError,
               "Duplicate signature: ", key);
    table_.insert(iter, {key, DefItem(t, args, is_variadic)});
    return Status::OK();
}

const std::string UdfResolveContext::GetArgSignature() const {
    return hybridse::udf::GetArgSignature(args_);
}

Status ExprUdfRegistry::ResolveFunction(UdfResolveContext* ctx,
                                        node::FnDefNode** result) {
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
        CHECK_TRUE(arg_type != nullptr, kCodegenError, "ExprUdf's ", i,
                   "th argument type is null, maybe error occurs in type infer "
                   "process");
        arg_types.push_back(arg_type);

        auto arg_expr = nm->MakeExprIdNode(arg_name);
        func_params.emplace_back(arg_expr);
        func_params_to_gen.emplace_back(arg_expr);
        arg_expr->SetOutputType(arg_type);
    }

    auto ret_expr = gen_impl_func_->gen(ctx, func_params_to_gen);
    CHECK_TRUE(ret_expr != nullptr && !ctx->HasError(), kCodegenError,
               "Fail to resolve expr udf: ", ctx->GetError());

    auto lambda = nm->MakeLambdaNode(func_params, ret_expr);
    *result = lambda;
    return Status::OK();
}

Status LlvmUdfRegistry::ResolveFunction(UdfResolveContext* ctx,
                                        node::FnDefNode** result) {
    std::vector<const node::TypeNode*> arg_types;
    std::vector<const ExprAttrNode*> arg_attrs;
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        auto arg_type = ctx->arg_type(i);
        bool nullable = ctx->arg_nullable(i);
        CHECK_TRUE(arg_type != nullptr, kCodegenError, i,
                   "th argument node type is unknown: ", name());
        arg_types.push_back(arg_type);
        arg_attrs.push_back(new ExprAttrNode(arg_type, nullable));
    }
    ExprAttrNode out_attr(nullptr, true);
    auto status = gen_impl_func_->infer(ctx, arg_attrs, &out_attr);
    for (auto ptr : arg_attrs) {
        delete const_cast<ExprAttrNode*>(ptr);
    }
    CHECK_STATUS(status, "Infer llvm output attr failed: ", status.str());

    auto return_type = out_attr.type();
    bool return_nullable = out_attr.nullable();
    CHECK_TRUE(return_type != nullptr && !ctx->HasError(), kCodegenError,
               "Infer node return type failed: ", ctx->GetError());

    std::vector<int> arg_nullable(arg_types.size(), false);
    for (size_t pos : nullable_arg_indices_) {
        arg_nullable[pos] = true;
    }
    for (size_t pos = fixed_arg_size_; pos < arg_nullable.size(); ++pos) {
        arg_nullable[pos] = true;
    }

    auto udf_def = dynamic_cast<node::UdfByCodeGenDefNode*>(
        ctx->node_manager()->MakeUdfByCodeGenDefNode(
            name(), arg_types, arg_nullable, return_type, return_nullable));
    udf_def->SetGenImpl(gen_impl_func_);
    *result = udf_def;
    return Status::OK();
}

Status ExternalFuncRegistry::ResolveFunction(UdfResolveContext* ctx,
                                             node::FnDefNode** result) {
    CHECK_TRUE(extern_def_->ret_type() != nullptr, kCodegenError,
               "No return type specified for ", extern_def_->function_name());
    DLOG(INFO) << "Resolve udf \"" << name() << "\" -> "
               << extern_def_->GetFlatString();
    *result = extern_def_;
    return Status::OK();
}

Status DynamicUdfRegistry::ResolveFunction(UdfResolveContext* ctx,
                                             node::FnDefNode** result) {
    CHECK_TRUE(extern_def_->ret_type() != nullptr, kCodegenError,
               "No return type specified for ", extern_def_->GetName());
    DLOG(INFO) << "Resolve udf \"" << name() << "\" -> " << extern_def_->GetFlatString();
    *result = extern_def_;
    return Status::OK();
}

Status DynamicUdafRegistry::ResolveFunction(UdfResolveContext* ctx,
                                             node::FnDefNode** result) {
    CHECK_TRUE(extern_def_->ret_type() != nullptr, kCodegenError,
               "No return type specified for ", extern_def_->GetName());
    DLOG(INFO) << "Resolve udaf \"" << name() << "\" -> " << extern_def_->GetFlatString();
    *result = extern_def_;
    return Status::OK();
}

Status SimpleUdfRegistry::ResolveFunction(UdfResolveContext* ctx,
                                          node::FnDefNode** result) {
    *result = fn_def_;
    return Status::OK();
}

Status UdafRegistry::ResolveFunction(UdfResolveContext* ctx,
                                     node::FnDefNode** result) {
    // gen init
    node::ExprNode* init_expr = nullptr;
    if (udaf_gen_.init_gen != nullptr) {
        init_expr = udaf_gen_.init_gen->gen(ctx, {});
        CHECK_TRUE(init_expr != nullptr, kCodegenError);
    }

    // gen update
    auto nm = ctx->node_manager();
    node::FnDefNode* update_func = nullptr;
    std::vector<const node::TypeNode*> list_types;
    std::vector<node::ExprNode*> update_args;
    auto state_arg = nm->MakeExprIdNode("state");
    state_arg->SetOutputType(udaf_gen_.state_type);
    state_arg->SetNullable(udaf_gen_.state_nullable);
    update_args.push_back(state_arg);
    for (size_t i = 0; i < ctx->arg_size(); ++i) {
        auto elem_arg = nm->MakeExprIdNode("elem_" + std::to_string(i));
        auto list_type = ctx->arg_type(i);
        CHECK_TRUE(list_type != nullptr && list_type->base() == node::kList,
                   kCodegenError);
        elem_arg->SetOutputType(list_type->GetGenericType(0));
        elem_arg->SetNullable(list_type->IsGenericNullable(0));
        update_args.push_back(elem_arg);
        list_types.push_back(list_type);
    }
    UdfResolveContext update_ctx(update_args, nm, ctx->library());
    CHECK_TRUE(udaf_gen_.update_gen != nullptr, kCodegenError);
    CHECK_STATUS(
        udaf_gen_.update_gen->ResolveFunction(&update_ctx, &update_func),
        "Resolve update function of ", name(), " failed");

    // gen merge
    node::FnDefNode* merge_func = nullptr;
    if (udaf_gen_.merge_gen != nullptr) {
        UdfResolveContext merge_ctx({state_arg, state_arg}, nm, ctx->library());
        CHECK_STATUS(
            udaf_gen_.merge_gen->ResolveFunction(&merge_ctx, &merge_func),
            "Resolve merge function of ", name(), " failed");
    }

    // gen output
    node::FnDefNode* output_func = nullptr;
    if (udaf_gen_.output_gen != nullptr) {
        UdfResolveContext output_ctx({state_arg}, nm, ctx->library());
        CHECK_STATUS(
            udaf_gen_.output_gen->ResolveFunction(&output_ctx, &output_func),
            "Resolve output function of ", name(), " failed");
    }
    *result = nm->MakeUdafDefNode(name(), list_types, init_expr, update_func,
                                  merge_func, output_func);
    return Status::OK();
}

DynamicUdfRegistryHelper::DynamicUdfRegistryHelper(const std::string& basename, UdfLibrary* library, void* fn,
        node::DataType return_type, const std::vector<node::DataType>& arg_types, void* udfcontext_fun)
    : UdfRegistryHelper(basename, library), fn_name_(basename), fn_ptr_(fn), udfcontext_fun_ptr_(udfcontext_fun) {
    auto nm = node_manager();
    return_type_ = nm->MakeTypeNode(return_type);
    for (const auto type : arg_types) {
        auto type_node = nm->MakeTypeNode(type);
        arg_types_.emplace_back(type_node);
        fn_name_.append(".").append(type_node->GetName());
        arg_nullable_.emplace_back(0);
    }
    switch (return_type) {
        case node::kVarchar:
        case node::kDate:
        case node::kTimestamp:
            return_by_arg_ = true;
            break;
        default:
            return_by_arg_ = false;
    }
}

Status DynamicUdfRegistryHelper::Register() {
    if (fn_ptr_ == nullptr || udfcontext_fun_ptr_ == nullptr) {
        LOG(WARNING) << "fun_ptr or udfcontext_fun_ptr is null";
        return Status(kCodegenError, "fun_ptr or udfcontext_fun_ptr is null");
    }
    if (return_type_ == nullptr) {
        LOG(WARNING) << "No return type specified for udf registry " << name();
        return Status(kCodegenError, "No return type specified for udf registry");;
    }
    std::string init_context_fn_name = "init_udfcontext.opaque";
    auto type_node = node_manager()->MakeOpaqueType(sizeof(UDFContext));
    auto init_context_node = node_manager()->MakeExternalFnDefNode(
            init_context_fn_name, udfcontext_fun_ptr_, type_node, false, {}, {}, -1, true);
    auto def = node_manager()->MakeDynamicUdfFnDefNode(
        fn_name_, fn_ptr_, return_type_, return_nullable_, arg_types_,
        arg_nullable_, return_by_arg_, init_context_node);
    auto registry = std::make_shared<DynamicUdfRegistry>(name(), def);
    library()->AddExternalFunction(fn_name_, fn_ptr_);
    this->InsertRegistry(arg_types_, false, registry);
    LOG(INFO) << "register function success. name: " << fn_name_ << " return type:" << return_type_->GetName();
    return Status::OK();
}

}  // namespace udf
}  // namespace hybridse
