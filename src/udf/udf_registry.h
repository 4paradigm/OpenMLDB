/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_registry.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_UDF_REGISTRY_H_
#define SRC_UDF_UDF_REGISTRY_H_

#include <memory>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/fe_status.h"
#include "codec/list_iterator_codec.h"
#include "codegen/context.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/literal_traits.h"
#include "udf/udf_library.h"

namespace fesql {
namespace udf {

using fesql::base::Status;
using fesql::codec::StringRef;
using fesql::node::ExprListNode;
using fesql::node::ExprNode;
using fesql::node::SQLNode;

/**
 * Overall information to resolve a sql function call.
 */
class UDFResolveContext {
 public:
    UDFResolveContext(ExprListNode* args, const node::SQLNode* over,
                      node::ExprAnalysisContext* analysis_ctx)
        : args_(args),
          over_(over),
          manager_(analysis_ctx->node_manager()),
          analysis_ctx_(analysis_ctx) {}

    ExprListNode* args() { return args_; }
    const node::SQLNode* over() { return over_; }
    node::NodeManager* node_manager() { return manager_; }

    size_t arg_size() const { return args_->GetChildNum(); }
    const ExprNode* arg(size_t i) const { return args_->GetChild(i); }

    const std::string& GetError() const { return error_msg_; }
    void SetError(const std::string& err) { error_msg_ = err; }
    bool HasError() const { return error_msg_ != ""; }

    node::ExprAnalysisContext* analysis_context() const {
        return analysis_ctx_;
    }

 private:
    ExprListNode* args_;
    const SQLNode* over_;
    node::NodeManager* manager_;

    node::ExprAnalysisContext* analysis_ctx_;
    std::string error_msg_;
};

/**
 * Interface to implement resolve and transform
 * logic for sql function call with fn name.
 */
class UDFTransformRegistry {
 public:
    explicit UDFTransformRegistry(const std::string& name) : name_(name) {}

    // transform "f(arg0, arg1, ...argN)" -> some expression
    virtual Status Transform(UDFResolveContext* ctx,
                             node::ExprNode** result) = 0;

    virtual ~UDFTransformRegistry() {}

    const std::string& name() const { return name_; }

 private:
    std::string name_;
};

/**
 * Interface to implement resolve logic for sql function
 * call without extra transformation.
 */
class UDFRegistry : public UDFTransformRegistry {
 public:
    explicit UDFRegistry(const std::string& name)
        : UDFTransformRegistry(name) {}

    // "f(arg0, arg1, ...argN)" -> resolved f
    virtual Status ResolveFunction(UDFResolveContext* ctx,
                                   node::FnDefNode** result) = 0;

    virtual ~UDFRegistry() {}

    Status Transform(UDFResolveContext* ctx, node::ExprNode** result) override {
        node::FnDefNode* fn_def = nullptr;
        CHECK_STATUS(ResolveFunction(ctx, &fn_def));

        *result =
            ctx->node_manager()->MakeFuncNode(fn_def, ctx->args(), ctx->over());
        return Status::OK();
    }
};

class CompositeRegistry : public UDFTransformRegistry {
 public:
    explicit CompositeRegistry(const std::string& name)
        : UDFTransformRegistry(name) {}

    void Add(std::shared_ptr<UDFTransformRegistry> item) {
        sub_.push_back(item);
    }

    Status Transform(UDFResolveContext* ctx, node::ExprNode** result) override;

    const std::vector<std::shared_ptr<UDFTransformRegistry>>& GetSubRegistries()
        const {
        return sub_;
    }

 private:
    std::vector<std::shared_ptr<UDFTransformRegistry>> sub_;
};

template <typename T>
class ArgSignatureTable {
 public:
    Status Find(UDFResolveContext* ctx, T* res, std::string* signature,
                int* variadic_pos) {
        std::stringstream ss;
        std::vector<std::string> input_args;
        for (size_t i = 0; i < ctx->arg_size(); ++i) {
            auto type_node = ctx->arg(i)->GetOutputType();
            if (type_node == nullptr) {
                input_args.emplace_back("?");
                ss << "?";
            } else {
                input_args.emplace_back(type_node->GetName());
                ss << type_node->GetName();
            }
            if (i < ctx->arg_size() - 1) {
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
            auto& def_args = iter->second.second;
            if (def_args.size() > 0 && def_args.back() == "...") {
                // variadic match
                bool match = true;
                bool placeholder_match = false;
                int non_variadic_arg_num = def_args.size() - 1;
                if (input_args.size() <
                    static_cast<size_t>(non_variadic_arg_num)) {
                    continue;
                }
                for (int j = 0; j < non_variadic_arg_num; ++j) {
                    if (def_args[j] == "?") {
                        placeholder_match = true;
                        match = false;
                    } else if (def_args[j] != input_args[j]) {
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

            } else if (input_args.size() == def_args.size()) {
                // explicit match
                bool match = true;
                bool placeholder_match = false;
                for (size_t j = 0; j < input_args.size(); ++j) {
                    if (def_args[j] == "?") {
                        placeholder_match = true;
                        match = false;
                    } else if (def_args[j] != input_args[j]) {
                        placeholder_match = false;
                        match = false;
                        break;
                    }
                }
                if (match) {
                    *variadic_pos = -1;
                    *signature = iter->first;
                    *res = iter->second.first;
                    return Status::OK();
                } else if (placeholder_match) {
                    placeholder_match_iter = iter;
                }
            }
        }

        if (placeholder_match_iter != table_.end()) {
            *variadic_pos = -1;
            *signature = placeholder_match_iter->first;
            *res = placeholder_match_iter->second.first;
            return Status::OK();
        } else if (variadic_match_iter != table_.end()) {
            *variadic_pos = variadic_match_pos;
            *signature = variadic_match_iter->first;
            *res = variadic_match_iter->second.first;
            return Status::OK();
        } else if (variadic_placeholder_match_iter != table_.end()) {
            *variadic_pos = variadic_placeholder_match_pos;
            *signature = variadic_placeholder_match_iter->first;
            *res = variadic_placeholder_match_iter->second.first;
            return Status::OK();
        } else {
            return Status(common::kCodegenError,
                          "Resolve udf signature failure: <" + ss.str() + ">");
        }
    }

    Status Register(const std::vector<std::string>& args, const T& t) {
        std::stringstream ss;
        for (size_t i = 0; i < args.size(); ++i) {
            ss << args[i];
            if (i < args.size() - 1) {
                ss << ", ";
            }
        }
        std::string key = ss.str();
        auto iter = table_.find(key);
        CHECK_TRUE(iter == table_.end(), "Duplicate signature: ", key);
        table_.insert(iter, std::make_pair(key, std::make_pair(t, args)));
        return Status::OK();
    }

    using TableType =
        std::unordered_map<std::string, std::pair<T, std::vector<std::string>>>;

    const TableType& GetTable() const { return table_; }

 private:
    TableType table_;
};

struct ExprUDFGenBase {
    virtual ExprNode* gen(UDFResolveContext* ctx,
                          const std::vector<ExprNode*>& args) = 0;
};

template <typename... LiteralArgTypes>
struct ExprUDFGen : public ExprUDFGenBase {
    using FType = std::function<ExprNode*(
        UDFResolveContext*,
        typename std::pair<LiteralArgTypes, ExprNode*>::second_type...)>;

    ExprNode* gen(UDFResolveContext* ctx,
                  const std::vector<ExprNode*>& args) override {
        return gen_internal(ctx, args,
                            std::index_sequence_for<LiteralArgTypes...>());
    }

    template <std::size_t... I>
    ExprNode* gen_internal(UDFResolveContext* ctx,
                           const std::vector<ExprNode*>& args,
                           const std::index_sequence<I...>&) {
        return gen_func(ctx, args[I]...);
    }

    explicit ExprUDFGen(const FType& f) : gen_func(f) {}
    const FType gen_func;
};

template <typename... LiteralArgTypes>
struct VariadicExprUDFGen : public ExprUDFGenBase {
    using FType = std::function<ExprNode*(
        UDFResolveContext*,
        typename std::pair<LiteralArgTypes, ExprNode*>::second_type...,
        const std::vector<ExprNode*>&)>;

    ExprNode* gen(UDFResolveContext* ctx,
                  const std::vector<ExprNode*>& args) override {
        return gen_internal(ctx, args,
                            std::index_sequence_for<LiteralArgTypes...>());
    };

    template <std::size_t... I>
    ExprNode* gen_internal(UDFResolveContext* ctx,
                           const std::vector<ExprNode*>& args,
                           const std::index_sequence<I...>&) {
        std::vector<ExprNode*> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        return this->gen_func(ctx, args[I]..., variadic_args);
    }

    explicit VariadicExprUDFGen(const FType& f) : gen_func(f) {}
    const FType gen_func;
};

template <typename RegistryT>
class UDFRegistryHelper {
 public:
    explicit UDFRegistryHelper(std::shared_ptr<RegistryT> registry,
                               UDFLibrary* library)
        : registry_(registry), library_(library) {}

    std::shared_ptr<RegistryT> registry() const { return registry_; }

    UDFLibrary* library() const { return library_; }

    node::NodeManager* node_manager() const { return library_->node_manager(); }

 private:
    std::shared_ptr<RegistryT> registry_;
    UDFLibrary* library_;
};

/**
 * Interface to resolve udf with expression construction.
 */
class ExprUDFRegistry : public UDFRegistry {
 public:
    explicit ExprUDFRegistry(const std::string& name)
        : UDFRegistry(name), allow_window_(true), allow_project_(true) {}

    Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

    Status Register(const std::vector<std::string>& args,
                    std::shared_ptr<ExprUDFGenBase> gen_impl_func);

    void SetAllowWindow(bool flag) { this->allow_window_ = flag; }

    void SetAllowProject(bool flag) { this->allow_project_ = flag; }

 private:
    ArgSignatureTable<std::shared_ptr<ExprUDFGenBase>> reg_table_;
    bool allow_window_;
    bool allow_project_;
};

class ExprUDFRegistryHelper : public UDFRegistryHelper<ExprUDFRegistry> {
 public:
    explicit ExprUDFRegistryHelper(std::shared_ptr<ExprUDFRegistry> registry,
                                   UDFLibrary* library)
        : UDFRegistryHelper<ExprUDFRegistry>(registry, library) {}

    template <typename... LiteralArgTypes>
    ExprUDFRegistryHelper& args(
        const typename ExprUDFGen<LiteralArgTypes...>::FType& func) {
        auto gen_ptr = std::make_shared<ExprUDFGen<LiteralArgTypes...>>(func);
        registry()->Register({DataTypeTrait<LiteralArgTypes>::to_string()...},
                             gen_ptr);
        return *this;
    }

    template <typename... LiteralArgTypes>
    ExprUDFRegistryHelper& variadic_args(
        const typename VariadicExprUDFGen<LiteralArgTypes...>::FType& func) {
        auto gen_ptr =
            std::make_shared<VariadicExprUDFGen<LiteralArgTypes...>>(func);
        registry()->Register(
            {DataTypeTrait<LiteralArgTypes>::to_string()..., "..."}, gen_ptr);
        return *this;
    }

    ExprUDFRegistryHelper& allow_project(bool flag) {
        registry()->SetAllowProject(flag);
        return *this;
    }

    ExprUDFRegistryHelper& allow_window(bool flag) {
        registry()->SetAllowWindow(flag);
        return *this;
    }
};

class LLVMUDFGenBase {
 public:
    virtual Status gen(codegen::CodeGenContext* ctx,
                       const std::vector<codegen::NativeValue>& args,
                       codegen::NativeValue* res) = 0;

    virtual const node::TypeNode* infer(
        UDFResolveContext* ctx,
        const std::vector<const node::TypeNode*>& args) = 0;

    node::TypeNode* fixed_ret_type() const { return fixed_ret_type_; }

    void SetFixedReturnType(node::TypeNode* dtype) {
        this->fixed_ret_type_ = dtype;
    }

 private:
    node::TypeNode* fixed_ret_type_ = nullptr;
};

template <typename... LiteralArgTypes>
struct LLVMUDFGen : public LLVMUDFGenBase {
    using FType = std::function<Status(
        codegen::CodeGenContext* ctx,
        typename std::pair<LiteralArgTypes,
                           codegen::NativeValue>::second_type...,
        codegen::NativeValue*)>;

    using InferFType = std::function<const node::TypeNode*(
        UDFResolveContext*,
        typename std::pair<LiteralArgTypes,
                           const node::TypeNode*>::second_type...)>;

    Status gen(codegen::CodeGenContext* ctx,
               const std::vector<codegen::NativeValue>& args,
               codegen::NativeValue* result) override {
        return gen_internal(ctx, args, result,
                            std::index_sequence_for<LiteralArgTypes...>());
    }

    template <std::size_t... I>
    Status gen_internal(codegen::CodeGenContext* ctx,
                        const std::vector<codegen::NativeValue>& args,
                        codegen::NativeValue* result,
                        const std::index_sequence<I...>&) {
        return gen_func(ctx, args[I]..., result);
    }

    const node::TypeNode* infer(
        UDFResolveContext* ctx,
        const std::vector<const node::TypeNode*>& args) override {
        return infer_internal(ctx, args,
                              std::index_sequence_for<LiteralArgTypes...>());
    }

    template <std::size_t... I>
    const node::TypeNode* infer_internal(
        UDFResolveContext* ctx, const std::vector<const node::TypeNode*>& args,
        const std::index_sequence<I...>&) {
        return infer_func(ctx, args[I]...);
    }

    LLVMUDFGen(const FType& f, const InferFType& infer)
        : gen_func(f), infer_func(infer) {}

    explicit LLVMUDFGen(const FType& f)
        : gen_func(f), infer_func([this](...) { return fixed_ret_type(); }) {}

    const FType gen_func;
    const InferFType infer_func;
};

template <typename... LiteralArgTypes>
struct VariadicLLVMUDFGen : public LLVMUDFGenBase {
    using FType = std::function<Status(
        codegen::CodeGenContext*,
        typename std::pair<LiteralArgTypes,
                           codegen::NativeValue>::second_type...,
        const std::vector<codegen::NativeValue>&, codegen::NativeValue*)>;

    using InferFType = std::function<const node::TypeNode*(
        UDFResolveContext*,
        typename std::pair<LiteralArgTypes,
                           const node::TypeNode*>::second_type...,
        const std::vector<codegen::NativeValue>&)>;

    Status gen(codegen::CodeGenContext* ctx,
               const std::vector<codegen::NativeValue>& args,
               codegen::NativeValue* result) override {
        return gen_internal(ctx, args, result,
                            std::index_sequence_for<LiteralArgTypes...>());
    };

    template <std::size_t... I>
    Status gen_internal(codegen::CodeGenContext* ctx,
                        const std::vector<codegen::NativeValue>& args,
                        codegen::NativeValue* result,
                        const std::index_sequence<I...>&) {
        std::vector<codegen::NativeValue> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        return this->gen_func(ctx, args[I]..., variadic_args, result);
    }

    const node::TypeNode* infer(
        UDFResolveContext* ctx,
        const std::vector<const node::TypeNode*>& args) override {
        return infer_internal(ctx, args,
                              std::index_sequence_for<LiteralArgTypes...>());
    }

    template <std::size_t... I>
    const node::TypeNode* infer_internal(
        UDFResolveContext* ctx, const std::vector<const node::TypeNode*>& args,
        const std::index_sequence<I...>&) {
        std::vector<node::TypeNode*> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        return this->infer_func(ctx, args[I]..., variadic_args);
    }

    VariadicLLVMUDFGen(const FType& f, const InferFType& infer)
        : gen_func(f), infer_func(infer) {}

    explicit VariadicLLVMUDFGen(const FType& f)
        : gen_func(f), infer_func([this](...) { return fixed_ret_type(); }) {}

    const FType gen_func;
    const InferFType infer_func;
};

/**
 * Interface to resolve udf with llvm codegen construction.
 */
class LLVMUDFRegistry : public UDFRegistry {
 public:
    explicit LLVMUDFRegistry(const std::string& name)
        : UDFRegistry(name), allow_window_(true), allow_project_(true) {}

    Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

    Status Register(const std::vector<std::string>& args,
                    std::shared_ptr<LLVMUDFGenBase> gen_impl_func);

    void SetAllowWindow(bool flag) { this->allow_window_ = flag; }

    void SetAllowProject(bool flag) { this->allow_project_ = flag; }

 private:
    ArgSignatureTable<std::shared_ptr<LLVMUDFGenBase>> reg_table_;
    bool allow_window_;
    bool allow_project_;
};

class LLVMUDFRegistryHelper : public UDFRegistryHelper<LLVMUDFRegistry> {
 public:
    LLVMUDFRegistryHelper(std::shared_ptr<LLVMUDFRegistry> registry,
                          UDFLibrary* library)
        : UDFRegistryHelper<LLVMUDFRegistry>(registry, library) {}

    LLVMUDFRegistryHelper(const LLVMUDFRegistryHelper& other)
        : UDFRegistryHelper<LLVMUDFRegistry>(other.registry(),
                                             other.library()) {}

    LLVMUDFRegistryHelper& allow_project(bool flag) {
        registry()->SetAllowProject(flag);
        return *this;
    }

    LLVMUDFRegistryHelper& allow_window(bool flag) {
        registry()->SetAllowWindow(flag);
        return *this;
    }

    template <typename RetType>
    LLVMUDFRegistryHelper& returns() {
        fixed_ret_type_ =
            DataTypeTrait<RetType>::to_type_node(library()->node_manager());
        if (cur_def_ != nullptr) {
            cur_def_->SetFixedReturnType(fixed_ret_type_);
        }
        return *this;
    }

    template <typename... LiteralArgTypes>
    LLVMUDFRegistryHelper& args(
        const typename LLVMUDFGen<LiteralArgTypes...>::FType& gen) {
        return args<LiteralArgTypes...>([](...) { return nullptr; }, gen);
    }

    template <typename... LiteralArgTypes>
    LLVMUDFRegistryHelper& args(
        const typename LLVMUDFGen<LiteralArgTypes...>::InferFType& infer,
        const typename LLVMUDFGen<LiteralArgTypes...>::FType& gen) {
        cur_def_ = std::make_shared<LLVMUDFGen<LiteralArgTypes...>>(gen, infer);
        registry()->Register({DataTypeTrait<LiteralArgTypes>::to_string()...},
                             cur_def_);
        if (fixed_ret_type_ != nullptr) {
            cur_def_->SetFixedReturnType(fixed_ret_type_);
        }

        return *this;
    }

    template <typename... LiteralArgTypes>
    LLVMUDFRegistryHelper& variadic_args(
        const typename VariadicLLVMUDFGen<LiteralArgTypes...>::FType&
            gen) {  // NOLINT
        return variadic_args<LiteralArgTypes...>([](...) { return nullptr; },
                                                 gen);
    }

    template <typename... LiteralArgTypes>
    LLVMUDFRegistryHelper& variadic_args(
        const typename VariadicLLVMUDFGen<LiteralArgTypes...>::InferFType&
            infer,
        const typename VariadicLLVMUDFGen<LiteralArgTypes...>::FType&
            gen) {  // NOLINT
        cur_def_ = std::make_shared<VariadicLLVMUDFGen<LiteralArgTypes...>>(
            gen, infer);
        registry()->Register(
            {DataTypeTrait<LiteralArgTypes>::to_string()..., "..."}, cur_def_);
        if (fixed_ret_type_ != nullptr) {
            cur_def_->SetFixedReturnType(fixed_ret_type_);
        }
        return *this;
    }

    std::shared_ptr<LLVMUDFGenBase> cur_def() const { return cur_def_; }

 private:
    std::shared_ptr<LLVMUDFGenBase> cur_def_ = nullptr;
    node::TypeNode* fixed_ret_type_ = nullptr;
};

template <template <typename> typename FTemplate>
class CodeGenUDFTemplateRegistryHelper {
 public:
    CodeGenUDFTemplateRegistryHelper(const std::string& name,
                                     UDFLibrary* library)
        : helper_(library->RegisterCodeGenUDF(name)) {}

    template <typename... LiteralArgTypes>
    CodeGenUDFTemplateRegistryHelper& args_in() {
        cur_defs_ = {RegisterSingle<
            LiteralArgTypes,
            typename FTemplate<LiteralArgTypes>::LiteralArgTypes>()(
            helper_)...};
        if (fixed_ret_type_ != nullptr) {
            for (auto def : cur_defs_) {
                def->SetFixedReturnType(fixed_ret_type_);
            }
        }
        return *this;
    }

    template <typename RetType>
    CodeGenUDFTemplateRegistryHelper& returns() {
        fixed_ret_type_ =
            DataTypeTrait<RetType>::to_type_node(helper_.node_manager());
        for (auto def : cur_defs_) {
            def->SetFixedReturnType(fixed_ret_type_);
        }
        return *this;
    }

 private:
    template <typename T, typename X>
    struct RegisterSingle;

    template <typename T, typename... LiteralArgTypes>
    struct RegisterSingle<T, std::tuple<LiteralArgTypes...>> {
        std::shared_ptr<LLVMUDFGenBase> operator()(
            LLVMUDFRegistryHelper& helper) {  // NOLINT
            helper.args<LiteralArgTypes...>(
                [](codegen::CodeGenContext* ctx,
                   typename std::pair<LiteralArgTypes, codegen::NativeValue>::
                       second_type... args,
                   codegen::NativeValue* result) {
                    return FTemplate<T>()(ctx, args..., result);
                });
            return helper.cur_def();
        }
    };

    LLVMUDFRegistryHelper helper_;
    std::vector<std::shared_ptr<LLVMUDFGenBase>> cur_defs_;
    node::TypeNode* fixed_ret_type_;
};

/**
 * Interface to resolve udf to external native functions.
 */
class ExternalFuncRegistry : public UDFRegistry {
 public:
    explicit ExternalFuncRegistry(const std::string& name)
        : UDFRegistry(name), allow_window_(true), allow_project_(true) {}

    Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

    Status Register(const std::vector<std::string>& args,
                    node::ExternalFnDefNode* func);

    void SetAllowWindow(bool flag) { this->allow_window_ = flag; }

    void SetAllowProject(bool flag) { this->allow_project_ = flag; }

    const ArgSignatureTable<node::ExternalFnDefNode*>& GetTable() const {
        return reg_table_;
    }

 private:
    ArgSignatureTable<node::ExternalFnDefNode*> reg_table_;
    bool allow_window_;
    bool allow_project_;
};

struct ImplicitFuncPtr {
    template <typename Ret, typename... Args>
    ImplicitFuncPtr(Ret (*fn)(Args...))  // NOLINT
        : ptr(reinterpret_cast<void*>(fn)),
          get_ret_func([](node::NodeManager* nm) {
              return DataTypeTrait<typename CCallDataTypeTrait<
                  Ret>::LiteralTag>::to_type_node(nm);
          }) {}

    void* ptr;
    std::function<node::TypeNode*(node::NodeManager*)> get_ret_func;
};

class ExternalFuncRegistryHelper
    : public UDFRegistryHelper<ExternalFuncRegistry> {
 public:
    explicit ExternalFuncRegistryHelper(
        std::shared_ptr<ExternalFuncRegistry> registry, UDFLibrary* library)
        : UDFRegistryHelper<ExternalFuncRegistry>(registry, library),
          cur_def_(nullptr) {}

    ExternalFuncRegistryHelper& allow_project(bool flag) {
        registry()->SetAllowProject(flag);
        return *this;
    }

    ExternalFuncRegistryHelper& allow_window(bool flag) {
        registry()->SetAllowWindow(flag);
        return *this;
    }

    template <typename RetType>
    ExternalFuncRegistryHelper& returns() {
        if (cur_def_ == nullptr) {
            LOG(WARNING) << "No arg types specified for "
                         << " udf registry " << registry()->name();
            return *this;
        }
        auto ret_type = DataTypeTrait<RetType>::to_type_node(node_manager());
        cur_def_->SetRetType(ret_type);
        return *this;
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const std::string& name,
                                     const ImplicitFuncPtr& fn_ptr) {
        args<LiteralArgTypes...>(name, fn_ptr.ptr);
        cur_def_->SetRetType(fn_ptr.get_ret_func(node_manager()));
        return *this;
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const ImplicitFuncPtr& fn_ptr) {
        args<LiteralArgTypes...>(fn_ptr.ptr);
        cur_def_->SetRetType(fn_ptr.get_ret_func(node_manager()));
        return *this;
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(const std::string& name, void* fn_ptr) {
        if (cur_def_ != nullptr && cur_def_->ret_type() == nullptr) {
            LOG(WARNING) << "Function " << cur_def_->function_name()
                         << " is not specified with return type for "
                         << " udf registry " << registry()->name();
        }
        std::vector<std::string> type_args(
            {DataTypeTrait<LiteralArgTypes>::to_string()...});

        std::vector<const node::TypeNode*> type_nodes(
            {DataTypeTrait<LiteralArgTypes>::to_type_node(node_manager())...});

        cur_def_ = dynamic_cast<node::ExternalFnDefNode*>(
            node_manager()->MakeExternalFnDefNode(name, fn_ptr, nullptr,
                                                  type_nodes, -1, false));
        registry()->Register(type_args, cur_def_);
        return *this;
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& args(void* fn_ptr) {
        std::string fn_name = registry()->name();
        for (auto param_name :
             {DataTypeTrait<LiteralArgTypes>::to_type_node(node_manager())
                  ->GetName()...}) {
            fn_name.append(".").append(param_name);
        }
        return args<LiteralArgTypes...>(fn_name, fn_ptr);
    }

    template <typename... LiteralArgTypes>
    ExternalFuncRegistryHelper& variadic_args(const std::string& name,
                                              void* fn_ptr) {
        if (cur_def_ != nullptr && cur_def_->ret_type() == nullptr) {
            LOG(WARNING) << "Function " << cur_def_->function_name()
                         << " is not specified with return type for "
                         << " udf registry " << registry()->name();
        }
        std::vector<std::string> type_args(
            {DataTypeTrait<LiteralArgTypes>::to_string()...});
        type_args.emplace_back("...");

        std::vector<const node::TypeNode*> type_nodes(
            {DataTypeTrait<LiteralArgTypes>::to_type_node(node_manager())...});

        cur_def_ = dynamic_cast<node::ExternalFnDefNode*>(
            node_manager()->MakeExternalFnDefNode(
                name, fn_ptr, nullptr, type_nodes, sizeof...(LiteralArgTypes),
                false));
        registry()->Register(type_args, cur_def_);
        return *this;
    }

    ExternalFuncRegistryHelper& return_by_arg(bool flag) {
        if (cur_def_ == nullptr) {
            LOG(WARNING) << "No arg types specified for "
                         << " udf registry " << registry()->name();
            return *this;
        }
        cur_def_->SetReturnByArg(flag);
        return *this;
    }

    node::ExternalFnDefNode* cur_def() const { return cur_def_; }

 private:
    node::ExternalFnDefNode* cur_def_;
};

template <template <typename> typename FTemplate>
class ExternalTemplateFuncRegistryHelper {
 public:
    ExternalTemplateFuncRegistryHelper(const std::string& name,
                                       UDFLibrary* library)
        : name_(name), library_(library) {}

    template <typename... LiteralArgTypes>
    ExternalTemplateFuncRegistryHelper& args_in() {
        auto helper = library_->RegisterExternal(name_);
        cur_defs_ = {
            RegisterSingle(helper, &FTemplate<LiteralArgTypes>::operator())...};
        for (auto def : cur_defs_) {
            def->SetReturnByArg(return_by_arg_);
        }
        return *this;
    }

    ExternalTemplateFuncRegistryHelper& return_by_arg(bool flag) {
        return_by_arg_ = flag;
        for (auto def : cur_defs_) {
            def->SetReturnByArg(flag);
        }
        return *this;
    }

 private:
    template <typename T>
    using LiteralTag = typename CCallDataTypeTrait<T>::LiteralTag;

    template <typename T, typename... LiteralArgTypes>
    struct FTemplateInst {
        static auto fcompute(LiteralArgTypes... args) {
            return FTemplate<T>()(args...);
        }
    };

    template <typename T, typename A1>
    node::ExternalFnDefNode* RegisterSingle(
        ExternalFuncRegistryHelper& helper,  // NOLINT
        void (FTemplate<T>::*fn)(A1*)) {     // NOLINT
        helper.args<>(reinterpret_cast<void*>(FTemplateInst<T, A1*>::fcompute))
            .template returns<LiteralTag<A1>>()
            .return_by_arg(true);
        return helper.cur_def();
    }

    template <typename T, typename FTemplateRet, typename A1>
    node::ExternalFnDefNode* RegisterSingle(
        ExternalFuncRegistryHelper& helper,      // NOLINT
        FTemplateRet (FTemplate<T>::*fn)(A1)) {  // NOLINT
        helper.args<LiteralTag<A1>>(FTemplateInst<T, A1>::fcompute)
            .template returns<LiteralTag<FTemplateRet>>();
        return helper.cur_def();
    }

    template <typename T, typename A1, typename A2>
    node::ExternalFnDefNode* RegisterSingle(
        ExternalFuncRegistryHelper& helper,   // NOLINT
        void (FTemplate<T>::*fn)(A1, A2*)) {  // NOLINT
        helper
            .args<LiteralTag<A1>>(
                reinterpret_cast<void*>(FTemplateInst<T, A1, A2*>::fcompute))
            .template returns<LiteralTag<A2>>()
            .return_by_arg(true);
        return helper.cur_def();
    }

    template <typename T, typename FTemplateRet, typename A1, typename A2>
    node::ExternalFnDefNode* RegisterSingle(
        ExternalFuncRegistryHelper& helper,          // NOLINT
        FTemplateRet (FTemplate<T>::*fn)(A1, A2)) {  // NOLINT
        helper
            .args<LiteralTag<A1>, LiteralTag<A2>>(
                FTemplateInst<T, A1, A2>::fcompute)
            .template returns<LiteralTag<FTemplateRet>>();
        return helper.cur_def();
    }

    template <typename T, typename A1, typename A2,
              typename A3>
    node::ExternalFnDefNode* RegisterSingle(
        ExternalFuncRegistryHelper& helper,       // NOLINT
        void (FTemplate<T>::*fn)(A1, A2, A3*)) {  // NOLINT
        helper
            .args<LiteralTag<A1>, LiteralTag<A2>>(reinterpret_cast<void*>(
                FTemplateInst<T, A1, A2, A3*>::fcompute))
            .template returns<LiteralTag<A3>>()
            .return_by_arg(true);
        return helper.cur_def();
    }

    template <typename T, typename FTemplateRet, typename A1, typename A2,
              typename A3>
    node::ExternalFnDefNode* RegisterSingle(
        ExternalFuncRegistryHelper& helper,              // NOLINT
        FTemplateRet (FTemplate<T>::*fn)(A1, A2, A3)) {  // NOLINT
        helper
            .args<LiteralTag<A1>, LiteralTag<A2>, LiteralTag<A3>>(
                FTemplateInst<T, A1, A2, A3>::fcompute)
            .template returns<LiteralTag<FTemplateRet>>();
        return helper.cur_def();
    }

    std::string name_;
    UDFLibrary* library_;
    bool return_by_arg_ = false;
    std::vector<node::ExternalFnDefNode*> cur_defs_;
};

class SimpleUDFRegistry : public UDFRegistry {
 public:
    explicit SimpleUDFRegistry(const std::string& name) : UDFRegistry(name) {}

    Status Register(const std::vector<std::string>& input_args,
                    node::UDFDefNode* udaf_def);

    Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    // input arg type -> udaf def
    ArgSignatureTable<node::UDFDefNode*> reg_table_;
};

class SimpleUDAFRegistry : public UDFRegistry {
 public:
    explicit SimpleUDAFRegistry(const std::string& name) : UDFRegistry(name) {}

    Status Register(const std::string& input_arg, node::UDAFDefNode* udaf_def);

    Status ResolveFunction(UDFResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    // input arg type -> udaf def
    std::unordered_map<std::string, node::UDAFDefNode*> reg_table_;
};

template <typename IN, typename ST, typename OUT>
class SimpleUDAFRegistryHelperImpl;

class SimpleUDAFRegistryHelper : public UDFRegistryHelper<SimpleUDAFRegistry> {
 public:
    explicit SimpleUDAFRegistryHelper(
        std::shared_ptr<SimpleUDAFRegistry> registry, UDFLibrary* library)
        : UDFRegistryHelper<SimpleUDAFRegistry>(registry, library) {}

    template <typename IN, typename ST, typename OUT>
    SimpleUDAFRegistryHelperImpl<IN, ST, OUT> templates();
};

template <typename IN, typename ST, typename OUT>
class SimpleUDAFRegistryHelperImpl {
 public:
    explicit SimpleUDAFRegistryHelperImpl(
        UDFLibrary* library, std::shared_ptr<SimpleUDAFRegistry> registry)
        : registry_(registry),
          library_(library),
          nm_(library->node_manager()),
          input_ty_(DataTypeTrait<IN>::to_type_node(nm_)),
          state_ty_(DataTypeTrait<ST>::to_type_node(nm_)),
          output_ty_(DataTypeTrait<OUT>::to_type_node(nm_)) {}

    ~SimpleUDAFRegistryHelperImpl() { finalize(); }

    template <typename NewIN, typename NewST, typename NewOUT>
    SimpleUDAFRegistryHelperImpl<NewIN, NewST, NewOUT> templates() {
        finalize();
        return SimpleUDAFRegistryHelperImpl<NewIN, NewST, NewOUT>(library_,
                                                                  registry_);
    }

    SimpleUDAFRegistryHelperImpl& init(const std::string& fname) {
        node::FnDefNode* fn = fn_spec_by_name(fname, {});
        if (check_fn_ret_type(fname, fn, state_ty_)) {
            this->init_ = nm_->MakeFuncNode(fn, nm_->MakeExprList(), nullptr);
        }
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& init(const std::string& fname, void* fn_ptr) {
        auto fn =
            dynamic_cast<node::ExternalFnDefNode*>(nm_->MakeExternalFnDefNode(
                fname, fn_ptr, state_ty_, {}, -1, false));
        this->init_ = nm_->MakeFuncNode(fn, nm_->MakeExprList(), nullptr);
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& const_init(const ST& value) {
        this->init_ = DataTypeTrait<ST>::to_const(nm_, value);
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& update(const std::string& fname) {
        node::FnDefNode* fn = fn_spec_by_name(fname, {state_ty_, input_ty_});
        if (check_fn_ret_type(fname, fn, state_ty_)) {
            this->update_ = fn;
        }
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& update(
        const std::function<
            const node::TypeNode*(UDFResolveContext* ctx, const node::TypeNode*,
                                  const node::TypeNode*)>& infer,
        const std::function<Status(codegen::CodeGenContext*,
                                   codegen::NativeValue, codegen::NativeValue,
                                   codegen::NativeValue*)>& gen) {
        auto fn = dynamic_cast<node::UDFByCodeGenDefNode*>(
            nm_->MakeUDFByCodeGenDefNode({state_ty_, input_ty_}, state_ty_));
        fn->SetGenImpl(std::make_shared<LLVMUDFGen<ST, IN>>(gen, infer));

        auto fname = "anonymous<" + registry_->name() + "::update>";
        if (check_fn_ret_type(fname, fn, state_ty_)) {
            this->update_ = fn;
        }
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& update(const std::string& fname,
                                         void* fn_ptr) {
        auto fn =
            dynamic_cast<node::ExternalFnDefNode*>(nm_->MakeExternalFnDefNode(
                fname, fn_ptr, state_ty_, {state_ty_, input_ty_}, -1, false));
        this->update_ = fn;
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& merge(const std::string& fname) {
        node::FnDefNode* fn = fn_spec_by_name(fname, {state_ty_, state_ty_});
        if (check_fn_ret_type(fname, fn, state_ty_)) {
            this->merge_ = fn;
        }
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& merge(const std::string& fname,
                                        void* fn_ptr) {
        auto fn =
            dynamic_cast<node::ExternalFnDefNode*>(nm_->MakeExternalFnDefNode(
                fname, fn_ptr, state_ty_, {state_ty_, state_ty_}, -1, false));
        this->merge_ = fn;
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& output(const std::string& fname) {
        node::FnDefNode* fn = fn_spec_by_name(fname, {state_ty_});
        if (check_fn_ret_type(fname, fn, output_ty_)) {
            this->output_ = fn;
        }
        return *this;
    }

    SimpleUDAFRegistryHelperImpl& output(const std::string& fname,
                                         void* fn_ptr) {
        auto fn =
            dynamic_cast<node::ExternalFnDefNode*>(nm_->MakeExternalFnDefNode(
                fname, fn_ptr, output_ty_, {state_ty_}, -1, false));
        this->output_ = fn;
        return *this;
    }

    void finalize() {
        if (update_ == nullptr) {
            LOG(WARNING) << "Update function not specified for "
                         << registry_->name() << "<" << input_ty_->GetName()
                         << ", " << state_ty_->GetName() << ", "
                         << output_ty_->GetName() << ">";
            return;
        }
        if (init_ == nullptr) {
            if (!std::is_same<IN, ST>::value) {
                LOG(WARNING) << "No init expr provided but input "
                             << "type does not equal to state type";
                return;
            }
        }
        auto input_type = nm_->MakeTypeNode(node::kList, input_ty_);

        // infer init expr
        if (init_ != nullptr) {
            node::ExprAnalysisContext analysis_ctx(nm_, nullptr, false);
            auto status = init_->InferAttr(&analysis_ctx);
            if (!status.isOK()) {
                LOG(WARNING)
                    << "Fail to resolve init expr: " << init_->GetExprString();
                return;
            }
        }

        auto udaf = dynamic_cast<node::UDAFDefNode*>(nm_->MakeUDAFDefNode(
            registry_->name(), input_type, init_, update_, merge_, output_));
        registry_->Register(input_ty_->GetName(), udaf);
    }

 private:
    template <typename... RetType>
    bool check_fn_ret_type(const std::string& ref, node::FnDefNode* fn,
                           node::TypeNode* expect) {
        if (fn == nullptr) {
            return false;
        }
        const node::TypeNode* ret_type = fn->GetReturnType();
        if (ret_type != nullptr && ret_type->Equals(expect)) {
            return true;
        } else {
            LOG(WARNING) << "Illegal return type of " << ref << ": "
                         << (ret_type == nullptr ? "null" : ret_type->GetName())
                         << ", expect " << expect->GetName();
            return false;
        }
    }

    template <typename... LiteralArgTypes>
    node::FnDefNode* fn_spec_by_name(
        const std::string& name,
        const std::vector<node::TypeNode*>& arg_types) {
        std::vector<node::ExprNode> dummy_args;
        auto arg_list = nm_->MakeExprList();
        for (size_t i = 0; i < arg_types.size(); ++i) {
            std::string arg_name = "arg_" + std::to_string(i);
            node::ExprNode* arg = nm_->MakeExprIdNode(arg_name);
            arg->SetOutputType(arg_types[i]);
            arg_list->AddChild(arg);
        }

        auto registries =
            std::dynamic_pointer_cast<CompositeRegistry>(library_->Find(name));
        if (registries == nullptr) {
            LOG(WARNING) << "Fail to find registry '" << name
                         << "' for simple udaf '" << registry_->name() << "'";
            return nullptr;
        }
        std::shared_ptr<UDFRegistry> udf_registry = nullptr;
        for (auto sub_reg : registries->GetSubRegistries()) {
            udf_registry = std::dynamic_pointer_cast<UDFRegistry>(sub_reg);
            if (udf_registry == nullptr) {
                continue;
            }
            node::FnDefNode* res = nullptr;
            node::ExprAnalysisContext analysis_ctx(nm_, nullptr, false);
            UDFResolveContext ctx(arg_list, nullptr, &analysis_ctx);
            auto status = udf_registry->ResolveFunction(&ctx, &res);
            if (!status.isOK() || res == nullptr) {
                LOG(WARNING) << status.msg;
                continue;
            }
            return res;
        }
        LOG(WARNING) << "Fail to resolve sub function def '" << name
                     << "' for simple udaf '" << registry_->name() << "'";
        return nullptr;
    }

    std::shared_ptr<SimpleUDAFRegistry> registry_;
    UDFLibrary* library_;
    node::NodeManager* nm_;
    node::TypeNode* input_ty_;
    node::TypeNode* state_ty_;
    node::TypeNode* output_ty_;

    node::ExprNode* init_ = nullptr;
    node::FnDefNode* update_ = nullptr;
    node::FnDefNode* merge_ = nullptr;
    node::FnDefNode* output_ = nullptr;
};

template <typename IN, typename ST, typename OUT>
SimpleUDAFRegistryHelperImpl<IN, ST, OUT>
SimpleUDAFRegistryHelper::templates() {
    return SimpleUDAFRegistryHelperImpl<IN, ST, OUT>(library(), registry());
}

template <template <typename> typename FTemplate>
class UDAFTemplateRegistryHelper
    : public UDFRegistryHelper<SimpleUDAFRegistry> {
 public:
    UDAFTemplateRegistryHelper(std::shared_ptr<SimpleUDAFRegistry> registry,
                               UDFLibrary* library)
        : UDFRegistryHelper<SimpleUDAFRegistry>(registry, library) {}

    template <typename... LiteralArgTypes>
    UDAFTemplateRegistryHelper& args_in() {
        SimpleUDAFRegistryHelper helper(registry(), library());
        results_ = {RegisterSingle<LiteralArgTypes>(helper)...};
        return *this;
    }

 private:
    template <typename T>
    int RegisterSingle(SimpleUDAFRegistryHelper& helper) {  // NOLINT
        FTemplate<T> inst;
        inst(helper);
        return 0;
    }

    std::vector<int> results_;
};

}  // namespace udf
}  // namespace fesql

#endif  // SRC_UDF_UDF_REGISTRY_H_
