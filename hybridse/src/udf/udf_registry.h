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

#ifndef HYBRIDSE_SRC_UDF_UDF_REGISTRY_H_
#define HYBRIDSE_SRC_UDF_UDF_REGISTRY_H_

#include <memory>
#include <set>
#include <sstream>
#include <string>
#include <tuple>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <vector>

#include "base/fe_status.h"
#include "codegen/context.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/literal_traits.h"
#include "udf/udf_library.h"

namespace hybridse {
namespace udf {

using hybridse::base::Status;
using hybridse::node::ExprAttrNode;
using hybridse::node::ExprNode;

/**
 * Overall information to resolve a sql function call.
 */
class UdfResolveContext {
 public:
    UdfResolveContext(const std::vector<node::ExprNode*>& args,
                      node::NodeManager* node_manager,
                      const udf::UdfLibrary* library)
        : args_(args), node_manager_(node_manager), library_(library) {}

    const std::vector<node::ExprNode*>& args() const { return args_; }
    node::NodeManager* node_manager() { return node_manager_; }
    const udf::UdfLibrary* library() { return library_; }

    size_t arg_size() const { return args_.size(); }
    const node::TypeNode* arg_type(size_t i) const {
        return args_[i]->GetOutputType();
    }
    bool arg_nullable(size_t i) const { return args_[i]->nullable(); }

    const std::string GetArgSignature() const;

    const std::string& GetError() const { return error_msg_; }
    void SetError(const std::string& err) { error_msg_ = err; }
    bool HasError() const { return error_msg_ != ""; }

 private:
    std::vector<node::ExprNode*> args_;
    node::NodeManager* node_manager_;
    const udf::UdfLibrary* library_;

    std::string error_msg_;
};

/**
 * Interface to implement resolve logic for sql function
 * call without extra transformation.
 */
class UdfRegistry {
 public:
    explicit UdfRegistry(const std::string& name) : name_(name) {}

    virtual ~UdfRegistry() {}

    virtual Status Transform(UdfResolveContext* ctx, node::ExprNode** result) {
        node::FnDefNode* fn_def = nullptr;
        CHECK_STATUS(ResolveFunction(ctx, &fn_def));
        *result =
            ctx->node_manager()->MakeFuncNode(fn_def, ctx->args(), nullptr);
        return Status::OK();
    }

    // "f(arg0, arg1, ...argN)" -> resolved f
    virtual Status ResolveFunction(UdfResolveContext* ctx,
                                   node::FnDefNode** result) = 0;

    const std::string& name() const { return name_; }

    void SetDoc(const std::string& doc) { this->doc_ = doc; }

    const std::string& doc() const { return doc_; }

 private:
    std::string name_;
    std::string doc_;
};

class ArgSignatureTable {
 public:
    Status Find(UdfResolveContext* ctx, std::shared_ptr<UdfRegistry>* res, std::string* signature, int* variadic_pos);

    Status Find(const std::vector<const node::TypeNode*>& arg_types, std::shared_ptr<UdfRegistry>* res,
                std::string* signature, int* variadic_pos);

    Status Register(const std::vector<const node::TypeNode*>& args,
            bool is_variadic, const std::shared_ptr<UdfRegistry>& t);

    struct DefItem {
        std::shared_ptr<UdfRegistry> value;
        std::vector<const node::TypeNode*> arg_types;
        bool is_variadic;
        DefItem(const std::shared_ptr<UdfRegistry>& value,
                const std::vector<const node::TypeNode*>& arg_types,
                bool is_variadic)
            : value(value), arg_types(arg_types), is_variadic(is_variadic) {}
    };

    using TableType = std::unordered_map<std::string, DefItem>;

    const TableType& GetTable() const { return table_; }

 private:
    TableType table_;
};

struct UdfLibraryEntry {
    // argument matching table
    ArgSignatureTable signature_table;

    // record whether is udaf for specified argument num
    std::unordered_set<size_t> udaf_arg_nums;

    // record whether always require list input at position
    std::unordered_map<size_t, bool> arg_is_always_list;

    // record whether always return list
    bool always_return_list = false;

    // canonical funtion name
    std::string fn_name;

    // alias name to the function
    std::set<std::string> alias;
};

struct ExprUdfGenBase {
    virtual ExprNode* gen(UdfResolveContext* ctx,
                          const std::vector<ExprNode*>& args) = 0;
    virtual ~ExprUdfGenBase() {}
};

template <typename... Args>
struct ExprUdfGen : public ExprUdfGenBase {
    using FType = std::function<ExprNode*(
        UdfResolveContext*,
        typename std::pair<Args, ExprNode*>::second_type...)>;

    ExprNode* gen(UdfResolveContext* ctx,
                  const std::vector<ExprNode*>& args) override {
        if (args.size() != sizeof...(Args)) {
            LOG(WARNING) << "Fail to invoke ExprUdfGen::gen, args size do not "
                            "match with template args)";
            return nullptr;
        }
        return gen_internal(ctx, args, std::index_sequence_for<Args...>());
    }

    template <std::size_t... I>
    ExprNode* gen_internal(UdfResolveContext* ctx,
                           const std::vector<ExprNode*>& args,
                           const std::index_sequence<I...>&) {
        return gen_func(ctx, args[I]...);
    }

    explicit ExprUdfGen(const FType& f) : gen_func(f) {}
    const FType gen_func;
};

struct DynamicExprUdfGen : public ExprUdfGenBase {
    using FType = std::function<ExprNode*(UdfResolveContext*)>;
    explicit DynamicExprUdfGen(const FType& f) : gen_func(f) {}
    ExprNode* gen(UdfResolveContext* ctx, const std::vector<ExprNode*>& args) override {
        return gen_func(ctx);
    }
    const FType gen_func;
};

template <typename... Args>
struct VariadicExprUdfGen : public ExprUdfGenBase {
    using FType = std::function<ExprNode*(
        UdfResolveContext*, typename std::pair<Args, ExprNode*>::second_type...,
        const std::vector<ExprNode*>&)>;

    ExprNode* gen(UdfResolveContext* ctx,
                  const std::vector<ExprNode*>& args) override {
        return gen_internal(ctx, args, std::index_sequence_for<Args...>());
    };

    template <std::size_t... I>
    ExprNode* gen_internal(UdfResolveContext* ctx,
                           const std::vector<ExprNode*>& args,
                           const std::index_sequence<I...>&) {
        if (args.size() < sizeof...(Args)) {
            LOG(WARNING) << "Fail to invoke VariadicExprUdfGen::gen, "
                            "args size do not match with template args)";
            return nullptr;
        }
        std::vector<ExprNode*> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        return this->gen_func(ctx, args[I]..., variadic_args);
    }

    explicit VariadicExprUdfGen(const FType& f) : gen_func(f) {}
    const FType gen_func;
};

class UdfRegistryHelper {
 public:
    explicit UdfRegistryHelper(const std::string& name, UdfLibrary* library)
        : name_(name), library_(library) {}

    UdfLibrary* library() const { return library_; }

    node::NodeManager* node_manager() const { return library_->node_manager(); }

    const std::string& name() const { return name_; }
    const std::string& doc() const { return doc_; }

    const std::string& GetDoc() const { return doc(); }

    void SetDoc(const std::string& doc) {
        doc_ = doc;
        for (auto& reg : registries_) {
            reg->SetDoc(doc);
        }
    }

    void SetAlwaysReturnList(bool flag) { always_return_list_ = flag; }

    void SetAlwaysListAt(size_t index, bool flag) {
        if (flag) {
            always_list_argidx_.insert(index);
        } else {
            always_list_argidx_.erase(index);
        }
    }

    void InsertRegistry(const std::vector<const node::TypeNode*>& signature,
                        bool is_variadic, std::shared_ptr<UdfRegistry> registry) {
        registry->SetDoc(doc_);
        library_->InsertRegistry(name_, signature, is_variadic,
                                 always_return_list_, always_list_argidx_,
                                 registry);
        registries_.push_back(registry);
    }

 private:
    std::string name_;
    UdfLibrary* library_;
    std::string doc_;
    bool always_return_list_ = false;
    std::unordered_set<size_t> always_list_argidx_;
    std::vector<std::shared_ptr<UdfRegistry>> registries_;
};

/**
 * Interface to resolve udf with expression construction.
 */
class ExprUdfRegistry : public UdfRegistry {
 public:
    explicit ExprUdfRegistry(const std::string& name,
                             std::shared_ptr<ExprUdfGenBase> gen_impl_func)
        : UdfRegistry(name), gen_impl_func_(gen_impl_func) {}

    Status ResolveFunction(UdfResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    std::shared_ptr<ExprUdfGenBase> gen_impl_func_;
};

class ExprUdfRegistryHelper : public UdfRegistryHelper {
 public:
    explicit ExprUdfRegistryHelper(const std::string& name, UdfLibrary* library)
        : UdfRegistryHelper(name, library) {}

    template <typename... Args>
    ExprUdfRegistryHelper& args(
        const typename ExprUdfGen<Args...>::FType& func) {
        auto gen_ptr = std::make_shared<ExprUdfGen<Args...>>(func);
        auto registry = std::make_shared<ExprUdfRegistry>(name(), gen_ptr);
        this->InsertRegistry(
            {DataTypeTrait<Args>::to_type_node(node_manager())...}, false,
            registry);
        return *this;
    }

    template <typename... Args>
    ExprUdfRegistryHelper& variadic_args(
        const typename VariadicExprUdfGen<Args...>::FType& func) {
        auto gen_ptr = std::make_shared<VariadicExprUdfGen<Args...>>(func);
        auto registry = std::make_shared<ExprUdfRegistry>(name(), gen_ptr);
        this->InsertRegistry(
            {DataTypeTrait<Args>::to_type_node(node_manager())...}, true,
            registry);
        return *this;
    }

    ExprUdfRegistryHelper& doc(const std::string& doc) {
        SetDoc(doc);
        return *this;
    }

    ExprUdfRegistryHelper& return_list() {
        SetAlwaysReturnList(true);
        return *this;
    }

    ExprUdfRegistryHelper& list_argument_at(size_t index) {
        SetAlwaysListAt(index, true);
        return *this;
    }
};

template <template <typename> typename FTemplate>
class ExprUdfTemplateRegistryHelper {
 public:
    ExprUdfTemplateRegistryHelper(const std::string& name, UdfLibrary* library)
        : helper_(library->RegisterExprUdf(name)) {}

    template <typename... Args>
    std::initializer_list<int> args_in() {
        return {
            RegisterSingle<Args, typename FTemplate<Args>::Args>()(helper_)...};
    }

    auto& doc(const std::string& str) {
        helper_.doc(str);
        return *this;
    }

    auto& return_list() {
        helper_.return_list();
        return *this;
    }

    auto& list_argument_at(size_t index) {
        helper_.list_argument_at(index);
        return *this;
    }

 private:
    template <typename T, typename... Args>
    struct FTemplateInst {
        static ExprNode* fcompute(
            UdfResolveContext* ctx,
            typename std::pair<Args, ExprNode*>::second_type... args) {
            return FTemplate<T>()(ctx, args...);
        }
    };

    template <typename T, typename X>
    struct RegisterSingle;

    template <typename T, typename... Args>
    struct RegisterSingle<T, std::tuple<Args...>> {
        int operator()(ExprUdfRegistryHelper& helper) {  // NOLINT
            helper.args<Args...>(FTemplateInst<T, Args...>::fcompute);
            return 0;
        }
    };

    ExprUdfRegistryHelper helper_;
};

class LlvmUdfGenBase {
 public:
    virtual Status gen(codegen::CodeGenContext* ctx,
                       const std::vector<codegen::NativeValue>& args,
                       const ExprAttrNode& return_info,
                       codegen::NativeValue* res) = 0;

    virtual Status infer(UdfResolveContext* ctx,
                         const std::vector<ExprAttrNode>& args,
                         ExprAttrNode*) = 0;

    node::TypeNode* fixed_ret_type() const { return fixed_ret_type_; }

    void SetFixedReturnType(node::TypeNode* dtype) {
        this->fixed_ret_type_ = dtype;
    }

    virtual ~LlvmUdfGenBase() {}

 private:
    node::TypeNode* fixed_ret_type_ = nullptr;
};

template <typename... Args>
struct LlvmUdfGen : public LlvmUdfGenBase {
    using FType = std::function<Status(
        codegen::CodeGenContext* ctx,
        typename std::pair<Args, codegen::NativeValue>::second_type...,
        const ExprAttrNode& return_info,
        codegen::NativeValue*)>;

    using InferFType = std::function<Status(
        UdfResolveContext*,
        typename std::pair<Args, const ExprAttrNode&>::second_type...,
        ExprAttrNode*)>;

    Status gen(codegen::CodeGenContext* ctx,
               const std::vector<codegen::NativeValue>& args,
               const ExprAttrNode& return_info,
               codegen::NativeValue* result) override {
        CHECK_TRUE(args.size() == sizeof...(Args), common::kCodegenError,
                   "Fail to invoke LlvmUefGen::gen, args size do not "
                   "match with template args)");
        return gen_internal(ctx, args, return_info, result,
                            std::index_sequence_for<Args...>());
    }

    template <std::size_t... I>
    Status gen_internal(codegen::CodeGenContext* ctx,
                        const std::vector<codegen::NativeValue>& args,
                        const ExprAttrNode& return_info,
                        codegen::NativeValue* result,
                        const std::index_sequence<I...>&) {
        return gen_func(ctx, args[I]..., return_info, result);
    }

    Status infer(UdfResolveContext* ctx,
                 const std::vector<ExprAttrNode>& args,
                 ExprAttrNode* out) override {
        return infer_internal(ctx, args, out,
                              std::index_sequence_for<Args...>());
    }

    template <std::size_t... I>
    Status infer_internal(UdfResolveContext* ctx,
                          const std::vector<ExprAttrNode>& args,
                          ExprAttrNode* out, const std::index_sequence<I...>&) {
        if (this->infer_func) {
            return infer_func(ctx, args[I]..., out);
        } else {
            out->SetType(fixed_ret_type());
            out->SetNullable(false);
            return Status::OK();
        }
    }

    LlvmUdfGen(const FType& f, const InferFType& infer)
        : gen_func(f), infer_func(infer) {}

    explicit LlvmUdfGen(const FType& f) : gen_func(f), infer_func() {}

    virtual ~LlvmUdfGen() {}
    const FType gen_func;
    const InferFType infer_func;
};

template <typename... Args>
struct VariadicLLVMUdfGen : public LlvmUdfGenBase {
    using FType = std::function<Status(
        codegen::CodeGenContext*, typename std::pair<Args, codegen::NativeValue>::second_type...,
        const std::vector<codegen::NativeValue>&, const ExprAttrNode& return_info, codegen::NativeValue*)>;

    using InferFType = std::function<Status(
        UdfResolveContext*,
        typename std::pair<Args, const ExprAttrNode&>::second_type...,
        const std::vector<ExprAttrNode>&, ExprAttrNode*)>;

    Status gen(codegen::CodeGenContext* ctx,
               const std::vector<codegen::NativeValue>& args,
               const ExprAttrNode& return_info,
               codegen::NativeValue* result) override {
        CHECK_TRUE(args.size() >= sizeof...(Args), common::kCodegenError,
                   "Fail to invoke VariadicLLVMUdfGen::gen, "
                   "args size do not match with template args)");
        return gen_internal(ctx, args, return_info, result, std::index_sequence_for<Args...>());
    };

    template <std::size_t... I>
    Status gen_internal(codegen::CodeGenContext* ctx,
                        const std::vector<codegen::NativeValue>& args,
                        const ExprAttrNode& return_info,
                        codegen::NativeValue* result,
                        const std::index_sequence<I...>&) {
        std::vector<codegen::NativeValue> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        return this->gen_func(ctx, args[I]..., variadic_args, return_info, result);
    }

    Status infer(UdfResolveContext* ctx,
                 const std::vector<ExprAttrNode>& args,
                 ExprAttrNode* out) override {
        return infer_internal(ctx, args, out,
                              std::index_sequence_for<Args...>());
    }

    template <std::size_t... I>
    Status infer_internal(UdfResolveContext* ctx,
                          const std::vector<ExprAttrNode>& args,
                          ExprAttrNode* out, const std::index_sequence<I...>&) {
        std::vector<ExprAttrNode> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        if (this->infer_func) {
            return this->infer_func(ctx, args[I]..., variadic_args, out);
        } else {
            out->SetType(fixed_ret_type());
            out->SetNullable(false);
            return Status::OK();
        }
    }

    VariadicLLVMUdfGen(const FType& f, const InferFType& infer)
        : gen_func(f), infer_func(infer) {}

    explicit VariadicLLVMUdfGen(const FType& f) : gen_func(f), infer_func() {}

    const FType gen_func;
    const InferFType infer_func;
};

/**
 * Interface to resolve udf with llvm codegen construction.
 */
class LlvmUdfRegistry : public UdfRegistry {
 public:
    explicit LlvmUdfRegistry(const std::string& name,
                             std::shared_ptr<LlvmUdfGenBase> gen_impl_func,
                             size_t fixed_arg_size,
                             const std::vector<size_t>& nullable_arg_indices)
        : UdfRegistry(name),
          gen_impl_func_(gen_impl_func),
          fixed_arg_size_(fixed_arg_size),
          nullable_arg_indices_(nullable_arg_indices) {}

    Status ResolveFunction(UdfResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    std::shared_ptr<LlvmUdfGenBase> gen_impl_func_;
    size_t fixed_arg_size_;
    std::vector<size_t> nullable_arg_indices_;
};

class LlvmUdfRegistryHelper : public UdfRegistryHelper {
 public:
    LlvmUdfRegistryHelper(const std::string& name, UdfLibrary* library)
        : UdfRegistryHelper(name, library) {}

    LlvmUdfRegistryHelper(const LlvmUdfRegistryHelper& other)
        : UdfRegistryHelper(other.name(), other.library()) {}

    template <typename RetType>
    LlvmUdfRegistryHelper& returns() {
        fixed_ret_type_ =
            DataTypeTrait<RetType>::to_type_node(library()->node_manager());
        if (cur_def_ != nullptr) {
            cur_def_->SetFixedReturnType(fixed_ret_type_);
        }
        return *this;
    }

    template <typename... Args>
    LlvmUdfRegistryHelper& args(
        const typename LlvmUdfGen<Args...>::FType& gen) {
        using InferF = typename LlvmUdfGen<Args...>::InferFType;
        return args<Args...>(InferF(), gen);
    }

    template <typename... Args>
    LlvmUdfRegistryHelper& args(
        const typename LlvmUdfGen<Args...>::InferFType& infer,
        const typename LlvmUdfGen<Args...>::FType& gen) {
        // find nullable arg positions
        std::vector<size_t> null_indices;
        std::vector<int> arg_nullable = {IsNullableTrait<Args>::value...};
        for (size_t i = 0; i < arg_nullable.size(); ++i) {
            if (arg_nullable[i] > 0) {
                null_indices.push_back(i);
            }
        }
        cur_def_ = std::make_shared<LlvmUdfGen<Args...>>(gen, infer);
        if (fixed_ret_type_ != nullptr) {
            cur_def_->SetFixedReturnType(fixed_ret_type_);
            if (fixed_ret_type_->base() == node::kList) {
                return_list();
            }
        }
        auto registry = std::make_shared<LlvmUdfRegistry>(
            name(), cur_def_, sizeof...(Args), null_indices);
        this->InsertRegistry(
            {DataTypeTrait<Args>::to_type_node(node_manager())...}, false,
            registry);
        return *this;
    }

    template <typename... Args>
    LlvmUdfRegistryHelper& variadic_args(
        const typename VariadicLLVMUdfGen<Args...>::FType& gen) {  // NOLINT
        return variadic_args<Args...>([](...) { return nullptr; }, gen);
    }

    template <typename... Args>
    LlvmUdfRegistryHelper& variadic_args(
        const typename VariadicLLVMUdfGen<Args...>::InferFType& infer,
        const typename VariadicLLVMUdfGen<Args...>::FType& gen) {  // NOLINT
        // find nullable arg positions
        std::vector<size_t> null_indices;
        std::vector<int> arg_nullable = {IsNullableTrait<Args>::value...};
        for (size_t i = 0; i < arg_nullable.size(); ++i) {
            if (arg_nullable[i] > 0) {
                null_indices.push_back(i);
            }
        }
        cur_def_ = std::make_shared<VariadicLLVMUdfGen<Args...>>(gen, infer);
        if (fixed_ret_type_ != nullptr) {
            cur_def_->SetFixedReturnType(fixed_ret_type_);
            if (fixed_ret_type_->base() == node::kList) {
                return_list();
            }
        }
        auto registry = std::make_shared<LlvmUdfRegistry>(
            name(), cur_def_, sizeof...(Args), null_indices);
        this->InsertRegistry(
            {DataTypeTrait<Args>::to_type_node(node_manager())...}, true,
            registry);
        return *this;
    }

    LlvmUdfRegistryHelper& doc(const std::string& doc) {
        SetDoc(doc);
        return *this;
    }

    LlvmUdfRegistryHelper& return_list() {
        SetAlwaysReturnList(true);
        return *this;
    }

    LlvmUdfRegistryHelper& list_argument_at(size_t index) {
        SetAlwaysListAt(index, true);
        return *this;
    }

    std::shared_ptr<LlvmUdfGenBase> cur_def() const { return cur_def_; }

 private:
    std::shared_ptr<LlvmUdfGenBase> cur_def_ = nullptr;
    node::TypeNode* fixed_ret_type_ = nullptr;
};

template <template <typename> typename FTemplate>
class CodeGenUdfTemplateRegistryHelper {
 public:
    CodeGenUdfTemplateRegistryHelper(const std::string& name,
                                     UdfLibrary* library)
        : helper_(library->RegisterCodeGenUdf(name)) {}

    template <typename... Args>
    CodeGenUdfTemplateRegistryHelper& args_in() {
        cur_defs_ = {
            RegisterSingle<Args, typename FTemplate<Args>::Args>()(helper_)...};
        if (fixed_ret_type_ != nullptr) {
            for (auto def : cur_defs_) {
                def->SetFixedReturnType(fixed_ret_type_);
            }
        }
        return *this;
    }

    template <typename RetType>
    CodeGenUdfTemplateRegistryHelper& returns() {
        fixed_ret_type_ =
            DataTypeTrait<RetType>::to_type_node(helper_.node_manager());
        for (auto def : cur_defs_) {
            def->SetFixedReturnType(fixed_ret_type_);
        }
        return *this;
    }

    auto& doc(const std::string& str) {
        helper_.doc(str);
        return *this;
    }

    auto& return_list() {
        helper_.return_list();
        return *this;
    }

    auto& list_argument_at(size_t index) {
        helper_.list_argument_at(index);
        return *this;
    }

 private:
    template <typename T, typename X>
    struct RegisterSingle;

    template <typename T, typename... Args>
    struct RegisterSingle<T, std::tuple<Args...>> {
        std::shared_ptr<LlvmUdfGenBase> operator()(
            LlvmUdfRegistryHelper& helper) {  // NOLINT
            helper.args<Args...>(
                [](codegen::CodeGenContext* ctx,
                   typename std::pair<Args, codegen::NativeValue>::second_type... args,
                   const ExprAttrNode& return_info, codegen::NativeValue* result) {
                    return FTemplate<T>()(ctx, args..., result);
                });
            return helper.cur_def();
        }
    };

    LlvmUdfRegistryHelper helper_;
    std::vector<std::shared_ptr<LlvmUdfGenBase>> cur_defs_;
    node::TypeNode* fixed_ret_type_;
};

/**
 * Interface to resolve udf to external native functions.
 */
class ExternalFuncRegistry : public UdfRegistry {
 public:
    explicit ExternalFuncRegistry(const std::string& name,
                                  node::ExternalFnDefNode* extern_def)
        : UdfRegistry(name), extern_def_(extern_def) {}

    Status ResolveFunction(UdfResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    node::ExternalFnDefNode* extern_def_;
};

class DynamicUdfRegistry : public UdfRegistry {
 public:
    explicit DynamicUdfRegistry(const std::string& name, node::DynamicUdfFnDefNode* extern_def)
        : UdfRegistry(name), extern_def_(extern_def) {}

    Status ResolveFunction(UdfResolveContext* ctx, node::FnDefNode** result) override;

 private:
    node::DynamicUdfFnDefNode* extern_def_;
};

template <bool A, bool B>
struct ConditionAnd {
    static const bool value = false;
};
template <>
struct ConditionAnd<true, true> {
    static const bool value = true;
};

template <typename Arg, typename CArg>
struct FuncArgTypeCheckHelper {
    static const bool value =
        std::is_same<Arg, typename CCallDataTypeTrait<CArg>::LiteralTag>::value;
};

template <typename, typename>
struct FuncTupleRetTypeCheckHelper {
    using Remain = void;
    static const bool value = false;
};

// FuncRetTypeCheckHelper
// checker for void functions that writes return values to last one or two parameters
// intend to used for void funtions only
template <typename Ret, typename>
struct FuncRetTypeCheckHelper {
    static const bool value = false;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Ret, std::tuple<Ret*>> {
    static const bool value = true;
    using RetType = Ret;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Ret, std::tuple<Ret*, bool*>> {
    static const bool value = true;
    using RetType = Nullable<Ret>;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Nullable<Ret>, std::tuple<Ret*, bool*>> {
    static const bool value = true;
    using RetType = Nullable<Ret>;
};
template <typename Ret>
struct FuncRetTypeCheckHelper<Opaque<Ret>, std::tuple<Ret*>> {
    static const bool value = true;
    using RetType = Opaque<Ret>;
};

template <typename... TupleArgs, typename... CArgs>
struct FuncRetTypeCheckHelper<Tuple<TupleArgs...>, std::tuple<CArgs...>> {
    using RecCheck = FuncTupleRetTypeCheckHelper<std::tuple<TupleArgs...>,
                                                 std::tuple<CArgs...>>;
    using RetType = Tuple<TupleArgs...>;
    static const bool value =
        ConditionAnd<RecCheck::value, std::is_same<typename RecCheck::Remain,
                                                   std::tuple<>>::value>::value;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleRetTypeCheckHelper<std::tuple<TupleHead, TupleTail...>,
                                   std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck = FuncRetTypeCheckHelper<TupleHead, std::tuple<CArgHead>>;
    using TailCheck = FuncTupleRetTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleRetTypeCheckHelper<
    std::tuple<Nullable<TupleHead>, TupleTail...>,
    std::tuple<CArgHead, bool*, CArgTail...>> {
    using HeadCheck = FuncRetTypeCheckHelper<TupleHead, std::tuple<CArgHead>>;

    using TailCheck = FuncTupleRetTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename... TupleArgs, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleRetTypeCheckHelper<
    std::tuple<Tuple<TupleArgs...>, TupleTail...>,
    std::tuple<CArgHead, CArgTail...>> {
    using RecCheck =
        FuncTupleRetTypeCheckHelper<std::tuple<TupleArgs...>,
                                    std::tuple<CArgHead, CArgTail...>>;
    using RecRemain = typename RecCheck::Remain;
    using TailCheck =
        FuncTupleRetTypeCheckHelper<std::tuple<TupleTail...>, RecRemain>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<RecCheck::value, TailCheck::value>::value;
};

template <typename... CArgs>
struct FuncTupleRetTypeCheckHelper<std::tuple<>, std::tuple<CArgs...>> {
    static const bool value = true;
    using Remain = std::tuple<CArgs...>;
};

template <typename, typename>
struct FuncTupleArgTypeCheckHelper {
    using Remain = void;
    static const bool value = false;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleArgTypeCheckHelper<std::tuple<TupleHead, TupleTail...>,
                                   std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<TupleHead, CArgHead>;
    using TailCheck = FuncTupleArgTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename TupleHead, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleArgTypeCheckHelper<
    std::tuple<Nullable<TupleHead>, TupleTail...>,
    std::tuple<CArgHead, bool, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<TupleHead, CArgHead>;
    using TailCheck = FuncTupleArgTypeCheckHelper<std::tuple<TupleTail...>,
                                                  std::tuple<CArgTail...>>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

template <typename... TupleArgs, typename... TupleTail, typename CArgHead,
          typename... CArgTail>
struct FuncTupleArgTypeCheckHelper<
    std::tuple<Tuple<TupleArgs...>, TupleTail...>,
    std::tuple<CArgHead, CArgTail...>> {
    using RecCheck =
        FuncTupleArgTypeCheckHelper<std::tuple<TupleArgs...>,
                                    std::tuple<CArgHead, CArgTail...>>;
    using RecRemain = typename RecCheck::Remain;
    using TailCheck =
        FuncTupleArgTypeCheckHelper<std::tuple<TupleTail...>, RecRemain>;
    using Remain = typename TailCheck::Remain;
    static const bool value =
        ConditionAnd<RecCheck::value, TailCheck::value>::value;
};

template <typename... CArgs>
struct FuncTupleArgTypeCheckHelper<std::tuple<>, std::tuple<CArgs...>> {
    static const bool value = true;
    using Remain = std::tuple<CArgs...>;
};

//==================================================================//
//             FuncTypeCheckHelper                                  //
//==================================================================//
//
//  compile time assertion between Group 1 types and Group 2 types
//  provided by a external registered bultin function.
template <typename Ret, typename Args, typename CRet, typename CArgs>
struct FuncTypeCheckHelper {
    // true if two group types are compatible
    static const bool value = false;
    // return type in Group 1
    using RetType = void;
    // true if return value is stored in last parameters in C signature.
    // e.g void fn(CArgs..., CRet ret)
    static const bool return_by_arg = false;
};

// Ret (ArgHead, ArgTail...)
// <->
// CRet (CArgHead, CArgTail...)
template <typename Ret, typename ArgHead, typename... ArgTail, typename CRet,
          typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<ArgHead, ArgTail...>, CRet,
                           std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<ArgHead, CArgHead>;

    using TailCheck = FuncTypeCheckHelper<Ret, std::tuple<ArgTail...>, CRet,
                                          std::tuple<CArgTail...>>;

    using RetType = typename TailCheck::RetType;

    static const bool return_by_arg = TailCheck::return_by_arg;

    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

// Ret (Nullable<ArgHead>, ArgTail...)
// <->
// CRet (CArgHead, bool, CArgTail...)
template <typename Ret, typename ArgHead, typename... ArgTail, typename CRet,
          typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<Nullable<ArgHead>, ArgTail...>, CRet,
                           std::tuple<CArgHead, bool, CArgTail...>> {
    using HeadCheck = FuncArgTypeCheckHelper<ArgHead, CArgHead>;

    using TailCheck = FuncTypeCheckHelper<Ret, std::tuple<ArgTail...>, CRet,
                                          std::tuple<CArgTail...>>;

    using RetType = typename TailCheck::RetType;

    static const bool return_by_arg = TailCheck::return_by_arg;

    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

// Ret (Tuple<TupleArgs...>, ArgTail...)
// <->
// CRet ([CArgHead, CArgTail1...], CArgTail2...)
template <typename Ret, typename... TupleArgs, typename... ArgTail,
          typename CRet, typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<Tuple<TupleArgs...>, ArgTail...>,
                           CRet, std::tuple<CArgHead, CArgTail...>> {
    using HeadCheck =
        FuncTupleArgTypeCheckHelper<std::tuple<TupleArgs...>,
                                    std::tuple<CArgHead, CArgTail...>>;

    using TailCheck = FuncTypeCheckHelper<Ret, std::tuple<ArgTail...>, CRet,
                                          typename HeadCheck::Remain>;

    using RetType = typename TailCheck::RetType;

    static const bool return_by_arg = TailCheck::return_by_arg;

    static const bool value =
        ConditionAnd<HeadCheck::value, TailCheck::value>::value;
};

// All input arg check passed, check return by arg convention
template <typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<void, std::tuple<>, void,
                           std::tuple<CArgHead, CArgTail...>> {
    using RetCheck =
        FuncRetTypeCheckHelper<typename CCallDataTypeTrait<CArgHead>::LiteralTag, std::tuple<CArgHead, CArgTail...>>;

    static const bool value = RetCheck::value;
    using RetType = typename RetCheck::RetType;
    static const bool return_by_arg = true;
};

// Ret ()
// <->
// void (CArgHead, CArgTail...)
template <typename Ret, typename CArgHead, typename... CArgTail>
struct FuncTypeCheckHelper<Ret, std::tuple<>, void,
                           std::tuple<CArgHead, CArgTail...>> {
    using RetCheck =
        FuncRetTypeCheckHelper<Ret, std::tuple<CArgHead, CArgTail...>>;

    static const bool value = RetCheck::value;
    using RetType = Ret;
    static const bool return_by_arg = true;
};

// All input arg check passed, check simple return
template <typename Ret, typename CRet>
struct FuncTypeCheckHelper<Ret, std::tuple<>, CRet, std::tuple<>> {
    static const bool value = FuncArgTypeCheckHelper<Ret, CRet>::value;
    using RetType = Ret;
    static const bool return_by_arg = false;
};

template <typename>
struct TypeAnnotatedFuncPtrImpl;  // primitive decl

// Category of type systems:
// - (group 0): the SQL types, int16, int32, bool, string, date...,
//   we represent those types in CPP source with the group 1 types below
// - (group 1) SQL types represented by CPP.
// - (group 2) C/CPP types: the actual paramter or return types in C functions.
//
// Till this moment, those types are, from
//   function param type (group 2) -> udf registry type (group 1) -> SQL data type:
//
//   - bool -> bool -> bool
//   - int16_t -> int16_t -> int16
//   - int32_t -> int32_t -> int or int32
//   - int64_t -> int64_t -> int64
//   - flat -> float -> float
//   - double -> double -> double
//   - Timestamp* -> Timestamp -> timestamp
//   - Date* -> Date -> date
//   - StringRef* -> StringRef -> string
//   - ArrayRef<T>* -> ArrayRef<T> -> array<T>
//
// about `Nullable` and `ListRef`
//
// For any new type, it must be able to convert from function param type to udf registry type,
// by impl the `CCallDataTypeTrait::LiteralTag`
//
//
// Ret and EnvArgs belong to udf registry type (group 1), CRet and CArgs belong to function type (group 2)
template <typename Ret, typename EnvArgs, typename CRet, typename CArgs>
struct StaticFuncTypeCheck {
    using Checker = FuncTypeCheckHelper<Ret, EnvArgs, CRet, CArgs>;

    static void check() {
        static_assert(Checker::value,
                      "C function can not match expect abstract types");
    }
};

/**
 * Store type checked function ptr. The expected argument types
 * are specified by literal template arguments `Args`. Type check
 * errors would just raise a compile-time error.
 */
template <typename... Args>
struct TypeAnnotatedFuncPtrImpl<std::tuple<Args...>> {
    using GetTypeF =
        typename std::function<void(node::NodeManager*, node::TypeNode**)>;

    // TypeAnnotatedFuncPtr can only be bulit from non-void return type
    // Extra return type information should be provided for return-by-arg
    // function.
    template <typename CRet, typename... CArgs>
    TypeAnnotatedFuncPtrImpl(CRet (*fn)(CArgs...)) {  // NOLINT
        // Check signature, assume return type is same
        using CheckType = StaticFuncTypeCheck<typename CCallDataTypeTrait<CRet>::LiteralTag, std::tuple<Args...>, CRet,
                                              std::tuple<CArgs...>>;
        CheckType::check();

        using RetType = typename CheckType::Checker::RetType;

        this->ptr = reinterpret_cast<void*>(fn);
        this->return_by_arg = CheckType::Checker::return_by_arg;
        this->return_nullable = IsNullableTrait<RetType>::value;
        this->get_ret_type_func = [](node::NodeManager* nm, node::TypeNode** ret) {
            *ret = DataTypeTrait<RetType>::to_type_node(nm);
        };
    }

    TypeAnnotatedFuncPtrImpl() {}

    void* ptr;
    bool return_by_arg;
    bool return_nullable;
    GetTypeF get_ret_type_func;
};

template <typename Ret, typename... Args>
struct TypeAnnotatedFuncPtrImpl<Ret(Args...)> {
    using GetTypeF =
        typename std::function<void(node::NodeManager*, node::TypeNode**)>;

    // TypeAnnotatedFuncPtr can only be bulit from non-void return type
    // Extra return type information should be provided for return-by-arg
    // function.
    template <typename CRet, typename... CArgs>
    TypeAnnotatedFuncPtrImpl(CRet (*fn)(CArgs...)) {  // NOLINT
        // Check signature, assume return type is same
        using CheckType = StaticFuncTypeCheck<Ret, std::tuple<Args...>, CRet, std::tuple<CArgs...>>;
        CheckType::check();

        using RetType = typename CheckType::Checker::RetType;
        static_assert(std::is_same<Ret, RetType>::value, "");
        this->ptr = reinterpret_cast<void*>(fn);
        this->return_by_arg = CheckType::Checker::return_by_arg;
        this->return_nullable = IsNullableTrait<RetType>::value;
        this->get_ret_type_func = [](node::NodeManager* nm, node::TypeNode** ret) {
            *ret = DataTypeTrait<RetType>::to_type_node(nm);
        };
    }

    TypeAnnotatedFuncPtrImpl() {}

    void* ptr;
    bool return_by_arg;
    bool return_nullable;
    GetTypeF get_ret_type_func;
};


// used to instantiate tuple type from template param pack
template <typename... Args>
struct TypeAnnotatedFuncPtr {
    using type = TypeAnnotatedFuncPtrImpl<std::tuple<Args...>>;
};

// used to instantiate tuple type from template param pack
template <typename Ret, typename... Args>
struct WithReturnTypeAnnotatedFuncPtr {
    using type = TypeAnnotatedFuncPtrImpl<Ret(Args...)>;
};


class ExternalFuncRegistryHelper : public UdfRegistryHelper {
 public:
    ExternalFuncRegistryHelper(const std::string& basename, UdfLibrary* library)
        : UdfRegistryHelper(basename, library) {}

    ~ExternalFuncRegistryHelper() {
        if (args_specified_) {
            finalize();
        }
    }

    template <typename Ret>
    ExternalFuncRegistryHelper& returns() {
        return_type_ = DataTypeTrait<Ret>::to_type_node(node_manager());
        return_nullable_ = IsNullableTrait<Ret>::value;
        return *this;
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& args(
        const std::string& name,
        const typename TypeAnnotatedFuncPtr<Args...>::type& fn_ptr) {
        args<Args...>(name, fn_ptr.ptr);
        update_return_info<Args...>(fn_ptr);
        return *this;
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& args(
        const typename TypeAnnotatedFuncPtr<Args...>::type& fn_ptr) {
        args<Args...>(fn_ptr.ptr);
        update_return_info<Args...>(fn_ptr);
        return *this;
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& args(const std::string& name, void* fn_ptr) {
        if (args_specified_) {
            finalize();
        }
        args_specified_ = true;
        fn_name_ = name;
        fn_ptr_ = fn_ptr;
        arg_types_ = {DataTypeTrait<Args>::to_type_node(node_manager())...};
        arg_nullable_ = {IsNullableTrait<Args>::value...};
        variadic_pos_ = -1;
        return *this;
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& args(void* fn_ptr) {
        std::string fn_name = name();
        for (auto param_name :
             {DataTypeTrait<Args>::to_type_node(node_manager())
                  ->GetName()...}) {
            fn_name.append(".").append(param_name);
        }
        return args<Args...>(fn_name, fn_ptr);
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& variadic_args(const std::string& name,
                                              void* fn_ptr) {
        if (args_specified_) {
            finalize();
        }
        fn_name_ = name;
        fn_ptr_ = fn_ptr;
        args_specified_ = true;
        arg_types_ = {DataTypeTrait<Args>::to_type_node(node_manager())...};
        arg_nullable_ = {IsNullableTrait<Args>::value...};
        variadic_pos_ = sizeof...(Args);
        return *this;
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& variadic_args(void* fn_ptr) {
        std::string fn_name = name();
        for (auto param_name :
             {DataTypeTrait<Args>::to_type_node(node_manager())
                  ->GetName()...}) {
            fn_name.append(".").append(param_name);
        }
        return variadic_args<Args...>(fn_name, fn_ptr);
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& variadic_args(
        const std::string& name,
        const typename TypeAnnotatedFuncPtr<Args...>::type& fn_ptr) {
        variadic_args<Args...>(name, fn_ptr.ptr);
        update_return_info(fn_ptr);
        return *this;
    }

    template <typename... Args>
    ExternalFuncRegistryHelper& variadic_args(
        const typename TypeAnnotatedFuncPtr<Args...>::type& fn_ptr) {
        variadic_args<Args...>(fn_ptr.ptr);
        update_return_info(fn_ptr);
        return *this;
    }

    template <typename Ret, typename... Args>
    ExternalFuncRegistryHelper& with_return_args(
        const typename WithReturnTypeAnnotatedFuncPtr<Ret, Args...>::type& fn_ptr) {
        args<Args...>(fn_ptr.ptr);
        node::TypeNode* dtype = nullptr;
        fn_ptr.get_ret_type_func(node_manager(), &dtype);
        if (dtype != nullptr) {
            return_type_ = dtype;
        }
        return_by_arg_ = fn_ptr.return_by_arg;
        return_nullable_ = fn_ptr.return_nullable;
        return *this;
    }

    ExternalFuncRegistryHelper& return_by_arg(bool flag) {
        return_by_arg_ = flag;
        return *this;
    }

    ExternalFuncRegistryHelper& doc(const std::string& str) {
        SetDoc(str);
        return *this;
    }

    ExternalFuncRegistryHelper& return_list() {
        SetAlwaysReturnList(true);
        return *this;
    }

    ExternalFuncRegistryHelper& list_argument_at(size_t index) {
        SetAlwaysListAt(index, true);
        return *this;
    }

    node::ExternalFnDefNode* cur_def() const { return cur_def_; }

    void finalize() {
        if (return_type_ == nullptr) {
            LOG(WARNING) << "No return type specified for "
                         << " udf registry " << name();
            return;
        }
        if (return_type_->base() == node::kList) {
            return_list();
        }
        auto def = node_manager()->MakeExternalFnDefNode(
            fn_name_, fn_ptr_, return_type_, return_nullable_, arg_types_,
            arg_nullable_, variadic_pos_, return_by_arg_);
        cur_def_ = def;

        auto registry = std::make_shared<ExternalFuncRegistry>(name(), def);
        library()->AddExternalFunction(fn_name_, fn_ptr_);
        this->InsertRegistry(arg_types_, variadic_pos_ >= 0, registry);
        reset();
    }

 private:
    template <typename... Args>
    void update_return_info(
        const typename TypeAnnotatedFuncPtr<Args...>::type& fn_ptr) {
        node::TypeNode* dtype = nullptr;
        fn_ptr.get_ret_type_func(node_manager(), &dtype);
        if (dtype != nullptr) {
            return_type_ = dtype;
        }
        return_by_arg_ = fn_ptr.return_by_arg;
        return_nullable_ = fn_ptr.return_nullable;
    }

    void reset() {
        fn_name_ = "";
        fn_ptr_ = nullptr;
        args_specified_ = false;
        arg_types_.clear();
        arg_nullable_.clear();
        return_type_ = nullptr;
        return_nullable_ = false;
        variadic_pos_ = -1;
    }

    std::string fn_name_;
    void* fn_ptr_;
    bool args_specified_ = false;
    std::vector<const node::TypeNode*> arg_types_;
    std::vector<int> arg_nullable_;
    node::TypeNode* return_type_ = nullptr;
    bool return_nullable_ = false;
    int variadic_pos_ = -1;
    bool return_by_arg_ = false;

    node::ExternalFnDefNode* cur_def_ = nullptr;
};

class DynamicUdfRegistryHelper : public UdfRegistryHelper {
 public:
    DynamicUdfRegistryHelper(const std::string& basename, UdfLibrary* library, void* fn,
            node::DataType return_type, bool return_nullable,
            const std::vector<node::DataType>& arg_types, bool arg_nullable,
            void* udfcontext_fun);
    Status Register();

 private:
    std::string fn_name_;
    void* fn_ptr_;
    void* udfcontext_fun_ptr_;
    std::vector<const node::TypeNode*> arg_types_;
    std::vector<int> arg_nullable_;
    node::TypeNode* return_type_ = nullptr;
    bool return_nullable_ = false;
    bool return_by_arg_ = false;
};

template <template <typename> typename FTemplate>
class ExternalTemplateFuncRegistryHelper {
 public:
    ExternalTemplateFuncRegistryHelper(const std::string& name,
                                       UdfLibrary* library)
        : name_(name),
          library_(library),
          helper_(library_->RegisterExternal(name_)) {}

    template <typename... Args>
    ExternalTemplateFuncRegistryHelper& args_in() {
        cur_defs_ = {RegisterSingle<Args, typename FTemplate<Args>::Args>()(helper_, &FTemplate<Args>::operator())...};
        if (return_by_arg_.has_value()) {
            for (auto def : cur_defs_) {
                def->SetReturnByArg(return_by_arg_.value());
            }
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

    auto& doc(const std::string& str) {
        helper_.doc(str);
        return *this;
    }

    auto& return_list() {
        helper_.return_list();
        return *this;
    }

    auto& list_argument_at(size_t index) {
        helper_.list_argument_at(index);
        return *this;
    }

 private:
    template <typename T>
    using LiteralTag = typename CCallDataTypeTrait<T>::LiteralTag;

    template <typename T, typename... CArgs>
    struct FTemplateInst {
        static auto fcompute(CArgs... args) { return FTemplate<T>()(args...); }
    };

    template <typename T, typename>
    struct RegisterSingle;  // prmiary decl

    template <typename T, typename... Args>
    struct RegisterSingle<T, std::tuple<Args...>> {
        template <typename CRet, typename... CArgs>
        node::ExternalFnDefNode* operator()(ExternalFuncRegistryHelper& helper,    // NOLINT
                                            CRet (FTemplate<T>::*fn)(CArgs...)) {  // NOLINT
            helper.args<Args...>(FTemplateInst<T, CArgs...>::fcompute).finalize();
            return helper.cur_def();
        }
    };

    template <typename T, typename Ret, typename... Args>
    struct RegisterSingle<T, Ret(Args...)> {
        template <typename CRet, typename... CArgs>
        node::ExternalFnDefNode* operator()(ExternalFuncRegistryHelper& helper,    // NOLINT
                                            CRet (FTemplate<T>::*fn)(CArgs...)) {  // NOLINT
            helper.with_return_args<Ret, Args...>(FTemplateInst<T, CArgs...>::fcompute).finalize();
            return helper.cur_def();
        }
    };

    std::string name_;
    UdfLibrary* library_;
    std::optional<bool> return_by_arg_;
    std::vector<node::ExternalFnDefNode*> cur_defs_;
    ExternalFuncRegistryHelper helper_;
};

class SimpleUdfRegistry : public UdfRegistry {
 public:
    SimpleUdfRegistry(const std::string& name, node::UdfDefNode* fn_def)
        : UdfRegistry(name), fn_def_(fn_def) {}

    Status ResolveFunction(UdfResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    node::UdfDefNode* fn_def_;
};

struct UdafDefGen {
    std::shared_ptr<ExprUdfGenBase> init_gen = nullptr;
    std::shared_ptr<UdfRegistry> update_gen = nullptr;
    std::shared_ptr<UdfRegistry> merge_gen = nullptr;
    std::shared_ptr<UdfRegistry> output_gen = nullptr;
    node::TypeNode* state_type = nullptr;
    bool state_nullable = false;
};

class UdafRegistry : public UdfRegistry {
 public:
    UdafRegistry(const std::string& name, const UdafDefGen& udaf_gen)
        : UdfRegistry(name), udaf_gen_(udaf_gen) {}

    Status ResolveFunction(UdfResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    UdafDefGen udaf_gen_;
};

template <typename OUT, typename ST, typename... IN>
class UdafRegistryHelperImpl;

class UdafRegistryHelper : public UdfRegistryHelper {
 public:
    explicit UdafRegistryHelper(const std::string& name, UdfLibrary* library)
        : UdfRegistryHelper(name, library) {}

    template <typename OUT, typename ST, typename... IN>
    UdafRegistryHelperImpl<OUT, ST, IN...> templates();

    auto& doc(const std::string doc) {
        SetDoc(doc);
        return *this;
    }

    auto& return_list() {
        SetAlwaysReturnList(true);
        return *this;
    }

    auto& list_argument_at(size_t index) {
        SetAlwaysListAt(index, true);
        return *this;
    }
};

template <typename OUT, typename ST, typename... IN>
class UdafRegistryHelperImpl : UdfRegistryHelper {
 public:
    explicit UdafRegistryHelperImpl(const std::string& name,
                                    UdfLibrary* library)
        : UdfRegistryHelper(name, library),
          elem_tys_(
              {DataTypeTrait<IN>::to_type_node(library->node_manager())...}),
          elem_nullable_({IsNullableTrait<IN>::value...}),
          state_ty_(DataTypeTrait<ST>::to_type_node(library->node_manager())),
          state_nullable_(IsNullableTrait<ST>::value),
          output_ty_(DataTypeTrait<OUT>::to_type_node(library->node_manager())),
          output_nullable_(IsNullableTrait<OUT>::value) {
        // specify update function argument types
        update_tys_.push_back(state_ty_);
        update_nullable_.push_back(state_nullable_);
        update_tags_.push_back(state_ty_->GetName());
        for (size_t i = 0; i < elem_tys_.size(); ++i) {
            update_tys_.push_back(elem_tys_[i]);
            update_nullable_.push_back(elem_nullable_[i]);
            update_tags_.push_back(elem_tys_[i]->GetName());
        }
    }

    ~UdafRegistryHelperImpl() { finalize(); }

    // Start next registry types
    template <typename NewOUT, typename NewST, typename... NewIN>
    UdafRegistryHelperImpl<NewOUT, NewST, NewIN...> templates() {
        finalize();
        auto helper_impl =
            UdafRegistryHelperImpl<NewOUT, NewST, NewIN...>(name(), library());
        helper_impl.doc(this->GetDoc());
        return helper_impl;
    }

    UdafRegistryHelperImpl& init(const std::string& fname) {
        udaf_gen_.init_gen =
            std::make_shared<ExprUdfGen>([fname](UdfResolveContext* ctx) {
                return ctx->node_manager()->MakeFuncNode(fname, {}, nullptr);
            });
        return *this;
    }

    UdafRegistryHelperImpl& init(const std::string& fname, void* fn_ptr) {
        auto fn = dynamic_cast<node::ExternalFnDefNode*>(
            library()->node_manager()->MakeExternalFnDefNode(
                fname, fn_ptr, state_ty_, state_nullable_, {}, {}, -1, false));

        udaf_gen_.init_gen =
            std::make_shared<ExprUdfGen>([fn](UdfResolveContext* ctx) {
                return ctx->node_manager()->MakeFuncNode(fn, {}, nullptr);
            });
        library()->AddExternalFunction(fname, fn_ptr);
        return *this;
    }

    UdafRegistryHelperImpl& init(
        const std::string& fname,
        const typename TypeAnnotatedFuncPtr<>::type& fn_ptr) {
        node::TypeNode* ret_type = nullptr;
        fn_ptr.get_ret_type_func(library()->node_manager(), &ret_type);

        if (ret_type == nullptr) {
            LOG(WARNING) << "Fail to get return type of function ptr";
            return *this;
        } else if (!ret_type->Equals(state_ty_) ||
                   (fn_ptr.return_nullable && !state_nullable_)) {
            LOG(WARNING)
                << "Illegal input type of external init typed function '"
                << fname << "': expected "
                << (state_nullable_ ? "nullable " : "") << state_ty_->GetName()
                << " but get " << (fn_ptr.return_nullable ? "nullable " : "")
                << ret_type->GetName();
            return *this;
        }
        auto fn = library()->node_manager()->MakeExternalFnDefNode(
            fname, fn_ptr.ptr, state_ty_, state_nullable_, {}, {}, -1,
            fn_ptr.return_by_arg);
        udaf_gen_.init_gen =
            std::make_shared<ExprUdfGen<>>([fn](UdfResolveContext* ctx) {
                return ctx->node_manager()->MakeFuncNode(fn, {}, nullptr);
            });
        library()->AddExternalFunction(fname, fn_ptr.ptr);
        return *this;
    }

    UdafRegistryHelperImpl& const_init(const ST& value) {
        udaf_gen_.init_gen =
            std::make_shared<ExprUdfGen<>>([value](UdfResolveContext* ctx) {
                return DataTypeTrait<ST>::to_const(ctx->node_manager(), value);
            });
        return *this;
    }

    UdafRegistryHelperImpl& const_init(const std::string& str) {
        udaf_gen_.init_gen =
            std::make_shared<ExprUdfGen<>>([str](UdfResolveContext* ctx) {
                return DataTypeTrait<ST>::to_const(ctx->node_manager(), str);
            });
        return *this;
    }

    UdafRegistryHelperImpl& update(const std::string& fname) {
        auto registry = library()->Find(fname, update_tys_);
        if (registry != nullptr) {
            udaf_gen_.update_gen = registry;
        } else {
            std::stringstream ss;
            for (size_t i = 0; i < update_tys_.size(); ++i) {
                if (update_tys_[i] != nullptr) {
                    ss << update_tys_[i]->GetName();
                } else {
                    ss << "?";
                }
                if (i < update_tys_.size() - 1) {
                    ss << ", ";
                }
            }
            LOG(WARNING) << "Fail to find udaf registry " << fname << "<"
                         << ss.str() << ">";
        }
        return *this;
    }

    UdafRegistryHelperImpl& update(
        const std::function<
            Status(UdfResolveContext* ctx, const ExprAttrNode*,
                   typename std::pair<IN, const ExprAttrNode*>::second_type...,
                   ExprAttrNode*)>& infer,
        const std::function<
            Status(codegen::CodeGenContext*, codegen::NativeValue,
                   typename std::pair<IN, codegen::NativeValue>::second_type...,
                   codegen::NativeValue*)>& gen) {
        auto llvm_gen = std::make_shared<LlvmUdfGen<ST, IN...>>(gen, infer);

        std::vector<size_t> null_indices;
        std::vector<int> arg_nullable = {IsNullableTrait<IN>::value...};
        for (size_t i = 0; i < arg_nullable.size(); ++i) {
            if (arg_nullable[i] > 0) {
                null_indices.push_back(1 + i);
            }
        }
        auto registry = std::make_shared<LlvmUdfRegistry>(
            name() + "@update", llvm_gen, 1 + sizeof...(IN), null_indices);
        udaf_gen_.update_gen = registry;
        return *this;
    }

    UdafRegistryHelperImpl& update(
        const std::function<node::ExprNode*(
            UdfResolveContext*, node::ExprNode*,
            typename std::pair<IN, node::ExprNode*>::second_type...)>& gen) {
        auto expr_gen = std::make_shared<ExprUdfGen<ST, IN...>>(gen);
        auto registry =
            std::make_shared<ExprUdfRegistry>(name() + "@update", expr_gen);
        udaf_gen_.update_gen = registry;
        return *this;
    }

    UdafRegistryHelperImpl& update(const std::string& fname, void* fn_ptr,
                                   bool return_by_arg = false) {
        auto fn = dynamic_cast<node::ExternalFnDefNode*>(
            library()->node_manager()->MakeExternalFnDefNode(
                fname, fn_ptr, state_ty_, state_nullable_, update_tys_,
                update_nullable_, -1, return_by_arg));
        auto registry = std::make_shared<ExternalFuncRegistry>(fname, fn);
        udaf_gen_.update_gen = registry;
        library()->AddExternalFunction(fname, fn_ptr);
        return *this;
    }

    UdafRegistryHelperImpl& update(
        const std::string& fname,
        const typename TypeAnnotatedFuncPtr<ST, IN...>::type& fn_ptr) {
        node::TypeNode* ret_type = nullptr;
        fn_ptr.get_ret_type_func(library()->node_manager(), &ret_type);
        if (ret_type == nullptr) {
            LOG(WARNING) << "Fail to get return type of function ptr";
            return *this;
        } else if (!ret_type->Equals(state_ty_) ||
                   (fn_ptr.return_nullable && !state_nullable_)) {
            LOG(WARNING)
                << "Illegal return type of external update typed function '"
                << fname << "': expected "
                << (state_nullable_ ? "nullable " : "") << state_ty_->GetName()
                << " but get " << (fn_ptr.return_nullable ? "nullable " : "")
                << ret_type->GetName();
            return *this;
        }
        return update(fname, fn_ptr.ptr, fn_ptr.return_by_arg);
    }

    UdafRegistryHelperImpl& merge(const std::string& fname) {
        auto registry = library()->Find(fname, {state_ty_, state_ty_});
        if (registry != nullptr) {
            udaf_gen_.merge_gen = registry;
        } else {
            std::string state_ty_name =
                state_ty_ == nullptr ? "?" : state_ty_->GetName();
            LOG(WARNING) << "Fail to find udaf registry " << fname << "<"
                         << state_ty_name << ", " << state_ty_name << ">";
        }
        return *this;
    }

    UdafRegistryHelperImpl& merge(const std::string& fname, void* fn_ptr) {
        auto fn = dynamic_cast<node::ExternalFnDefNode*>(
            library()->node_manager()->MakeExternalFnDefNode(
                fname, fn_ptr, state_ty_, state_nullable_,
                {state_ty_, state_ty_}, {state_nullable_, state_nullable_}, -1,
                false));
        auto registry = std::make_shared<ExternalFuncRegistry>(fname, fn);
        udaf_gen_.merge_gen = registry;
        library()->AddExternalFunction(fname, fn_ptr);
        return *this;
    }

    UdafRegistryHelperImpl& output(const std::string& fname) {
        auto registry = library()->Find(fname, {state_ty_});
        if (registry != nullptr) {
            udaf_gen_.output_gen = registry;
        } else {
            std::string state_ty_name =
                state_ty_ == nullptr ? "?" : state_ty_->GetName();
            LOG(WARNING) << "Fail to find udaf registry " << fname << "<"
                         << state_ty_name << ">";
        }
        return *this;
    }

    UdafRegistryHelperImpl& output(const std::string& fname, void* fn_ptr,
                                   bool return_by_arg = false) {
        auto fn = library()->node_manager()->MakeExternalFnDefNode(fname, fn_ptr, output_ty_, output_nullable_,
                                                                   {state_ty_}, {state_nullable_}, -1, return_by_arg);
        auto registry = std::make_shared<ExternalFuncRegistry>(fname, fn);
        auto state_tag = state_ty_->GetName();
        udaf_gen_.output_gen = registry;
        library()->AddExternalFunction(fname, fn_ptr);
        return *this;
    }

    UdafRegistryHelperImpl& output(
        const std::string& fname,
        const typename TypeAnnotatedFuncPtr<ST>::type& fn_ptr) {
        node::TypeNode* ret_type = nullptr;
        fn_ptr.get_ret_type_func(library()->node_manager(), &ret_type);
        if (ret_type == nullptr) {
            LOG(WARNING) << "Fail to get return type of function ptr";
            return *this;
        } else if (!ret_type->Equals(output_ty_)) {
            LOG(WARNING)
                << "Illegal return type of external update typed function '"
                << fname << "': expected "
                << (state_nullable_ ? "nullable " : "") << state_ty_->GetName()
                << " but get " << (fn_ptr.return_nullable ? "nullable " : "")
                << ret_type->GetName();
            return *this;
        }
        return output(fname, fn_ptr.ptr, fn_ptr.return_by_arg);
    }

    UdafRegistryHelperImpl& output(
        const std::function<node::ExprNode*(UdfResolveContext*,
                                            node::ExprNode*)>& gen) {
        auto expr_gen = std::make_shared<ExprUdfGen<ST>>(gen);
        auto registry =
            std::make_shared<ExprUdfRegistry>(name() + "@output", expr_gen);
        udaf_gen_.output_gen = registry;
        return *this;
    }

    void finalize() {
        if (elem_tys_.empty()) {
            LOG(WARNING) << "UDAF must take at least one input";
            return;
        }
        if (udaf_gen_.update_gen == nullptr) {
            LOG(WARNING) << "Update function not specified for " << name();
            return;
        }
        if (udaf_gen_.init_gen == nullptr) {
            if (!(elem_tys_.size() == 1 && elem_tys_[0]->Equals(state_ty_))) {
                LOG(WARNING) << "No init expr provided but input "
                             << "type does not equal to state type";
                return;
            }
        }
        if (output_ty_ != nullptr && output_ty_->base() == node::kList) {
            return_list();
        }
        udaf_gen_.state_type = state_ty_;
        udaf_gen_.state_nullable = state_nullable_;
        std::vector<const node::TypeNode*> input_list_types;
        for (auto elem_ty : elem_tys_) {
            input_list_types.push_back(
                library()->node_manager()->MakeTypeNode(node::kList, elem_ty));
        }
        auto registry = std::make_shared<UdafRegistry>(name(), udaf_gen_);
        this->InsertRegistry(input_list_types, false, registry);
        library()->SetIsUdaf(name(), sizeof...(IN));
    }

    UdafRegistryHelperImpl& doc(const std::string& doc) {
        SetDoc(doc);
        return *this;
    }

    UdafRegistryHelperImpl& return_list() {
        SetAlwaysReturnList(true);
        return *this;
    }

 private:
    std::vector<const node::TypeNode*> elem_tys_;
    std::vector<int> elem_nullable_;
    node::TypeNode* state_ty_;
    bool state_nullable_;
    node::TypeNode* output_ty_;
    bool output_nullable_;

    UdafDefGen udaf_gen_;
    std::vector<const node::TypeNode*> update_tys_;
    std::vector<int> update_nullable_;
    std::vector<std::string> update_tags_;
};

class DynamicUdafRegistryHelperImpl : public UdfRegistryHelper {
 public:
     DynamicUdafRegistryHelperImpl(const std::string& basename, UdfLibrary* library,
             node::DataType return_type, bool return_nullable,
             const std::vector<node::DataType>& arg_types, bool arg_nullable);
     ~DynamicUdafRegistryHelperImpl() { finalize(); }

    DynamicUdafRegistryHelperImpl& init(const std::string& fname, void* init_context_ptr, void* fn_ptr);
    DynamicUdafRegistryHelperImpl& update(const std::string& fname, void* fn_ptr);
    DynamicUdafRegistryHelperImpl& output(const std::string& fname, void* fn_ptr);

    void finalize();

 private:
    std::vector<const node::TypeNode*> elem_tys_;
    std::vector<int> elem_nullable_;
    node::TypeNode* state_ty_;
    bool state_nullable_;
    node::TypeNode* output_ty_;
    bool output_nullable_;
    bool return_by_arg_;

    UdafDefGen udaf_gen_;
    std::vector<const node::TypeNode*> update_tys_;
    std::vector<int> update_nullable_;
};

template <typename OUT, typename ST, typename... IN>
UdafRegistryHelperImpl<OUT, ST, IN...> UdafRegistryHelper::templates() {
    auto helper_impl =
        UdafRegistryHelperImpl<OUT, ST, IN...>(name(), library());
    helper_impl.doc(this->GetDoc());
    return helper_impl;
}

template <template <typename> typename FTemplate>
class UdafTemplateRegistryHelper : public UdfRegistryHelper {
 public:
    UdafTemplateRegistryHelper(const std::string& name, UdfLibrary* library)
        : UdfRegistryHelper(name, library),
          helper_(name, library) {}

    template <typename... Args>
    UdafTemplateRegistryHelper& args_in() {
        results_ = {RegisterSingle<Args>(helper_)...};
        return *this;
    }

    auto& doc(const std::string& str) {
        helper_.doc(str);
        return *this;
    }

    auto& return_list() {
        helper_.return_list();
        return *this;
    }

    auto& list_argument_at(size_t index) {
        helper_.list_argument_at(index);
        return *this;
    }

 private:
    template <typename T>
    int RegisterSingle(UdafRegistryHelper& helper) {  // NOLINT
        FTemplate<T> inst;
        inst(helper);
        return 0;
    }

    UdafRegistryHelper helper_;
    std::vector<int> results_;
};

class VariadicUdfDefGenBase {
public:
    explicit VariadicUdfDefGenBase() {}
    virtual Status infer(UdfResolveContext* ctx,
                         const std::vector<ExprAttrNode>& args,
                         ExprAttrNode* out) = 0;
    
    std::shared_ptr<UdfRegistry> init_gen;
    ArgSignatureTable update_gen;
    std::unordered_map<std::string, ArgSignatureTable> output_gen;
    int variadic_pos;
};

template <typename... Args>
class VariadicUdfDefGen: public VariadicUdfDefGenBase {
public:
    using InferFType = typename VariadicLLVMUdfGen<Args...>::InferFType;
    template <std::size_t... I>
    Status infer_internal(UdfResolveContext* ctx,
                                 const std::vector<ExprAttrNode>& args,
                                 ExprAttrNode* out, const std::index_sequence<I...>&) {
        std::vector<ExprAttrNode> variadic_args;
        for (size_t i = sizeof...(I); i < args.size(); ++i) {
            variadic_args.emplace_back(args[i]);
        }
        return infer_func(ctx, args[I]..., variadic_args, out);
    }

    explicit VariadicUdfDefGen() {}
    virtual Status infer(UdfResolveContext* ctx,
                         const std::vector<ExprAttrNode>& args,
                         ExprAttrNode* out) override {
        if (infer_func == nullptr) {
            CHECK_TRUE(output_type != nullptr, common::kCodegenError, "No output type");
            out->SetType(output_type);
            out->SetNullable(output_nullable);
            return Status::OK();
        }
        return infer_internal(ctx, args, out, std::index_sequence_for<Args...>());
    }

    InferFType infer_func = nullptr;
    node::TypeNode* output_type = nullptr;
    bool output_nullable = false;
};

class VariadicUdfRegistry : public UdfRegistry {
 public:
    explicit VariadicUdfRegistry(const std::string& name,
                             std::shared_ptr<VariadicUdfDefGenBase> cur_def,
                             size_t fixed_arg_size,
                             const std::vector<size_t>& nullable_arg_indices)
        : UdfRegistry(name),
          cur_def_(cur_def),
          fixed_arg_size_(fixed_arg_size),
          nullable_arg_indices_(nullable_arg_indices) {}

    Status ResolveFunction(UdfResolveContext* ctx,
                           node::FnDefNode** result) override;

 private:
    std::shared_ptr<VariadicUdfDefGenBase> cur_def_;
    size_t fixed_arg_size_;
    std::vector<size_t> nullable_arg_indices_;
};

template<typename ST, typename... Args>
class VariadicUdfRegistryHelper : public UdfRegistryHelper {
    using InferFType = typename VariadicLLVMUdfGen<Args...>::InferFType;

 public:
    VariadicUdfRegistryHelper(const std::string& name, UdfLibrary* library, InferFType infer = nullptr)
        : UdfRegistryHelper(name, library),
          arg_tys_({DataTypeTrait<Args>::to_type_node(library->node_manager())...}),
          arg_nullable_({IsNullableTrait<Args>::value...}),
          state_ty_(DataTypeTrait<ST>::to_type_node(library->node_manager())),
          state_nullable_(IsNullableTrait<ST>::value),
          name_prefix_(name),
          cur_def_(std::make_shared<VariadicUdfDefGen<Args...>>()) {
        cur_def_->infer_func = infer;
        for (const node::TypeNode* arg_type: arg_tys_) {
            name_prefix_.append(".").append(arg_type->GetName());
        }
    }

    ~VariadicUdfRegistryHelper() { finalize(); }

    VariadicUdfRegistryHelper& init(const typename TypeAnnotatedFuncPtr<Args...>::type& fn_ptr) {
        node::TypeNode* ret_type = nullptr;
        fn_ptr.get_ret_type_func(library()->node_manager(), &ret_type);
        if (ret_type == nullptr) {
            LOG(WARNING) << "Fail to get return type of function ptr";
            return *this;
        }
        std::string fname = name_prefix_;
        fname.append("@init");
        if (!ret_type->Equals(state_ty_) || fn_ptr.return_nullable != state_nullable_) {
            LOG(WARNING)
                << "Illegal return type of variadic external function '"
                << fname << "': expected "
                << (state_nullable_ ? "nullable " : "") << state_ty_->GetName()
                << " but get " << (fn_ptr.return_nullable ? "nullable " : "")
                << ret_type->GetName();
            return *this;
        }
        auto fn = dynamic_cast<node::ExternalFnDefNode*>(
            library()->node_manager()->MakeExternalFnDefNode(fname, fn_ptr.ptr,
                ret_type, fn_ptr.return_nullable, arg_tys_, arg_nullable_, -1, fn_ptr.return_by_arg));
        cur_def_->init_gen = std::make_shared<ExternalFuncRegistry>(fname, fn);
        library()->AddExternalFunction(fname, fn_ptr.ptr);
        return *this;
    }

    template<typename IN>
    VariadicUdfRegistryHelper& update(const typename TypeAnnotatedFuncPtr<ST, IN>::type& fn_ptr) {
        std::vector<const node::TypeNode*> update_tys;
        update_tys.push_back(state_ty_);
        update_tys.push_back(DataTypeTrait<IN>::to_type_node(library()->node_manager()));
        std::vector<int> update_nullable;
        update_nullable.push_back(state_nullable_);
        update_nullable.push_back(IsNullableTrait<IN>::value);
        node::TypeNode* ret_type = nullptr;
        fn_ptr.get_ret_type_func(library()->node_manager(), &ret_type);
        if (ret_type == nullptr) {
            LOG(WARNING) << "Fail to get return type of function ptr";
            return *this;
        }

        std::string fname = name_prefix_;
        fname.append("@update");
        fname.append(".").append(update_tys[0]->GetName());
        fname.append(".").append(update_tys[1]->GetName());
        if (!ret_type->Equals(state_ty_) || fn_ptr.return_nullable != state_nullable_) {
            LOG(WARNING)
                << "Illegal return type of variadic external function '"
                << fname << "': expected "
                << (state_nullable_ ? "nullable " : "") << state_ty_->GetName()
                << " but get " << (fn_ptr.return_nullable ? "nullable " : "")
                << ret_type->GetName();
            return *this;
        }
        auto fn = dynamic_cast<node::ExternalFnDefNode*>(
            library()->node_manager()->MakeExternalFnDefNode(fname, fn_ptr.ptr,
                ret_type, fn_ptr.return_nullable, update_tys, update_nullable, -1, fn_ptr.return_by_arg));
        auto registry = std::make_shared<ExternalFuncRegistry>(fname, fn);
        cur_def_->update_gen.Register(update_tys, false, registry);
        library()->AddExternalFunction(fname, fn_ptr.ptr);
        return *this;
    }

    VariadicUdfRegistryHelper& output(const typename TypeAnnotatedFuncPtr<ST>::type& fn_ptr) {
        node::TypeNode* ret_type = nullptr;
        fn_ptr.get_ret_type_func(library()->node_manager(), &ret_type);
        if (ret_type == nullptr) {
            LOG(WARNING) << "Fail to get return type of function ptr";
            return *this;
        }
        if (cur_def_->infer_func == nullptr) {
            cur_def_->output_type = ret_type;
            cur_def_->output_nullable = fn_ptr.return_nullable;
        }

        std::string fname = name_prefix_;
        fname.append("@output@").append(ret_type->GetName());
        fname.append(".").append(state_ty_->GetName());
        auto fn = dynamic_cast<node::ExternalFnDefNode*>(
            library()->node_manager()->MakeExternalFnDefNode(fname, fn_ptr.ptr,
                ret_type, fn_ptr.return_nullable, {state_ty_}, {state_nullable_}, -1, fn_ptr.return_by_arg));
        auto registry = std::make_shared<ExternalFuncRegistry>(fname, fn);
        cur_def_->output_gen[ret_type->GetName()].Register({state_ty_}, false, registry);
        library()->AddExternalFunction(fname, fn_ptr.ptr);
        return *this;
    }

    void finalize() {
        // find nullable arg positions
        std::vector<size_t> null_indices;
        std::vector<int> arg_nullable = {IsNullableTrait<Args>::value...};
        for (size_t i = 0; i < arg_nullable.size(); ++i) {
            if (arg_nullable[i] > 0) {
                null_indices.push_back(i);
            }
        }
        auto registry = std::make_shared<VariadicUdfRegistry>(
            name(), cur_def_, sizeof...(Args), null_indices);
        this->InsertRegistry(
            {DataTypeTrait<Args>::to_type_node(node_manager())...}, true,
            registry);
    }

    VariadicUdfRegistryHelper& doc(const std::string& doc) {
        SetDoc(doc);
        return *this;
    }

 private:
    std::vector<const node::TypeNode*> arg_tys_;
    std::vector<int> arg_nullable_;
    node::TypeNode* state_ty_;
    bool state_nullable_;
    std::string name_prefix_;
    std::shared_ptr<VariadicUdfDefGen<Args...>> cur_def_ = nullptr;
};


}  // namespace udf
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_UDF_UDF_REGISTRY_H_
