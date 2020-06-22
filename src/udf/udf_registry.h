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

#include <string>
#include <vector>
#include <utility>

#include "base/fe_status.h"
#include "node/sql_node.h"
#include "node/node_manager.h"

namespace fesql {
namespace udf {

using fesql::base::Status;
using fesql::node::ExprListNode;
using fesql::node::ExprNode;
using fesql::node::SQLNode;

// Forward declarations
class ExprUDFRegistryHelper;
class ExternalFuncRegistryHelper;
class SimpleUDAFRegistryHelper;


/**
  * Overall information to resolve a sql function call.
  */
class UDFResolveContext {
 public:
 	UDFResolveContext(const ExprListNode* args,
                      const node::SQLNode* over,
 		    		  node::NodeManager* manager):
 		args_(args), over_(over), manager_(manager) {}

 	const ExprListNode* args() { return args_; }
 	const node::SQLNode* over() { return over_; }
 	node::NodeManager* node_manager() { return manager_; }

 	size_t arg_size() const { return args_->GetChildNum(); }
 	const ExprNode* arg(size_t i) const { return args_->GetChild(i); }

 	const std::string& GetError() const { return error_msg_; }
 	void SetError(const std::string& err) { error_msg_ = err; }
 	bool HasError() const { return error_msg_ != ""; }

 private:
 	const ExprListNode* args_;
 	const SQLNode* over_;
 	node::NodeManager* manager_;

 	std::string error_msg_;
};


/**
  * Interface to implement resolve and transform 
  * logic for sql function call with fn name.
  */
class UDFTransformRegistry {
 public:
 	explicit UDFTransformRegistry(const std::string& name):
 		name_(name) {}

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
 	explicit UDFRegistry(const std::string& name):
 		UDFTransformRegistry(name) {}

 	// "f(arg0, arg1, ...argN)" -> resolved f
 	virtual Status ResolveFunction(UDFResolveContext* ctx,
 								   node::FnDefNode** result) = 0;

 	virtual ~UDFRegistry() {}

 	Status Transform(UDFResolveContext* ctx,
 					 node::ExprNode** result) override {
 		node::FnDefNode* fn_def = nullptr;
 		CHECK_STATUS(ResolveFunction(ctx, &fn_def));

 		*result = ctx->node_manager()->MakeFuncNode(
 			fn_def, ctx->args(), ctx->over());
 		return Status::OK();
 	}
};


/**
  * Hold global udf registry entries.
  * "fn(arg0, arg1, ...argN)" -> some expression
  */
class UDFLibrary {
 public:
 	Status Transform(const std::string& name,
 					 const ExprListNode* args,
                     const node::SQLNode* over,
 		    		 node::NodeManager* manager,
 		    		 ExprNode** result) const;

 	Status Transform(const std::string& name,
 					 UDFResolveContext* ctx,
 		    		 ExprNode** result) const;

 	std::shared_ptr<UDFTransformRegistry> Find(
 		const std::string& name) const;

 	// register interfaces
 	ExprUDFRegistryHelper RegisterExprUDF(const std::string& name);
 	ExternalFuncRegistryHelper RegisterExternal(const std::string& name);
 	SimpleUDAFRegistryHelper RegisterSimpleUDAF(const std::string& name);

 private:
 	template <typename Helper, typename RegistryT>
	Helper DoStartRegister(const std::string& name);

 	std::unordered_map<std::string,
 		std::shared_ptr<UDFTransformRegistry>> table_;
};


template <typename T>
class ArgSignatureTable {
 public:
 	Status Find(UDFResolveContext* ctx, T* res,
 				std::string* signature, int* variadic_pos) {
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
 		typename TableType::iterator variadic_placeholder_match_iter = table_.end();
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
 				if (input_args.size() < non_variadic_arg_num) {
 					continue;
 				}
 				for (size_t j = 0; j < non_variadic_arg_num; ++j) {
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
		table_.insert(iter, std::make_pair(key,
			std::make_pair(t, args)));
		return Status::OK();
 	}

 private:
 	using TableType = std::unordered_map<std::string,
 		std::pair<T, std::vector<std::string>>>;
 	TableType table_;
};


template <typename T>
struct ArgTypeTrait {
	static std::string to_string();
	static node::TypeNode* to_type_node(node::NodeManager* nm);
	static node::ExprNode* to_const(node::NodeManager* nm, const T&);
};

struct AnyArg {
	AnyArg() = delete;
};

template<>
struct ArgTypeTrait<AnyArg> {
	static std::string to_string() { return "?"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nullptr;
	}
};

template<>
struct ArgTypeTrait<bool> {
	static std::string to_string() { return "bool"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kBool);
	}
};

template<>
struct ArgTypeTrait<int16_t> {
	static std::string to_string() { return "int16"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kInt16);
	}
};

template<>
struct ArgTypeTrait<int32_t> {
	static std::string to_string() { return "int32"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kInt32);
	}
	static node::ExprNode* to_const(node::NodeManager* nm, const int32_t& v) {
		return nm->MakeConstNode(v);
	}
};

template<>
struct ArgTypeTrait<int64_t> {
	static std::string to_string() { return "int64"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kInt64);
	}
};

template<>
struct ArgTypeTrait<float> {
	static std::string to_string() { return "float"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kFloat);
	}
	static node::ExprNode* to_const(node::NodeManager* nm, const float& v) {
		return nm->MakeConstNode(v);
	}
};

template<>
struct ArgTypeTrait<double> {
	static std::string to_string() { return "double"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kDouble);
	}
	static node::ExprNode* to_const(node::NodeManager* nm, const double& v) {
		return nm->MakeConstNode(v);
	}
};

template<>
struct ArgTypeTrait<std::string> {
	static std::string to_string() { return "string"; }
	static node::TypeNode* to_type_node(node::NodeManager* nm) {
		return nm->MakeTypeNode(node::kVarchar);
	}
};


template <typename ...LiteralArgTypes>
const std::string LiteralToArgTypesSignature() {
	std::stringstream ss;
	size_t idx = 0;
	for (auto type_str : {ArgTypeTrait<LiteralArgTypes>::to_string()...}) {
		ss << type_str;
		if (idx < sizeof...(LiteralArgTypes) - 1) {
			ss << ", ";
		}
		idx += 1;
	}
	return ss.str();
}


struct ExprUDFGenBase {
	virtual ExprNode* gen(UDFResolveContext* ctx,
						  const std::vector<ExprNode*>& args) = 0;
};


template <typename ...LiteralArgTypes>
struct ExprUDFGen : public ExprUDFGenBase {
	using FType = std::function<ExprNode*(
		UDFResolveContext*,
		typename std::pair<LiteralArgTypes, ExprNode*>::second_type...)>;

	ExprNode* gen(UDFResolveContext* ctx,
				  const std::vector<ExprNode*>& args) override {
		return gen_internal(ctx, args,
			std::index_sequence_for<LiteralArgTypes...>());
	};

	template<std::size_t... I>
	ExprNode* gen_internal(UDFResolveContext* ctx,
				           const std::vector<ExprNode*>& args,
				           const std::index_sequence<I...>&) {
		return gen_func(ctx, args[I]...);
	};

	explicit ExprUDFGen(const FType& f): gen_func(f) {}
	const FType gen_func;
};


template <typename ...LiteralArgTypes>
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

	template<std::size_t... I>
	ExprNode* gen_internal(UDFResolveContext* ctx,
				           const std::vector<ExprNode*>& args,
				           const std::index_sequence<I...>&) {
		std::vector<ExprNode*> variadic_args;
		for (size_t i = sizeof...(I); i < args.size(); ++i) {
			variadic_args.emplace_back(args[i]);
		}
		return this->gen_func(ctx, args[I]..., variadic_args);
	};

	explicit VariadicExprUDFGen(const FType& f): gen_func(f) {}
	const FType gen_func;
};


/**
  * Interface to resolve udf with expression construction.
  */
class ExprUDFRegistry : public UDFRegistry {
 public:
 	explicit ExprUDFRegistry(const std::string& name):
 		UDFRegistry(name), allow_window_(true), allow_project_(true) {}

 	Status ResolveFunction(UDFResolveContext* ctx,
 						   node::FnDefNode** result) override;

 	Status Register(const std::vector<std::string>& args,
 				    std::shared_ptr<ExprUDFGenBase> gen_impl_func);

 	void SetAllowWindow(bool flag) {
 		this->allow_window_ = flag;
 	}

 	void SetAllowProject(bool flag) {
 		this->allow_project_ = flag;
 	}

 	template <typename ...LiteralArgTypes>
 	using TemplateGenFunctor = ExprUDFGen<LiteralArgTypes...>;

 	template <typename ...LiteralArgTypes>
 	using TemplateVariadicGenFunctor = VariadicExprUDFGen<LiteralArgTypes...>;

 private:
 	ArgSignatureTable<std::shared_ptr<ExprUDFGenBase>> reg_table_;
 	bool allow_window_;
 	bool allow_project_;
};


template <typename RegistryT>
class UDFRegistryHelper {
 public:
	explicit UDFRegistryHelper(std::shared_ptr<RegistryT> registry):
		registry_(registry) {}

	template <typename ...LiteralArgTypes>
	UDFRegistryHelper<RegistryT>& args(
		const typename ExprUDFGen<LiteralArgTypes...>::FType& func) {

		auto gen_ptr = std::make_shared<typename RegistryT::template
			TemplateGenFunctor<LiteralArgTypes...>>(func);

		std::vector<std::string> type_args(
			{ArgTypeTrait<LiteralArgTypes>::to_string()...});
		
		registry_->Register(type_args, gen_ptr);
		return *this;
	}

	template <typename ...LiteralArgTypes>
	UDFRegistryHelper<RegistryT>& variadic_args(
		const typename VariadicExprUDFGen<LiteralArgTypes...>::FType& func) {

		auto gen_ptr = std::make_shared<typename RegistryT::template
			TemplateVariadicGenFunctor<LiteralArgTypes...>>(func);

		std::vector<std::string> type_args(
			{ArgTypeTrait<LiteralArgTypes>::to_string()...});
		type_args.emplace_back("...");
		
		registry_->Register(type_args, gen_ptr);
		return *this;
	}

	std::shared_ptr<RegistryT> GetRegistry() const { return registry_; }

 private:
 	std::shared_ptr<RegistryT> registry_;
};


class ExprUDFRegistryHelper : public UDFRegistryHelper<ExprUDFRegistry> {
 public:
	explicit ExprUDFRegistryHelper(std::shared_ptr<ExprUDFRegistry> registry):
		UDFRegistryHelper<ExprUDFRegistry>(registry) {}

	ExprUDFRegistryHelper& allow_project(bool flag) {
		GetRegistry()->SetAllowProject(flag);
		return *this;
	}

	ExprUDFRegistryHelper& allow_window(bool flag) {
		GetRegistry()->SetAllowWindow(flag);
		return *this;
	}
};


/**
  * Interface to resolve udf to external native functions.
  */
class ExternalFuncRegistry : public UDFRegistry {
 public:
 	explicit ExternalFuncRegistry(const std::string& name):
 		UDFRegistry(name), allow_window_(true), allow_project_(true) {}

 	Status ResolveFunction(UDFResolveContext* ctx,
 						   node::FnDefNode** result) override;

 	Status Register(const std::vector<std::string>& args,
 				    node::ExternalFnDefNode* func);

 	void SetAllowWindow(bool flag) {
 		this->allow_window_ = flag;
 	}

 	void SetAllowProject(bool flag) {
 		this->allow_project_ = flag;
 	}

 	node::NodeManager* node_manager() { return &nm_; }

 private:
 	node::NodeManager nm_;
 	ArgSignatureTable<node::ExternalFnDefNode*> reg_table_;
 	bool allow_window_;
 	bool allow_project_;
};


class ExternalFuncRegistryHelper:
	public UDFRegistryHelper<ExternalFuncRegistry> {

 public:
	explicit ExternalFuncRegistryHelper(
		std::shared_ptr<ExternalFuncRegistry> registry):
		UDFRegistryHelper<ExternalFuncRegistry>(registry),
		cur_def_(nullptr) {}

	ExternalFuncRegistryHelper& allow_project(bool flag) {
		GetRegistry()->SetAllowProject(flag);
		return *this;
	}

	ExternalFuncRegistryHelper& allow_window(bool flag) {
		GetRegistry()->SetAllowWindow(flag);
		return *this;
	}

	template <typename ...LiteralArgTypes>
	ExternalFuncRegistryHelper& args(const std::string& name, void* fn_ptr) {
		if (cur_def_ != nullptr && cur_def_->ret_type() == nullptr) {
			LOG(WARNING) << "Function " << cur_def_->function_name()
						 << " is not specified with return type for "
						 << " udf registry " << GetRegistry()->name();
		}
		std::vector<std::string> type_args(
			{ArgTypeTrait<LiteralArgTypes>::to_string()...});

		node::NodeManager* nm = GetRegistry()->node_manager();
		std::vector<const node::TypeNode*> type_nodes(
			{ArgTypeTrait<LiteralArgTypes>::to_type_node(nm)...});
		
		cur_def_ = dynamic_cast<node::ExternalFnDefNode*>(
			nm->MakeExternalFnDefNode(name, fn_ptr, nullptr, type_nodes, -1));
		GetRegistry()->Register(type_args, cur_def_);
		return *this;
	}

	template <typename ...LiteralArgTypes>
	ExternalFuncRegistryHelper& variadic_args(const std::string& name,
									     	  void* fn_ptr) {
		if (cur_def_ != nullptr && cur_def_->ret_type() == nullptr) {
			LOG(WARNING) << "Function " << cur_def_->function_name()
						 << " is not specified with return type for "
						 << " udf registry " << GetRegistry()->name();
		}
		std::vector<std::string> type_args(
			{ArgTypeTrait<LiteralArgTypes>::to_string()...});
		type_args.emplace_back("...");

		node::NodeManager* nm = GetRegistry()->node_manager();
		std::vector<const node::TypeNode*> type_nodes(
			{ArgTypeTrait<LiteralArgTypes>::to_type_node(nm)...});
		
		cur_def_ = dynamic_cast<node::ExternalFnDefNode*>(
			nm->MakeExternalFnDefNode(name, fn_ptr, nullptr, type_nodes,
				sizeof...(LiteralArgTypes)));
		GetRegistry()->Register(type_args, cur_def_);
		return *this;
	}

	template <typename RetType>
	ExternalFuncRegistryHelper& returns() {
		if (cur_def_ == nullptr) {
			LOG(WARNING) << "No arg types specified for "
						 << " udf registry " << GetRegistry()->name();
			return *this;
		}
		auto ret_type = ArgTypeTrait<RetType>::to_type_node(
			GetRegistry()->node_manager());
		cur_def_->SetRetType(ret_type);
		return *this;
	}

 private:
 	node::ExternalFnDefNode* cur_def_;
};



class SimpleUDAFRegistry : public UDFRegistry {
 public:
 	explicit SimpleUDAFRegistry(const std::string& name):
 		UDFRegistry(name) {}

 	node::NodeManager* node_manager() { return &nm_; }

 	Status Register(const std::string& input_arg,
 				    node::UDAFDefNode* udaf_def);

 	Status ResolveFunction(UDFResolveContext* ctx,
 						   node::FnDefNode** result) override;

 private:
 	// input arg type -> udaf def
 	std::unordered_map<std::string, node::UDAFDefNode*> reg_table_;
 	node::NodeManager nm_;
};


template <typename IN, typename ST, typename OUT>
class SimpleUDAFRegistryHelperImpl;

class SimpleUDAFRegistryHelper : public UDFRegistryHelper<SimpleUDAFRegistry>{
 public:
 	explicit SimpleUDAFRegistryHelper(
		std::shared_ptr<SimpleUDAFRegistry> registry):
		UDFRegistryHelper<SimpleUDAFRegistry>(registry) {}

 	template <typename IN, typename ST, typename OUT>
 	SimpleUDAFRegistryHelperImpl<IN, ST, OUT> templates();

 	UDFLibrary* GetLibrary() { return library_; }
 	void SetLibrary(UDFLibrary* lib) { library_ = lib; }

 private:
 	UDFLibrary* library_;
};

template <typename IN, typename ST, typename OUT>
class SimpleUDAFRegistryHelperImpl {
 public:
 	explicit SimpleUDAFRegistryHelperImpl(
 			UDFLibrary* library, std::shared_ptr<SimpleUDAFRegistry> registry):
 		library_(library), registry_(registry),
 		nm_(registry_->node_manager()),
 		input_ty_(ArgTypeTrait<IN>::to_type_node(nm_)), 
 		state_ty_(ArgTypeTrait<ST>::to_type_node(nm_)),
 		output_ty_(ArgTypeTrait<OUT>::to_type_node(nm_)) {};

 	template <typename NewIN, typename NewST, typename NewOUT>
 	SimpleUDAFRegistryHelperImpl<NewIN, NewST, NewOUT> templates() {
 		finalize();
 		return SimpleUDAFRegistryHelperImpl<NewIN, NewST, NewOUT>(
 			library_, registry_);
 	}

 	SimpleUDAFRegistryHelperImpl& init(const std::string& fname) {
 		node::FnDefNode* fn = fn_spec_by_name(fname, {});
 		if (check_fn_ret_type(fname, fn, state_ty_)) {
 			this->init_ = nm_->MakeFuncNode(fn, nm_->MakeExprList(), nullptr);
 		}
 		return *this;
 	}

 	SimpleUDAFRegistryHelperImpl& init(const std::string& fname, void* fn_ptr) {
 		auto fn = dynamic_cast<node::ExternalFnDefNode*>(
 			nm_->MakeExternalFnDefNode(fname, fn_ptr, state_ty_, {}, -1));
 		this->init_ = nm_->MakeFuncNode(fn, nm_->MakeExprList(), nullptr);
 		return *this;
 	}

 	SimpleUDAFRegistryHelperImpl& const_init(const ST& value) {
 		this->init_ = ArgTypeTrait<ST>::to_const(nm_, value);
 		return *this;
 	}

 	SimpleUDAFRegistryHelperImpl& update(const std::string& fname) {
 		node::FnDefNode* fn = fn_spec_by_name(fname, {state_ty_, input_ty_});
 		if (check_fn_ret_type(fname, fn, state_ty_)) {
 			this->update_ = fn;
 		}
 		return *this;
 	}

 	SimpleUDAFRegistryHelperImpl& update(const std::string& fname, void* fn_ptr) {
 		auto fn = dynamic_cast<node::ExternalFnDefNode*>(
 			nm_->MakeExternalFnDefNode(
 				fname, fn_ptr, state_ty_, {state_ty_, input_ty_}, -1));
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

 	SimpleUDAFRegistryHelperImpl& merge(const std::string& fname, void* fn_ptr) {
 		auto fn = dynamic_cast<node::ExternalFnDefNode*>(
 			nm_->MakeExternalFnDefNode(
 				fname, fn_ptr, state_ty_, {state_ty_, state_ty_}, -1));
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

 	SimpleUDAFRegistryHelperImpl& output(const std::string& fname, void* fn_ptr) {
 		auto fn = dynamic_cast<node::ExternalFnDefNode*>(
 			nm_->MakeExternalFnDefNode(
 				fname, fn_ptr, output_ty_, {state_ty_}, -1));
 		this->output_ = fn;
 		return *this;
 	}

 	void finalize() {
 		if (init_ == nullptr) {
 			LOG(WARNING) << "Init expr not specified for "
 				<< registry_->name()
 				<< "<" << input_ty_->GetName() << ", "
 				<< state_ty_->GetName() << ", "
 				<< output_ty_->GetName() << ">";
 		} else if (update_ == nullptr) {
 			LOG(WARNING) << "Update function not specified for "
 				<< registry_->name()
 				<< "<" << input_ty_->GetName() << ", "
 				<< state_ty_->GetName() << ", "
 				<< output_ty_->GetName() << ">";
 		}
 		auto udaf = dynamic_cast<node::UDAFDefNode*>(
 			nm_->MakeUDAFDefNode(init_, update_, merge_, output_));
 		registry_->Register(input_ty_->GetName(), udaf);
 	}

 private:
 	template <typename ...RetType>
 	bool check_fn_ret_type(const std::string& ref, 
 						   node::FnDefNode* fn, node::TypeNode* expect) {
 		if (fn == nullptr) { return false; }
 		const node::TypeNode* ret_type = fn->GetReturnType();
 		if (ret_type != nullptr && ret_type->Equals(expect)) {
 			return true;
 		} else {
 			LOG(WARNING) << "Illegal return type of " << ref << ": "
 				<< (ret_type == nullptr? "null" : ret_type->GetName())
 				<< ", expect " << expect->GetName();
 			return false;
 		}
 	}

 	template <typename ...LiteralArgTypes>
 	node::FnDefNode* fn_spec_by_name(const std::string& name,
 			const std::vector<node::TypeNode*>& arg_types) {
 		std::vector<node::ExprNode> dummy_args;
 		auto arg_list = nm_->MakeExprList();
 		for (size_t i = 0; i < arg_types.size(); ++i) {
 			std::string arg_name = "arg_" + std::to_string(i);
 			node::ExprNode* arg = nm_->MakeExprIdNode(arg_name);
 			arg->SetOutputType(arg_types[i]);
 			arg_list->AddChild(arg);
 		}
 
 		auto registry = std::dynamic_pointer_cast<UDFRegistry>(
 			library_->Find(name));
 		if (registry == nullptr) {
 			LOG(WARNING) << "No function def registry '" << name
 				<< "'' find for simple udaf " << registry_->name();
 			return nullptr;
 		}
 		node::FnDefNode* res = nullptr;
 		UDFResolveContext ctx(arg_list, nullptr, nm_);
 		auto status = registry->ResolveFunction(&ctx, &res);
 		if (!status.isOK()) {
 			LOG(WARNING) << "Resolve sub function def '" << name
 				<< "'' for simple udaf " << registry_->name()
 				<< " failed: " << status.msg;
 			return nullptr;
 		}
 		return res;
 	}

 	UDFLibrary* library_;
 	std::shared_ptr<SimpleUDAFRegistry> registry_;

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
	return SimpleUDAFRegistryHelperImpl<IN, ST, OUT>(library_, GetRegistry());
}

}  // namespace udf
}  // namespace fesql


#endif  // SRC_UDF_UDF_REGISTRY_H_
