/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_registry.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_registry.h"

#include <sstream>

namespace fesql {
namespace udf {


Status ExprUDFRegistry::ResolveFunction(UDFResolveContext* ctx,
					                    node::FnDefNode** result) {
    // env check
	if (ctx->over() != nullptr) {
		CHECK_TRUE(allow_window_, 
			"Called in aggregate window is not supported: ", name());
	} else {
		CHECK_TRUE(allow_project_, 
			"Called in project is not supported", name());
	}

	// find generator with specified input argument types
	int variadic_pos = -1;
	std::shared_ptr<ExprUDFGenBase> gen_ptr;
	std::string signature;
	CHECK_STATUS(reg_table_.Find(ctx, &gen_ptr, &signature, &variadic_pos),
		"Fail to resolve fn name \"", name(), "\"");

	LOG(INFO) << "Resolve expression udf \"" << name() << "\" -> "
			  << name() << "(" << signature << ")";

	// construct fn def node:
	// def fn(arg0, arg1, ...argN):
	//     return gen_impl(arg0, arg1, ...argN)
	auto nm = ctx->node_manager();
	auto func_header_param_list = nm->MakeFnListNode();
	std::vector<node::ExprNode*> func_params;
	for (size_t i = 0; i < ctx->arg_size(); ++i) {
		std::string arg_name = "arg_" + std::to_string(i);
		auto arg_type = ctx->arg(i)->GetOutputType();

		func_header_param_list->AddChild(
			nm->MakeFnParaNode(arg_name, arg_type));

		auto arg_expr = nm->MakeExprIdNode(arg_name);
		func_params.emplace_back(arg_expr);	
		arg_expr->SetOutputType(arg_type);
	}
	auto ret_expr = gen_ptr->gen(ctx, func_params);
	CHECK_TRUE(ret_expr != nullptr && !ctx->HasError(),
		"Fail to create expr udf: ", ctx->GetError());

	auto ret_stmt = nm->MakeReturnStmtNode(ret_expr);
	auto body = nm->MakeFnListNode();
	body->AddChild(ret_stmt);
	auto header = nm->MakeFnHeaderNode(
		name(), func_header_param_list, ret_expr->GetOutputType());
	auto fn_def = nm->MakeFnDefNode(header, body);

	*result = reinterpret_cast<node::FnDefNode*>(nm->MakeUDFDefNode(
		reinterpret_cast<node::FnNodeFnDef*>(fn_def)));
	return Status::OK();
}

Status ExprUDFRegistry::Register(
	const std::vector<std::string>& args,
	std::shared_ptr<ExprUDFGenBase> gen_impl_func) {

	return reg_table_.Register(args, gen_impl_func);
}


Status ExternalFuncRegistry::ResolveFunction(UDFResolveContext* ctx,
											 node::FnDefNode** result) {
	// env check
	if (ctx->over() != nullptr) {
		CHECK_TRUE(allow_window_, 
			"Called in aggregate window is not supported: ", name());
	} else {
		CHECK_TRUE(allow_project_, 
			"Called in project is not supported", name());
	}

	// find generator with specified input argument types
	int variadic_pos = -1;
	node::ExternalFnDefNode* external_def = nullptr;
	std::string signature;
	CHECK_STATUS(reg_table_.Find(
		ctx, &external_def, &signature, &variadic_pos),
		"Fail to resolve fn name \"", name(), "\"");

	CHECK_TRUE(external_def->ret_type() != nullptr,
		"No return type specified for ", external_def->function_name());
	LOG(INFO) << "Resolve udf \"" << name() << "\" -> "
			  << external_def->function_name() << "(" << signature << ")";
	*result = external_def;
	return Status::OK();
}

Status ExternalFuncRegistry::Register(
	const std::vector<std::string>& args, node::ExternalFnDefNode* func) {
	return reg_table_.Register(args, func);
}



Status SimpleUDAFRegistry::ResolveFunction(UDFResolveContext* ctx,
					   					   node::FnDefNode** result) {
	CHECK_TRUE(ctx->arg_size() == 1,
		"Input arg_num of simple udaf should be 1: ", name());

	auto arg_type = ctx->arg(0)->GetOutputType();
	CHECK_TRUE(arg_type != nullptr &&
		(arg_type->base_ == node::kIterator ||
		arg_type->base_ == node::kList),
		"Illegal input type for simple udaf: ",
		arg_type == nullptr ? "null" : arg_type->GetName());
	arg_type = arg_type->GetGenericType(0);

	auto iter = reg_table_.find(arg_type->GetName());
	CHECK_TRUE(iter != reg_table_.end(),
		"Fail to find registry for simple udaf ",
		name(), " of input element type ", arg_type->GetName());
	LOG(INFO) << "Resolve simple udaf " << name()
			  << "<" << arg_type->GetName() << ">";
	*result = iter->second;
	return Status::OK();
}

Status SimpleUDAFRegistry::Register(const std::string& input_arg,
			    					node::UDAFDefNode* udaf_def) {
	auto iter = reg_table_.find(input_arg);
	CHECK_TRUE(iter == reg_table_.end(), "Duplicate udaf register '",
		name(), "'' for element type ", input_arg);
	reg_table_.insert(iter, std::make_pair(input_arg, udaf_def));
	return Status::OK();
}


}  // namespace udf
}  // namespace fesql

