/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library.cc
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#include "udf/udf_library.h"
#include "udf/udf_registry.h"

#include <sstream>

namespace fesql {
namespace udf {

std::shared_ptr<UDFTransformRegistry> UDFLibrary::Find(
	const std::string& name) const {
	auto iter = table_.find(name);
	if (iter == table_.end()) {
		return nullptr;
	} else {
		return iter->second;
	}
}

template <typename Helper, typename RegistryT>
Helper UDFLibrary::DoStartRegister(const std::string& name) {
	std::shared_ptr<RegistryT> reg_item;
	auto iter = table_.find(name);
	if (iter == table_.end()) {
		reg_item = std::make_shared<RegistryT>(name);
		table_.insert(iter, std::make_pair(name, reg_item));
	} else {
		reg_item = std::dynamic_pointer_cast<RegistryT>(
			iter->second);
		if (reg_item == nullptr) {
			LOG(WARNING) << "Override exist udf registry for " << name;
			reg_item = std::make_shared<RegistryT>(name);
			table_.insert(iter, std::make_pair(name, reg_item));
		}
	}
	return Helper(reg_item);
}


ExprUDFRegistryHelper UDFLibrary::RegisterExprUDF(
	const std::string& name) {
	return DoStartRegister<
		ExprUDFRegistryHelper, ExprUDFRegistry>(name);
}


ExternalFuncRegistryHelper UDFLibrary::RegisterExternal(
	const std::string& name) {
	return DoStartRegister<
		ExternalFuncRegistryHelper, ExternalFuncRegistry>(name);
}

SimpleUDAFRegistryHelper UDFLibrary::RegisterSimpleUDAF(
	const std::string& name) {
	auto helper = DoStartRegister<
		SimpleUDAFRegistryHelper, SimpleUDAFRegistry>(name);
	helper.SetLibrary(this);
	return helper;
}

Status UDFLibrary::Transform(const std::string& name,
					    	 ExprListNode* args,
             				 const node::SQLNode* over,
				    		 node::NodeManager* manager,
				    		 ExprNode** result) const {
	UDFResolveContext ctx(args, over, manager);
	return this->Transform(name, &ctx, result);
}

Status UDFLibrary::Transform(const std::string& name,
				 		     UDFResolveContext* ctx,
	    		 			 ExprNode** result) const {
	auto iter = table_.find(name);
	CHECK_TRUE(iter != table_.end(),
		"Fail to find registered function: ", name);
	return iter->second->Transform(ctx, result);
}


}  // namespace udf
}  // namespace fesql

