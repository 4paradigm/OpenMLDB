/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * udf_library.h
 *
 * Author: chenjing
 * Date: 2019/11/26
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_UDF_UDF_LIBRARY_H_
#define SRC_UDF_UDF_LIBRARY_H_

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
class UDFTransformRegistry;
class UDFResolveContext;


/**
  * Hold global udf registry entries.
  * "fn(arg0, arg1, ...argN)" -> some expression
  */
class UDFLibrary {
 public:
 	Status Transform(const std::string& name,
 					 ExprListNode* args,
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



}  // namespace udf
}  // namespace fesql


#endif  // SRC_UDF_UDF_LIBRARY_H_
