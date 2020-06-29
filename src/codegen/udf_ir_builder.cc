/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/
#include "codegen/udf_ir_builder.h"
#include <iostream>
#include <utility>
#include "codegen/date_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "udf/udf.h"
namespace fesql {
namespace codegen {

bool UDFIRBuilder::BuildNativeCUDF(::llvm::Module* module,
                                   fesql::node::FnNodeFnHeander* header,
                                   void* fn_ptr, base::Status& status) {
    if (nullptr == name_function_map_) {
        LOG(WARNING) << "Fail Build Native UDF: name and functin map is null";
        return false;
    }
    codegen::FnIRBuilder fn_ir_builder(module);
    ::llvm::Function* fn;
    if (!fn_ir_builder.CreateFunction(header, &fn, status)) {
        LOG(WARNING) << "Fail to register native udf: "
                     << header->GeIRFunctionName();
        return false;
    }

    if (name_function_map_->find(fn->getName().str()) !=
        name_function_map_->cend()) {
        return false;
    }
    name_function_map_->insert(std::make_pair(fn->getName().str(), fn_ptr));
    DLOG(INFO) << "register native udf: " << fn->getName().str();
    return true;
}

}  // namespace codegen
}  // namespace fesql
