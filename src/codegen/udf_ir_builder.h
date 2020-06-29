/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_UDF_IR_BUILDER_H_
#define SRC_CODEGEN_UDF_IR_BUILDER_H_

#include <map>
#include <string>
#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/Module.h"
#include "node/sql_node.h"
namespace fesql {
namespace codegen {
class UDFIRBuilder {
 public:
    explicit UDFIRBuilder(std::map<std::string, void*>* map)
        : name_function_map_(map) {}
    ~UDFIRBuilder() {}

    bool BuildNativeCUDF(::llvm::Module* module,
                         fesql::node::FnNodeFnHeander* header, void* fn_ptr,
                         base::Status& status);  // NOLINT
    std::map<std::string, void*>* name_function_map_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_IR_BUILDER_H_
