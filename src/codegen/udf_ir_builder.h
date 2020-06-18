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

#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/Module.h"
namespace fesql {
namespace codegen {
class UDFIRBuilder {
 public:
    UDFIRBuilder() {}
    ~UDFIRBuilder() {}
    static bool BuildDayDate(::llvm::Module* module, base::Status& status);  // NOLINT
    static bool BuildMonthDate(::llvm::Module* module, base::Status& status);  // NOLINT
    static bool BuildYearDate(::llvm::Module* module, base::Status& status);  // NOLINT
    static bool BuildTimeUDF(::llvm::Module* module, base::Status& status);   // NOLINT
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_IR_BUILDER_H_
