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
    static bool BuildMinuteTimestamp(::llvm::Module* module,
                                     base::Status& status);  // NOLINT
    static bool BuildSecondTimestamp(::llvm::Module* module,
                                     base::Status& status);  // NOLINT
    static bool BuildHourTimestamp(::llvm::Module* module,
                                   base::Status& status);  // NOLINT
    static bool BuildMinuteInt64(::llvm::Module* module,
                                 base::Status& status);  // NOLINT
    static bool BuildSecondInt64(::llvm::Module* module,
                                 base::Status& status);  // NOLINT
    static bool BuildHourInt64(::llvm::Module* module,
                               base::Status& status);  // NOLINT
    static bool BuildDayDate(::llvm::Module* module,
                             base::Status& status);  // NOLINT
    static bool BuildMonthDate(::llvm::Module* module,
                               base::Status& status);  // NOLINT
    static bool BuildYearDate(::llvm::Module* module,
                              base::Status& status);  // NOLINT
    bool BuildUDF(::llvm::Module* module,
                  base::Status& status);  // NOLINT
    bool BuildNativeCUDF(::llvm::Module* module,
                         fesql::node::FnNodeFnHeander* header, void* fn_ptr,
                         base::Status& status);  // NOLINT
    static bool GetLibsFiles(const std::string& dir_path,
                             std::vector<std::string>& filenames,    // NOLINT
                             base::Status& status);                  // NOLINT
    static bool BuildFeLibs(llvm::Module* m, base::Status& status);  // NOLINT
    static bool CompileFeScript(llvm::Module* m, const std::string& path,
                                base::Status& status);  // NOLINT
    std::map<std::string, void*>* name_function_map_;
};
}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_UDF_IR_BUILDER_H_
