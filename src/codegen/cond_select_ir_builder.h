/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * cond_select_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/8/18
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_COND_SELECT_IR_BUILDER_H_
#define SRC_CODEGEN_COND_SELECT_IR_BUILDER_H_
#include "base/fe_status.h"
#include "codegen/native_value.h"
namespace fesql {
namespace codegen {
class CondSelectIRBuilder {
 public:
    CondSelectIRBuilder();
    ~CondSelectIRBuilder();
    base::Status Select(::llvm::BasicBlock* block, const NativeValue& cond,
                        const NativeValue& left, const NativeValue& right,
                        NativeValue* output);
};
}  // namespace codegen
}  // namespace fesql

#endif  // SRC_CODEGEN_COND_SELECT_IR_BUILDER_H_
