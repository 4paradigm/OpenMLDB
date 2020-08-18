/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * NullIRBuilder.h
 *
 * Author: chenjing
 * Date: 2020/8/18
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_NULLIRBUILDER_H_
#define SRC_CODEGEN_NULLIRBUILDER_H_
#include "codegen/native_value.h"
namespace fesql {
namespace codegen {
class NullIRBuilder{
 public:
    NullIRBuilder();
    ~NullIRBuilder();
    base::Status CheckAnyNull(::llvm::BasicBlock* block,
                 const NativeValue& value,
                 ::llvm::Value **should_ret_null);
    base::Status CheckAllNull(::llvm::BasicBlock* block,
                              const NativeValue& value,
                              ::llvm::Value **should_ret_null);
};
}
}  // namespace fesql
#endif  // SRC_CODEGEN_NULLIRBUILDER_H_
