/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * memery_ir_builder.h
 *
 * Author: chenjing
 * Date: 2020/7/22
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_MEMERY_IR_BUILDER_H_
#define SRC_CODEGEN_MEMERY_IR_BUILDER_H_
#include <string>
#include "base/fe_status.h"
#include "codegen/scope_var.h"
#include "llvm/IR/IRBuilder.h"
namespace fesql {
namespace codegen {

class MemoryIRBuilder {
 public:
    explicit MemoryIRBuilder(::llvm::Module* m);
    ~MemoryIRBuilder();

    base::Status Alloc(::llvm::BasicBlock* block,
                       const NativeValue& request_size,
                       NativeValue* output);  // NOLINT
    base::Status MemoryCopy(::llvm::BasicBlock* block, const NativeValue& dist,
                            const NativeValue& src, const NativeValue& size);

    base::Status MemoryAddrAdd(::llvm::BasicBlock* block,
                               const NativeValue& addr, const NativeValue& size,
                               NativeValue* new_addr);

 private:
    ::llvm::Module* m_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_MEMERY_IR_BUILDER_H_
