/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * native_value.h
 *
 * Author: chenjing
 * Date: 2020/2/12
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_CODEGEN_NATIVE_VALUE_H_
#define SRC_CODEGEN_NATIVE_VALUE_H_

#include <string>

#include "llvm/IR/Module.h"
#include "llvm/IR/IRBuilder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

class NativeValue {
 public:
    ::llvm::Value* GetFlag(::llvm::IRBuilder<>*) const;

    ::llvm::Value* GetValue(::llvm::IRBuilder<>*) const;

    ::llvm::Value* GetAddr(::llvm::IRBuilder<>*) const;

    ::llvm::Type* GetType() const;

    ::llvm::Value* GetRaw() const;

    bool IsMem() const;

    bool IsReg() const;

    bool HasFlag() const;

    bool IsMemFlag() const;

    bool IsRegFlag() const;

    bool IsConstNull() const;

    void SetName(const std::string& name);

    static NativeValue Create(::llvm::Value*);

    static NativeValue CreateNull(::llvm::Type*);

    static NativeValue CreateWithFlag(
        ::llvm::Value*, ::llvm::Value*);

    static NativeValue CreateMemWithFlag(
        ::llvm::Value*, ::llvm::Value*);

    NativeValue Replace(::llvm::Value*) const;

    NativeValue():
        raw_(nullptr), flag_(nullptr), type_(nullptr) {}

 private:
    NativeValue(::llvm::Value* raw, ::llvm::Value* flag,
                ::llvm::Type* type);

    ::llvm::Value* raw_;
    ::llvm::Value* flag_;
    ::llvm::Type* type_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_NATIVE_VALUE_H_
