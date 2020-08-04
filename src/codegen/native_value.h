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

#include "glog/logging.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"

namespace fesql {
namespace codegen {

class CodeGenContext;

class NativeValue {
 public:
    ::llvm::Value* GetIsNull(::llvm::IRBuilder<>*) const;
    ::llvm::Value* GetIsNull(CodeGenContext*) const;

    ::llvm::Value* GetValue(::llvm::IRBuilder<>*) const;
    ::llvm::Value* GetValue(CodeGenContext*) const;

    ::llvm::Value* GetAddr(::llvm::IRBuilder<>*) const;

    ::llvm::Type* GetType() const;

    ::llvm::Value* GetRaw() const;

    bool IsMem() const;

    bool IsReg() const;

    bool HasFlag() const;

    bool IsMemFlag() const;

    bool IsRegFlag() const;

    bool IsConstNull() const;

    bool IsTuple() const { return args_.size() > 0; }

    NativeValue GetField(size_t i) const { return args_[i]; }

    size_t GetFieldNum() const { return args_.size(); }

    void SetName(const std::string& name);

    static NativeValue Create(::llvm::Value*);

    static NativeValue CreateMem(::llvm::Value*);

    static NativeValue CreateNull(::llvm::Type*);

    static NativeValue CreateWithFlag(::llvm::Value*, ::llvm::Value*);

    static NativeValue CreateMemWithFlag(::llvm::Value*, ::llvm::Value*);

    static NativeValue CreateTuple(const std::vector<NativeValue>& args) {
        NativeValue v(nullptr, nullptr, nullptr);
        v.args_ = args;
        return v;
    }

    NativeValue Replace(::llvm::Value*) const;

    NativeValue WithFlag(::llvm::Value*) const;

    NativeValue() : raw_(nullptr), flag_(nullptr), type_(nullptr) {}

 private:
    NativeValue(::llvm::Value* raw, ::llvm::Value* flag, ::llvm::Type* type);
    ::llvm::Value* raw_;
    ::llvm::Value* flag_;
    ::llvm::Type* type_;
    std::vector<NativeValue> args_;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_NATIVE_VALUE_H_
