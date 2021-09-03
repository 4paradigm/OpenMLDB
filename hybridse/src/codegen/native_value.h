/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CODEGEN_NATIVE_VALUE_H_
#define SRC_CODEGEN_NATIVE_VALUE_H_

#include <string>
#include <vector>

#include "glog/logging.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"

namespace hybridse {
namespace codegen {

class CodeGenContext;

class NativeValue {
 public:
    ::llvm::Value* GetIsNull(::llvm::IRBuilder<>*) const;
    ::llvm::Value* GetIsNull(CodeGenContext*) const;

    ::llvm::Value* GetValue(::llvm::IRBuilder<>*) const;
    ::llvm::Value* GetValue(CodeGenContext*) const;

    ::llvm::Value* GetAddr(::llvm::IRBuilder<>*) const;

    void SetType(::llvm::Type* type);
    ::llvm::Type* GetType() const;
    ::llvm::Value* GetRaw() const;

    bool IsMem() const;

    bool IsReg() const;

    bool IsNullable() const;

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
}  // namespace hybridse
#endif  // SRC_CODEGEN_NATIVE_VALUE_H_
