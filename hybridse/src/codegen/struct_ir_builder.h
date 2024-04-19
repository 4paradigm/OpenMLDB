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

#ifndef HYBRIDSE_SRC_CODEGEN_STRUCT_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_STRUCT_IR_BUILDER_H_

#include <string>
#include <vector>
#include <memory>

#include "absl/status/statusor.h"
#include "base/fe_status.h"
#include "codegen/native_value.h"
#include "codegen/type_ir_builder.h"

namespace hybridse {
namespace codegen {

class StructTypeIRBuilder : public TypeIRBuilder {
 public:
    explicit StructTypeIRBuilder(::llvm::Module*);
    ~StructTypeIRBuilder();

    // construct corresponding struct ir builder if exists for input type,
    // otherwise, error status returned
    static absl::StatusOr<std::unique_ptr<StructTypeIRBuilder>> CreateStructTypeIRBuilder(::llvm::Module*,
                                                                                          ::llvm::Type*);
    static bool StructCopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist);

    virtual bool CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src, ::llvm::Value* dist) = 0;
    virtual base::Status CastFrom(::llvm::BasicBlock* block, const NativeValue& src, NativeValue* output) = 0;

    // construct the default null safe struct
    absl::StatusOr<NativeValue> CreateNull(::llvm::BasicBlock* block);

    virtual bool CreateDefault(::llvm::BasicBlock* block, ::llvm::Value** output) = 0;

    // Allocate and Initialize the struct value from args, each element in list represent exact argument in SQL literal.
    // For example with map data type, we create it in SQL with `map(key1, value1, ...)`, args is key or value for the
    // result map
    virtual absl::StatusOr<NativeValue> Construct(CodeGenContextBase* ctx, absl::Span<const NativeValue> args) const;

    // construct struct value from llvm values, each element in list represent exact
    // llvm struct field at that index
    virtual absl::StatusOr<::llvm::Value*> ConstructFromRaw(CodeGenContextBase* ctx,
                                                            absl::Span<::llvm::Value* const> args) const;

    // Extract element value from composite data type
    // 1. extract from array type by index
    // 2. extract from struct type by field name
    // 3. extract from map type by key
    virtual absl::StatusOr<NativeValue> ExtractElement(CodeGenContextBase* ctx, const NativeValue& arr,
                                                       const NativeValue& key) const;

    ::llvm::Type* GetType() const;

    std::string GetTypeDebugString() const;

 protected:
    virtual void InitStructType() = 0;

    // allocate the given struct on current stack, no initialization
    bool Allocate(::llvm::BasicBlock* block, ::llvm::Value** output) const;

    // Load the 'idx' th field into ''*output'
    // NOTE: not all types are loaded correctly, e.g for array type
    bool Load(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx, ::llvm::Value** output) const;
    // store 'value' into 'idx' field
    bool Set(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx, ::llvm::Value* value) const;
    // Get the address of 'idx' th field
    bool Get(::llvm::BasicBlock* block, ::llvm::Value* struct_value, unsigned int idx, ::llvm::Value** output) const;

    absl::Status Set(CodeGenContextBase* ctx, ::llvm::Value* struct_value,
                     absl::Span<::llvm::Value* const> members) const;

    // Load and return all fields from struct value pointer
    absl::StatusOr<std::vector<llvm::Value*>> Load(CodeGenContextBase* ctx, llvm::Value* struct_ptr) const;

    void EnsureOK() const;

 protected:
    ::llvm::Module* m_;
    ::llvm::StructType* struct_type_;
};

// construct a safe null value for type
// returns NativeValue{raw, is_null=true} on success, raw is ensured to be not nullptr
absl::StatusOr<NativeValue> CreateSafeNull(::llvm::BasicBlock* block, ::llvm::Type* type);

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_STRUCT_IR_BUILDER_H_
