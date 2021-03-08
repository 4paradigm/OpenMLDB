/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef SRC_CODEGEN_IR_BASE_BUILDER_H_
#define SRC_CODEGEN_IR_BASE_BUILDER_H_

#include <string>
#include <vector>
#include "glog/logging.h"
#include "llvm/IR/IRBuilder.h"
#include "node/sql_node.h"
#include "node/type_node.h"
#include "proto/fe_type.pb.h"

using ::fesql::common::kCodegenError;

namespace fesql {
namespace codegen {

bool GetLLVMType(::llvm::Module* m, const ::fesql::node::TypeNode* type,
                 ::llvm::Type** output);
bool GetLLVMType(::llvm::BasicBlock* block, const ::fesql::node::DataType& type,
                 ::llvm::Type** output);
bool GetLLVMType(::llvm::BasicBlock* block, const ::fesql::node::TypeNode* type,
                 ::llvm::Type** output);
bool GetLLVMType(::llvm::Module* m, const ::fesql::node::DataType& type,
                 ::llvm::Type** output);
bool GetLLVMListType(::llvm::Module* m, const ::fesql::node::TypeNode* type,
                     ::llvm::Type** output);
bool GetLLVMIteratorType(::llvm::Module* m, const ::fesql::node::TypeNode* type,
                         ::llvm::Type** output);
bool GetLLVMColumnSize(::fesql::node::TypeNode* v_type, uint32_t* size);

bool GetBaseType(::llvm::Type* type, ::fesql::node::DataType* output);
bool IsStringType(::llvm::Type* type);

bool GetFullType(node::NodeManager* nm, ::llvm::Type* type,
                 const ::fesql::node::TypeNode** type_node);

bool SchemaType2DataType(const ::fesql::type::Type type,
                         ::fesql::node::DataType* output);
bool SchemaType2DataType(const ::fesql::type::Type type,
                         ::fesql::node::TypeNode* output);
bool DataType2SchemaType(const ::fesql::node::TypeNode& type,
                         ::fesql::type::Type* output);

bool GetConstFeString(const std::string& val, ::llvm::BasicBlock* block,
                      ::llvm::Value** output);

base::Status GetLLVMFunctionType(
    ::llvm::Module* m, const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<int>& arg_nullable, const node::TypeNode* return_type,
    bool return_nullable, bool variadic, bool* return_by_arg,
    ::llvm::FunctionType** output);

template <typename T>
std::string GetLLVMObjectString(T* obj) {
    if (obj == nullptr) {
        return "<null>";
    }
    std::string res;
    llvm::raw_string_ostream ss(res);
    ss << *obj;
    ss.flush();
    return res;
}

inline bool GetConstFloat(::llvm::LLVMContext& ctx, float val,  // NOLINT
                          ::llvm::Value** output) {
    *output = ::llvm::ConstantFP::get(ctx, ::llvm::APFloat(val));
    return true;
}

inline bool GetConstDouble(::llvm::LLVMContext& ctx, double val,  // NOLINT
                           ::llvm::Value** output) {
    *output = ::llvm::ConstantFP::get(ctx, ::llvm::APFloat(val));
    return true;
}

bool BuildGetPtrOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                       ::llvm::Value* ptr, ::llvm::Value* offset,
                       ::llvm::Type* type, ::llvm::Value** outptr);

bool BuildLoadOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                     ::llvm::Value* ptr, ::llvm::Value* offset,
                     ::llvm::Type* type, ::llvm::Value** output);

bool BuildStoreOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                      ::llvm::Value* ptr, ::llvm::Value* offset,
                      ::llvm::Value* value);

llvm::Value* CreateAllocaAtHead(llvm::IRBuilder<>* builder, llvm::Type* dtype,
                                const std::string& name,
                                llvm::Value* size = nullptr);

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_IR_BASE_BUILDER_H_
