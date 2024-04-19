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

#ifndef HYBRIDSE_SRC_CODEGEN_IR_BASE_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_IR_BASE_BUILDER_H_

#include <string>
#include <vector>

#include "absl/base/attributes.h"
#include "llvm/IR/IRBuilder.h"
#include "node/sql_node.h"
#include "node/type_node.h"
#include "proto/fe_type.pb.h"

namespace hybridse {
namespace codegen {

bool GetLlvmType(::llvm::Module* m, const ::hybridse::node::TypeNode* type,
                 ::llvm::Type** output);
bool GetLlvmType(::llvm::BasicBlock* block,
                 const ::hybridse::node::DataType& type, ::llvm::Type** output);
bool GetLlvmType(::llvm::BasicBlock* block,
                 const ::hybridse::node::TypeNode* type, ::llvm::Type** output);
bool GetLlvmType(::llvm::Module* m, const ::hybridse::node::DataType& type,
                 ::llvm::Type** output);
bool GetLlvmListType(::llvm::Module* m, const ::hybridse::node::TypeNode* type,
                     ::llvm::Type** output);
bool GetLlvmIteratorType(::llvm::Module* m,
                         const ::hybridse::node::TypeNode* type,
                         ::llvm::Type** output);
bool GetLlvmColumnSize(::hybridse::node::TypeNode* v_type, uint32_t* size);

bool GetBaseType(::llvm::Type* type, ::hybridse::node::DataType* output);
bool IsStringType(::llvm::Type* type);

bool GetFullType(node::NodeManager* nm, ::llvm::Type* type,
                 const ::hybridse::node::TypeNode** type_node);

[[deprecated("can't handle comple data type, use ColumnSchema2Type instead")]]
bool SchemaType2DataType(const ::hybridse::type::Type type, ::hybridse::node::DataType* output);

[[deprecated("can't handle comple data type, use ColumnSchema2Type instead")]]
bool SchemaType2DataType(const ::hybridse::type::Type type, ::hybridse::node::TypeNode* output);

[[deprecated("can not handle complex type, use Type2ColumnSchema instead")]]
bool DataType2SchemaType(const ::hybridse::node::TypeNode& type, ::hybridse::type::Type* output);

absl::Status Type2ColumnSchema(const node::TypeNode* type, type::ColumnSchema* mut_schema) ABSL_ATTRIBUTE_NONNULL();

absl::StatusOr<node::TypeNode*> ColumnSchema2Type(const type::ColumnSchema& schema, node::NodeManager* tmp_nm)
    ABSL_ATTRIBUTE_NONNULL();

bool GetConstFeString(const std::string& val, ::llvm::BasicBlock* block,
                      ::llvm::Value** output);

base::Status GetLlvmFunctionType(
    ::llvm::Module* m, const std::vector<const node::TypeNode*>& arg_types,
    const std::vector<int>& arg_nullable, const node::TypeNode* return_type,
    bool return_nullable, bool variadic, bool* return_by_arg,
    ::llvm::FunctionType** output);

template <typename T>
std::string GetLlvmObjectString(T* obj) {
    if (obj == nullptr) {
        return "<null>";
    }
    std::string res;
    llvm::raw_string_ostream ss(res);
    ss << *obj;
    ss.flush();
    return res;
}

// string representation in IR code for Type
std::string GetIRTypeName(llvm::Type* type) ABSL_ATTRIBUTE_NONNULL();

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

absl::StatusOr<::llvm::Value*> BuildGetPtrOffset(::llvm::IRBuilder<>* builder, ::llvm::Value* ptr,
                                                 ::llvm::Value* offset, ::llvm::Type* dst_type = nullptr);

bool BuildLoadOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                     ::llvm::Value* ptr, ::llvm::Value* offset,
                     ::llvm::Type* type, ::llvm::Value** output);

// write value into `int8` pointer at offset, value type should only base types (intX, float, double)
absl::Status BuildStoreOffset(::llvm::IRBuilder<>* builder, ::llvm::Value* ptr, ::llvm::Value* offset,
                              ::llvm::Value* value);

llvm::Value* CreateAllocaAtHead(llvm::IRBuilder<>* builder, llvm::Type* dtype,
                                const std::string& name,
                                llvm::Value* size = nullptr);

llvm::Value* CodecSizeForPrimitive(llvm::IRBuilder<>* builder, llvm::Type* type);

void PrintLog(llvm::LLVMContext* context, llvm::Module* module, llvm::IRBuilder<>* builder, absl::string_view toPrint,
              bool useGlobal = true);

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_IR_BASE_BUILDER_H_
