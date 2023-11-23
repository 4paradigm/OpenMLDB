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

#include "codegen/string_ir_builder.h"
#include <string>
#include <vector>
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/cond_select_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/memery_ir_builder.h"
#include "codegen/null_ir_builder.h"

namespace hybridse {
namespace codegen {

using ::hybridse::common::kCodegenError;

StringIRBuilder::StringIRBuilder(::llvm::Module* m) : StructTypeIRBuilder(m) {
    InitStructType();
}
StringIRBuilder::~StringIRBuilder() {}
void StringIRBuilder::InitStructType() {
    std::string name = "fe.string_ref";
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);
    ::llvm::Type* size_ty = (::llvm::Type::getInt32Ty(m_->getContext()));
    ::llvm::Type* data_ptr_ty = (::llvm::Type::getInt8PtrTy(m_->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(size_ty);
    elements.push_back(data_ptr_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
}
bool StringIRBuilder::NewString(::llvm::BasicBlock* block,
                                const std::string& val,
                                ::llvm::Value** output) {
    ::llvm::IRBuilder<> builder(block);
    ::llvm::StringRef val_ref(val);
    ::llvm::Value* str_val = builder.CreateGlobalStringPtr(val_ref);
    ::llvm::Value* size = builder.getInt32(val.size());
    return NewString(block, size, str_val, output);
}

bool StringIRBuilder::CreateDefault(::llvm::BasicBlock* block,
                                    ::llvm::Value** output) {
    return NewString(block, output);
}

bool StringIRBuilder::NewString(::llvm::BasicBlock* block,
                                ::llvm::Value** output) {
    if (!Create(block, output)) {
        LOG(WARNING) << "Fail to Create Default String";
        return false;
    }
    ::llvm::StringRef val_ref("");
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* str_val = builder.CreateGlobalStringPtr(val_ref);
    if (!SetData(block, *output, str_val)) {
        LOG(WARNING) << "Fail to Init String Data";
        return false;
    }

    if (!SetSize(block, *output, builder.getInt32(0))) {
        LOG(WARNING) << "Fail to Init String Size";
        return false;
    }
    return true;
}
bool StringIRBuilder::NewString(::llvm::BasicBlock* block, ::llvm::Value* size,
                                ::llvm::Value* data, ::llvm::Value** output) {
    if (!Create(block, output)) {
        LOG(WARNING) << "Fail to Create Default String";
        return false;
    }
    if (!SetData(block, *output, data)) {
        LOG(WARNING) << "Fail to Init String Data";
        return false;
    }
    if (!SetSize(block, *output, size)) {
        LOG(WARNING) << "Fail to Init String Size";
        return false;
    }
    return true;
}
bool StringIRBuilder::GetSize(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value** output) {
    return Load(block, str, 0, output);
}

// 浅拷贝
bool StringIRBuilder::CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                               ::llvm::Value* dist) {
    if (nullptr == src || nullptr == dist) {
        LOG(WARNING) << "Fail to copy string: src or dist is null";
        return false;
    }
    if (!IsStringPtr(src->getType()) || !IsStringPtr(dist->getType())) {
        LOG(WARNING) << "Fail to copy string: src or dist isn't String Ptr";
        return false;
    }
    ::llvm::Value* size;
    ::llvm::Value* data;
    if (!GetSize(block, src, &size)) {
        return false;
    }

    if (!GetData(block, src, &data)) {
        return false;
    }

    if (!SetSize(block, dist, size)) {
        return false;
    }

    if (!SetData(block, dist, data)) {
        return false;
    }

    return true;
}
bool StringIRBuilder::SetSize(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value* size) {
    return Set(block, str, 0, size);
}
bool StringIRBuilder::GetData(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value** output) {
    return Load(block, str, 1, output);
}
bool StringIRBuilder::SetData(::llvm::BasicBlock* block, ::llvm::Value* str,
                              ::llvm::Value* data) {
    return Set(block, str, 1, data);
}
base::Status StringIRBuilder::CastFrom(::llvm::BasicBlock* block,
                                       const NativeValue& src,
                                       NativeValue* output) {
    NullIRBuilder null_ir_builder;
    ::llvm::Value* should_ret_null = nullptr;
    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, src, &should_ret_null));
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* casted_value = nullptr;
    CHECK_STATUS(CastFrom(block, src.GetValue(&builder), &casted_value))
    if (should_ret_null) {
        *output = NativeValue::CreateWithFlag(casted_value, should_ret_null);
    } else {
        *output = NativeValue::Create(casted_value);
    }
    return Status::OK();
}
base::Status StringIRBuilder::CastToNumber(::llvm::BasicBlock* block,
                                           const NativeValue& src,
                                           ::llvm::Type* type,
                                           NativeValue* output) {
    CHECK_TRUE(nullptr != output, kCodegenError,
               "fail to cast to number : output native value is null")
    CHECK_TRUE(IsNumber(type), kCodegenError,
               "fail to cast to number: dist type is ")
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* dist_ptr =
        CreateAllocaAtHead(&builder, type, "cast_dist_number_alloca");
    ::llvm::Value* is_null_ptr = CreateAllocaAtHead(
        &builder, builder.getInt1Ty(), "cast_dist_number_is_null, alloca");
    ::std::string fn_name = TypeName(type) + ".string";

    auto cast_func = m_->getOrInsertFunction(
        fn_name,
        ::llvm::FunctionType::get(builder.getVoidTy(),
                                  {src.GetType(), type->getPointerTo(),
                                   builder.getInt1Ty()->getPointerTo()},
                                  false));
    builder.CreateCall(cast_func,
                       {src.GetValue(&builder), dist_ptr, is_null_ptr});
    ::llvm::Value* should_return_null = builder.CreateLoad(is_null_ptr);
    ::llvm::Value* dist = builder.CreateLoad(dist_ptr);

    NullIRBuilder null_ir_builder;
    null_ir_builder.CheckAnyNull(block, src, &should_return_null);
    *output = NativeValue::CreateWithFlag(dist, should_return_null);
    return Status::OK();
}
base::Status StringIRBuilder::CastFrom(::llvm::BasicBlock* block,
                                       ::llvm::Value* src,
                                       ::llvm::Value** output) {
    base::Status status;
    if (nullptr == src || nullptr == output) {
        status.code = common::kCodegenError;
        status.msg = "Fail to cast string: src or dist is null";
        return status;
    }
    if (IsStringPtr(src->getType())) {
        *output = src;
        return status;
    }
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* dist = nullptr;
    if (!NewString(block, &dist)) {
        status.code = common::kCodegenError;
        status.msg = "Fail to cast string: create string fail";
        return status;
    }
    ::std::string fn_name = "string." + TypeName(src->getType());

    auto cast_func = m_->getOrInsertFunction(
        fn_name,
        ::llvm::FunctionType::get(builder.getVoidTy(),
                                  {src->getType(), dist->getType()}, false));
    ::llvm::Value* ret = builder.CreateCall(cast_func, {src, dist});
    if (nullptr == ret) {
        status.code = common::kCodegenError;
        status.msg = "Fail to cast string: invoke " + fn_name + "function fail";
        return status;
    }
    *output = dist;
    return status;
}

base::Status StringIRBuilder::Compare(::llvm::BasicBlock* block,
                                      const NativeValue& s1,
                                      const NativeValue& s2,
                                      NativeValue* output) {
    CHECK_TRUE(nullptr != output, kCodegenError,
               "fail to compare string: output llvm value is null")
    ::std::string fn_name = "strcmp.string.string";

    CHECK_TRUE(TypeIRBuilder::IsStringPtr(s1.GetType()), kCodegenError);
    CHECK_TRUE(TypeIRBuilder::IsStringPtr(s2.GetType()), kCodegenError);
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* should_ret_null = builder.getInt1(false);
    NullIRBuilder null_ir_builder;

    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, s1, &should_ret_null));
    CHECK_STATUS(null_ir_builder.CheckAnyNull(block, s2, &should_ret_null));

    CondSelectIRBuilder cond_select_ir_builder;
    auto func = m_->getOrInsertFunction(
        fn_name,
        ::llvm::FunctionType::get(builder.getInt32Ty(),
                                  {s1.GetType(), s2.GetType()}, false));
    return cond_select_ir_builder.Select(
        block, NativeValue::Create(should_ret_null),
        NativeValue::CreateNull(builder.getInt32Ty()),
        NativeValue::Create(
            builder.CreateCall(func, {s1.GetRaw(), s2.GetRaw()})),
        output);
}
base::Status StringIRBuilder::Concat(::llvm::BasicBlock* block,
                                     const std::vector<NativeValue>& args,
                                     NativeValue* output) {
    return ConcatWS(block, NativeValue::CreateNull(nullptr), args, output);
}

base::Status StringIRBuilder::ConcatWS(::llvm::BasicBlock* block,
                                       const NativeValue& arg,
                                       const std::vector<NativeValue>& args,
                                       NativeValue* output) {
    CHECK_TRUE(nullptr != output, kCodegenError,
               "fail to concat string: output llvm value is null")
    CHECK_TRUE(!args.empty(), kCodegenError,
               "fail to concat string: concat args are empty");
    codegen::MemoryIRBuilder memory_ir_builder(m_);
    codegen::ArithmeticIRBuilder arithmetic_ir_builder(block);
    NullIRBuilder null_ir_builder;
    base::Status status;

    if (args.empty()) {
        ::llvm::Value* empty_string = nullptr;
        CHECK_TRUE(NewString(block, "", &empty_string), kCodegenError,
                   "fail create empty string");
        *output = NativeValue::Create(empty_string);
        return base::Status();
    }

    NativeValue concat_on;
    if (nullptr != arg.GetRaw() && !TypeIRBuilder::IsStringPtr(arg.GetType())) {
        ::llvm::Value* casted_str;
        CHECK_STATUS(
            CastFrom(block, arg.GetRaw(), &casted_str),
            "fail to concat string: concat on arg can't cast to string");
        concat_on = NativeValue::Create(casted_str);
    } else {
        concat_on = arg;
    }

    ::llvm::Value* ret_null = nullptr;
    for (auto& concat_arg : args) {
        CHECK_STATUS(
            null_ir_builder.CheckAnyNull(block, concat_arg, &ret_null));
    }

    std::vector<::llvm::Value*> strs;
    // TODO(chenjing): cast arg to string
    for (size_t i = 0; i < args.size(); i++) {
        if (TypeIRBuilder::IsStringPtr(args[i].GetType())) {
            strs.push_back(args[i].GetRaw());
            continue;
        }
        ::llvm::Value* casted_str;
        CHECK_STATUS(CastFrom(block, args[i].GetRaw(), &casted_str),
                     "fail to concat string: args[", i,
                     "] can't cast to string");
        strs.push_back(casted_str);
    }
    if (1 == strs.size()) {
        *output = NativeValue::CreateWithFlag(strs[0], ret_null);
        return base::Status();
    }

    ::llvm::Value* concat_on_size = nullptr;
    ::llvm::Value* concat_on_data = nullptr;
    if (nullptr != concat_on.GetRaw()) {
        CHECK_TRUE(GetSize(block, concat_on.GetRaw(), &concat_on_size),
                   kCodegenError,
                   "fail to concat string: fail get concat on string size");
        CHECK_TRUE(GetData(block, concat_on.GetRaw(), &concat_on_data),
                   kCodegenError,
                   "fail to concat string: fail get concat on"
                   " string data ptr");
    }

    ::llvm::Value* concat_str_size = nullptr;
    CHECK_TRUE(GetSize(block, strs[0], &concat_str_size), kCodegenError,
               "fail to concat string: fail get 1st string size");

    std::vector<NativeValue> sizes;
    std::vector<NativeValue> datas;

    sizes.push_back(NativeValue::Create(concat_str_size));

    for (size_t i = 1; i < strs.size(); i++) {
        ::llvm::Value* size_i = nullptr;
        CHECK_TRUE(GetSize(block, strs[i], &size_i), kCodegenError,
                   "fail to concat string: fail get ", i + 1, " string size");
        sizes.push_back(NativeValue::Create(size_i));

        if (nullptr != concat_on_size) {
            CHECK_TRUE(
                arithmetic_ir_builder.BuildAddExpr(block, concat_str_size,
                                                   concat_on_size,
                                                   &concat_str_size, status),
                kCodegenError,
                "fail to concat string: fail to compute concat string total "
                "size")
        }
        CHECK_TRUE(
            arithmetic_ir_builder.BuildAddExpr(block, concat_str_size, size_i,
                                               &concat_str_size, status),
            kCodegenError,
            "fail to concat string: fail to compute concat string total size")
    }
    NativeValue concat_str_data;
    CHECK_STATUS(
        memory_ir_builder.Alloc(block, NativeValue::Create(concat_str_size),
                                &concat_str_data),
        "fail to concat string: fail to alloc string size");

    NativeValue addr = concat_str_data;
    for (size_t i = 0; i < strs.size(); i++) {
        if (nullptr != concat_on_data && i >= 1) {
            CHECK_STATUS(memory_ir_builder.MemoryCopy(
                             block, addr, NativeValue::Create(concat_on_data),
                             NativeValue::Create(concat_on_size)),
                         "fail to concat string: fail copy concat on str");
            CHECK_STATUS(
                memory_ir_builder.MemoryAddrAdd(
                    block, addr, NativeValue::Create(concat_on_size), &addr),
                "fail to concat string")
        }
        ::llvm::Value* data_i;
        CHECK_TRUE(GetData(block, strs[i], &data_i), kCodegenError,
                   "fail to concat string: fail get ", i + 1,
                   " string data ptr");
        CHECK_STATUS(memory_ir_builder.MemoryCopy(
                         block, addr, NativeValue::Create(data_i), sizes[i]),
                     kCodegenError, "fail to concat string: fail copy strs[", i,
                     "]");
        CHECK_STATUS(
            memory_ir_builder.MemoryAddrAdd(block, addr, sizes[i], &addr),
            kCodegenError, "fail to concat string")
    }

    ::llvm::Value* concat_str = nullptr;
    CHECK_TRUE(NewString(block, concat_str_size, concat_str_data.GetRaw(),
                         &concat_str),
               kCodegenError,
               "fail to concat string: create concat string fail");
    *output = NativeValue::CreateWithFlag(concat_str, ret_null);
    return base::Status();
}
}  // namespace codegen
}  // namespace hybridse
