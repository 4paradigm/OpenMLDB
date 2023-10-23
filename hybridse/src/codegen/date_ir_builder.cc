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

#include "codegen/date_ir_builder.h"
#include <string>
#include <vector>
#include "codegen/arithmetic_expr_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/null_ir_builder.h"

namespace hybridse {
namespace codegen {

DateIRBuilder::DateIRBuilder(::llvm::Module* m) : StructTypeIRBuilder(m) {
    InitStructType();
}
DateIRBuilder::~DateIRBuilder() {}
void DateIRBuilder::InitStructType() {
    std::string name = "fe.date";
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);
    ::llvm::Type* ts_ty = (::llvm::Type::getInt32Ty(m_->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(ts_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
    return;
}

base::Status DateIRBuilder::CreateNull(::llvm::BasicBlock* block, NativeValue* output) {
    ::llvm::Value* value = nullptr;
    CHECK_TRUE(CreateDefault(block, &value), common::kCodegenError, "Fail to construct string")
    ::llvm::IRBuilder<> builder(block);
    *output = NativeValue::CreateWithFlag(value, builder.getInt1(true));
    return base::Status::OK();
}

bool DateIRBuilder::CreateDefault(::llvm::BasicBlock* block,
                                  ::llvm::Value** output) {
    return NewDate(block, output);
}
bool DateIRBuilder::NewDate(::llvm::BasicBlock* block, ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* date;
    if (!Create(block, &date)) {
        return false;
    }
    if (!SetDate(block, date,
                 ::llvm::ConstantInt::get(
                     ::llvm::Type::getInt32Ty(m_->getContext()), 0, false))) {
        return false;
    }
    *output = date;
    return true;
}
bool DateIRBuilder::NewDate(::llvm::BasicBlock* block, ::llvm::Value* days,
                            ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Value* date;
    if (!Create(block, &date)) {
        return false;
    }
    if (!SetDate(block, date, days)) {
        return false;
    }
    *output = date;
    return true;
}
bool DateIRBuilder::CopyFrom(::llvm::BasicBlock* block, ::llvm::Value* src,
                             ::llvm::Value* dist) {
    if (nullptr == src || nullptr == dist) {
        LOG(WARNING) << "Fail to copy string: src or dist is null";
        return false;
    }
    if (!IsDatePtr(src->getType()) || !IsDatePtr(dist->getType())) {
        LOG(WARNING) << "Fail to copy string: src or dist isn't Date Ptr";
        return false;
    }
    ::llvm::Value* days;
    if (!GetDate(block, src, &days)) {
        return false;
    }
    if (!SetDate(block, dist, days)) {
        return false;
    }
    return true;
}
base::Status DateIRBuilder::CastFrom(::llvm::BasicBlock* block,
                                     const NativeValue& src,
                                     NativeValue* output) {
    base::Status status;
    NullIRBuilder null_ir_builder;

    if (IsDatePtr(src.GetType())) {
        *output = src;
        return status;
    } else if (IsTimestampPtr(src.GetType()) || IsStringPtr(src.GetType())) {
        ::llvm::IRBuilder<> builder(block);
        ::llvm::Value* dist = nullptr;
        ::llvm::Value* is_null_ptr = CreateAllocaAtHead(
            &builder, builder.getInt1Ty(), "timestamp_is_null_alloca");
        if (!CreateDefault(block, &dist)) {
            status.code = common::kCodegenError;
            status.msg = "Fail to cast date: create default date fail";
            return status;
        }
        ::std::string fn_name = "date." + TypeName(src.GetType());

        auto cast_func = m_->getOrInsertFunction(
            fn_name,
            ::llvm::FunctionType::get(builder.getVoidTy(),
                                      {src.GetType(), dist->getType(), builder.getInt1Ty()->getPointerTo()}, false));

        builder.CreateCall(cast_func, {src.GetValue(&builder), dist, is_null_ptr});

        ::llvm::Value* should_return_null = builder.CreateLoad(is_null_ptr);
        null_ir_builder.CheckAnyNull(block, src, &should_return_null);
        *output = NativeValue::CreateWithFlag(dist, should_return_null);
    } else {
        return base::Status(
            common::kCodegenError,
            "Fail to cast from " + TypeName(src.GetType()) + " to date");
    }
    return base::Status::OK();
}
bool DateIRBuilder::GetDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                            ::llvm::Value** output) {
    return Load(block, date, 0, output);
}
bool DateIRBuilder::SetDate(::llvm::BasicBlock* block, ::llvm::Value* date,
                            ::llvm::Value* code) {
    return Set(block, date, 0, code);
}

// return dayOfYear
// *day = date & 0x0000000FF;
bool DateIRBuilder::Day(::llvm::BasicBlock* block, ::llvm::Value* date,
                        ::llvm::Value** output, base::Status& status) {
    ::llvm::Value* code;
    if (!GetDate(block, date, &code)) {
        LOG(WARNING) << "Fail to GetDate";
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    codegen::ArithmeticIRBuilder arithmetic_ir_builder(block);
    if (!arithmetic_ir_builder.BuildAnd(block, code, builder.getInt32(255),
                                        &code, status)) {
        LOG(WARNING) << "Fail Compute Day of Date: " << status.msg;
        return false;
    }
    *output = code;
    return true;
}
// Return Month
//    *day = date & 0x0000000FF;
//    date = date >> 8;
//    *month = 1 + (date & 0x0000FF);
//    *year = 1900 + (date >> 8);
bool DateIRBuilder::Month(::llvm::BasicBlock* block, ::llvm::Value* date,
                          ::llvm::Value** output, base::Status& status) {
    ::llvm::Value* code;
    if (!GetDate(block, date, &code)) {
        LOG(WARNING) << "Fail to GetDate";
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    codegen::ArithmeticIRBuilder arithmetic_ir_builder(block);

    if (!arithmetic_ir_builder.BuildLShiftRight(
            block, code, builder.getInt32(8), &code, status)) {
        LOG(WARNING) << "Fail Compute Month of Date: " << status.msg;
        return false;
    }

    if (!arithmetic_ir_builder.BuildAnd(block, code, builder.getInt32(255),
                                        &code, status)) {
        LOG(WARNING) << "Fail Compute Month of Date: " << status.msg;
        return false;
    }

    if (!arithmetic_ir_builder.BuildAddExpr(block, code, builder.getInt32(1),
                                            &code, status)) {
        LOG(WARNING) << "Fail Compute Month of Date: " << status.msg;
        return false;
    }
    *output = code;
    return true;
}
// Return Year
//    *year = 1900 + (date >> 16);
bool DateIRBuilder::Year(::llvm::BasicBlock* block, ::llvm::Value* date,
                         ::llvm::Value** output, base::Status& status) {
    ::llvm::Value* code;
    if (!GetDate(block, date, &code)) {
        LOG(WARNING) << "Fail to GetDate";
        return false;
    }

    ::llvm::IRBuilder<> builder(block);
    codegen::ArithmeticIRBuilder arithmetic_ir_builder(block);

    if (!arithmetic_ir_builder.BuildLShiftRight(
            block, code, builder.getInt32(16), &code, status)) {
        LOG(WARNING) << "Fail Compute Year of Date: " << status.msg;
        return false;
    }
    if (!arithmetic_ir_builder.BuildAddExpr(block, code, builder.getInt32(1900),
                                            &code, status)) {
        LOG(WARNING) << "Fail Compute Year of Date: " << status.msg;
        return false;
    }
    *output = code;
    return true;
}

}  // namespace codegen
}  // namespace hybridse
