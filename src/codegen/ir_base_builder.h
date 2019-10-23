/*
 * ir_base_builder.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#ifndef IR_BASE_BUILDER_H_
#define IR_BASE_BUILDER_H_

#include "llvm/IR/IRBuilder.h"
#include "glog/logging.h"


namespace fesql {
namespace codegen {


bool BuildLoadRelative(::llvm::IRBuilder<>& builder,
        ::llvm::LLVMContext& ctx,
        ::llvm::Value* ptr, 
        ::llvm::Value* offset,
        ::llvm::Type* type,
        ::llvm::Value** output) {

    if (!ptr->getType()->isPointerTy()) {
        LOG(WARNING) << "ptr should be pointer but " <<  ptr->getType()->getTypeID();
        return false;
    }

    if (!offset->getType()->isIntegerTy()) {
        LOG(WARNING) << "offset should be integer type but " << ptr->getType()->getTypeID();
        return false;
    }

    // cast ptr to int64
    ::llvm::Type* int64_ty = ::llvm::Type::getInt64Ty(ctx);
    ::llvm::Value* ptr_int64_ty = builder.CreatePtrToInt(ptr, int64_ty);
    // TODO no need cast if offset is int64 
    ::llvm::Value* offset_int64 = builder.CreateIntCast(offset, int64_ty, true, "cast_32_to_64");
    ::llvm::Value* ptr_add_offset = builder.CreateAdd(ptr_int64_ty, offset_int64, "ptr_add_offset");
    // todo check the type
    ::llvm::Value* int64_to_ty_ptr = builder.CreateIntToPtr(ptr_add_offset, type->getPointerTo());
    *output = builder.CreateLoad(type, int64_to_ty_ptr, "load_type_value");
    return true;
}

} // namespace of codegen
} // namespace of fesql
#endif /* !IR_BASE_BUILDER_H_ */
