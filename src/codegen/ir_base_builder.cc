/*
 * ir_base_builder.cc
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

#include "codegen/ir_base_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

bool GetLLVMType(::llvm::IRBuilder<>& builder,
        const ::fesql::type::Type& type,
        ::llvm::Type** output) {

    if (output == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }

    switch(type) {
        case ::fesql::type::kInt16: 
            {
                *output = builder.getInt16Ty();
                return true;
            }
        case ::fesql::type::kInt32:
            {
                *output = builder.getInt32Ty();
                return true;
            }
        case ::fesql::type::kInt64:
            {
                *output = builder.getInt64Ty();
                return true;
            }
        case ::fesql::type::kFloat:
            {
                *output = builder.getFloatTy();
                return true;
            }
        case ::fesql::type::kDouble:
            {
                *output = builder.getDoubleTy();
                return true;
            }
        case ::fesql::type::kVarchar:
            {
                ::llvm::StructType* type =
                ::llvm::StructType::create(builder.getContext(), "fe.string_ref");
                ::llvm::Type* size_ty = builder.getInt32Ty();
                ::llvm::Type* data_ptr_ty = builder.getInt8PtrTy();
                std::vector<::llvm::Type*> elements;
                elements.push_back(size_ty);
                elements.push_back(data_ptr_ty);
                type->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
                *output = type;
                return true;
            }
        default:
            {
                LOG(WARNING) << "not supported type " << ::fesql::type::Type_Name(type);
                return false;
            }
    }
}

bool BuildGetPtrOffset(::llvm::IRBuilder<>& builder,
        ::llvm::Value* ptr,
        ::llvm::Value* offset,
        ::llvm::Type* type,
        ::llvm::Value** outptr) {

    if (outptr == NULL) {
        LOG(WARNING) << "outptr is null";
        return false;
    
    }

    if (!ptr->getType()->isPointerTy()) {
        LOG(WARNING) << "ptr should be pointer but " <<  ptr->getType()->getTypeID();
        return false;
    }

    if (!offset->getType()->isIntegerTy()) {
        LOG(WARNING) << "offset should be integer type but " << ptr->getType()->getTypeID();
        return false;
    }

    // cast ptr to int64
    ::llvm::Type* int64_ty = builder.getInt64Ty();
    ::llvm::Value* ptr_int64_ty = builder.CreatePtrToInt(ptr, int64_ty);
    // TODO no need cast if offset is int64 
    ::llvm::Value* offset_int64 = builder.CreateIntCast(offset, int64_ty, true, "cast_32_to_64");
    ::llvm::Value* ptr_add_offset = builder.CreateAdd(ptr_int64_ty, offset_int64, "ptr_add_offset");
    // todo check the type
    *outptr = builder.CreateIntToPtr(ptr_add_offset, type->getPointerTo());
    return true;
}

bool GetTableType(::llvm::Type* type,
        ::fesql::type::Type* output) {

    if (type == NULL || output == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }

    switch(type->getTypeID()) {
        case ::llvm::Type::FloatTyID:
            {
                *output = ::fesql::type::kFloat;
                return true;
            }
        case ::llvm::Type::DoubleTyID: 
            {
                *output = ::fesql::type::kDouble;
                return true;
            }
        case ::llvm::Type::IntegerTyID:
            {
                switch (type->getIntegerBitWidth()) {
                    case 16: 
                        {
                            *output = ::fesql::type::kInt16;
                            return true;
                        }
                    case 32: 
                        {
                            *output = ::fesql::type::kInt32;
                            return true;
                        }
                    case 64: 
                        {
                            *output = ::fesql::type::kInt64;
                            return true;
                        }
                    default:
                        {
                            LOG(WARNING) << "no mapping type for llvm type";
                            return false;
                        }

                }
            }
        default: 
            {
             LOG(WARNING) << "no mapping type for llvm type";
             return false;
            }
    }
}

bool BuildLoadOffset(::llvm::IRBuilder<>& builder,
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
    ::llvm::Type* int64_ty = builder.getInt64Ty();
    ::llvm::Value* ptr_int64_ty = builder.CreatePtrToInt(ptr, int64_ty);
    // TODO no need cast if offset is int64 
    ::llvm::Value* offset_int64 = builder.CreateIntCast(offset, int64_ty, true, "cast_32_to_64");
    ::llvm::Value* ptr_add_offset = builder.CreateAdd(ptr_int64_ty, offset_int64, "ptr_add_offset");
    // todo check the type
    ::llvm::Value* int64_to_ty_ptr = builder.CreateIntToPtr(ptr_add_offset, type->getPointerTo());
    *output = builder.CreateLoad(type, int64_to_ty_ptr, "load_type_value");
    return true;
}

bool BuildStoreOffset(::llvm::IRBuilder<>& builder,
        ::llvm::Value* ptr,
        ::llvm::Value* offset,
        ::llvm::Value* value) {

    if (ptr == NULL || offset == NULL || value == NULL) {
        LOG(WARNING) << "ptr or offset or value is null";
        return false;
    }
    ::llvm::Value* ptr_with_offset = NULL;
    bool ok = BuildGetPtrOffset(builder, ptr, offset, value->getType(),
            &ptr_with_offset);
    if (!ok || ptr_with_offset == NULL) {
        LOG(WARNING) << "fail to get offset ptr";
        return false;
    }
    builder.CreateStore(value, ptr_with_offset, false);
    return true;
}


} // namespace codegen
} // namespace fesql



