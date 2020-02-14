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
#include <string>
#include <vector>
#include "glog/logging.h"
#include "storage/window.h"

namespace fesql {
namespace codegen {

bool GetLLVMType(::llvm::BasicBlock* block, const ::fesql::type::Type& type,
                 ::llvm::Type** output) {
    if (output == NULL || block == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    return GetLLVMType(block->getModule(), type, output);
}
bool GetLLVMType(::llvm::Module* m, const ::fesql::type::Type& type,
                 ::llvm::Type** output) {
    if (output == NULL || m == NULL) {
        LOG(WARNING) << "the output ptr or module  is NULL ";
        return false;
    }
    switch (type) {
        case ::fesql::type::kInt16: {
            *output = ::llvm::Type::getInt16Ty(m->getContext());
            return true;
        }
        case ::fesql::type::kInt32: {
            *output = ::llvm::Type::getInt32Ty(m->getContext());
            return true;
        }
        case ::fesql::type::kInt64: {
            *output = ::llvm::Type::getInt64Ty(m->getContext());
            return true;
        }
        case ::fesql::type::kFloat: {
            *output = ::llvm::Type::getFloatTy(m->getContext());
            return true;
        }
        case ::fesql::type::kDouble: {
            *output = ::llvm::Type::getDoubleTy(m->getContext());
            return true;
        }
        case ::fesql::type::kBool: {
            *output = ::llvm::Type::getInt1Ty(m->getContext());
            return true;
        }
        case ::fesql::type::kVarchar: {
            std::string name = "fe.string_ref";
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != NULL) {
                *output = stype;
                return true;
            }
            stype = ::llvm::StructType::create(m->getContext(), name);
            ::llvm::Type* size_ty = ::llvm::Type::getInt32Ty(m->getContext());
            ::llvm::Type* data_ptr_ty =
                ::llvm::Type::getInt8PtrTy(m->getContext());
            std::vector<::llvm::Type*> elements;
            elements.push_back(size_ty);
            elements.push_back(data_ptr_ty);
            stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
            *output = stype;
            return true;
        }
        case ::fesql::type::kList: {
            std::string name = "fe.list_ref";
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != NULL) {
                *output = stype;
                return true;
            }
            stype = ::llvm::StructType::create(m->getContext(), name);
            ::llvm::Type* data_ptr_ty =
                ::llvm::Type::getInt8PtrTy(m->getContext());
            std::vector<::llvm::Type*> elements;
            elements.push_back(data_ptr_ty);
            stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
            *output = stype;
            return true;
        }
        default: {
            LOG(WARNING) << "not supported type "
                         << ::fesql::type::Type_Name(type);
            return false;
        }
    }
}

bool GetLLVMColumnSize(const ::fesql::type::Type& v_type, uint32_t* size) {
    if (nullptr == size) {
        LOG(WARNING) << "the size ptr is NULL ";
        return false;
    }

    switch (v_type) {
        case ::fesql::type::kInt16: {
            *size = sizeof(::fesql::storage::ColumnImpl<int16_t>);
            break;
        }
        case ::fesql::type::kInt32: {
            *size = sizeof(::fesql::storage::ColumnImpl<int32_t>);
            break;
        }
        case ::fesql::type::kInt64: {
            *size = sizeof(::fesql::storage::ColumnImpl<int64_t>);
            break;
        }
        case ::fesql::type::kDouble: {
            *size = sizeof(::fesql::storage::ColumnImpl<double>);
            break;
        }
        case ::fesql::type::kFloat: {
            *size = sizeof(::fesql::storage::ColumnImpl<float>);
            break;
        }
        case ::fesql::type::kVarchar: {
            *size = sizeof(::fesql::storage::StringColumnImpl);
            break;
        }
        default: {
            LOG(WARNING) << "not supported type "
                         << ::fesql::type::Type_Name(v_type);
            return false;
        }
    }
    return true;
}
bool GetLLVMColumnIteratorSize(const ::fesql::type::Type& v_type,
                               uint32_t* size) {
    if (nullptr == size) {
        LOG(WARNING) << "the size ptr is NULL ";
        return false;
    }

    switch (v_type) {
        case ::fesql::type::kInt16: {
            *size = sizeof(::fesql::storage::IteratorImpl<int16_t>);
            break;
        }
        case ::fesql::type::kInt32: {
            *size = sizeof(::fesql::storage::IteratorImpl<int32_t>);
            break;
        }
        case ::fesql::type::kInt64: {
            *size = sizeof(::fesql::storage::IteratorImpl<int64_t>);
            break;
        }
        case ::fesql::type::kDouble: {
            *size = sizeof(::fesql::storage::IteratorImpl<double>);
            break;
        }
        case ::fesql::type::kFloat: {
            *size = sizeof(::fesql::storage::IteratorImpl<float>);
            break;
        }
        case ::fesql::type::kVarchar: {
            *size = sizeof(
                ::fesql::storage::IteratorImpl<fesql::storage::StringRef>);
            break;
        }
        default: {
            LOG(WARNING) << "not supported type "
                         << ::fesql::type::Type_Name(v_type);
            return false;
        }
    }
    return true;
}
bool GetLLVMListType(::llvm::Module* m,  // NOLINT
                     const ::fesql::type::Type& v_type, ::llvm::Type** output) {
    if (output == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    std::string name;
    switch (v_type) {
        case ::fesql::type::kInt16: {
            name = "fe.list_int16_ref";
            break;
        }
        case ::fesql::type::kInt32: {
            name = "fe.list_int32_ref";
            break;
        }
        case ::fesql::type::kInt64: {
            name = "fe.list_int64_ref";
            break;
        }
        case ::fesql::type::kFloat: {
            name = "fe.list_float_ref";
            break;
        }
        case ::fesql::type::kDouble: {
            name = "fe.list_double_ref";
            break;
        }
        case ::fesql::type::kVarchar: {
            name = "fe.list_string_ref";
            break;
        }
        default: {
            LOG(WARNING) << "not supported list<type> when type is  "
                         << ::fesql::type::Type_Name(v_type);
            return false;
        }
    }
    ::llvm::StringRef sr(name);
    ::llvm::StructType* stype = m->getTypeByName(sr);
    if (stype != NULL) {
        *output = stype;
        return true;
    }
    stype = ::llvm::StructType::create(m->getContext(), name);
    ::llvm::Type* data_ptr_ty =
        ::llvm::IntegerType::getInt8PtrTy(m->getContext());
    std::vector<::llvm::Type*> elements;
    elements.push_back(data_ptr_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    *output = stype;
    return true;
}

bool GetConstFeString(const std::string& val, ::llvm::BasicBlock* block,
                      ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    ::llvm::Type* str_type = NULL;
    bool ok = GetLLVMType(block, ::fesql::type::kVarchar, &str_type);
    if (!ok) return false;
    ::llvm::IRBuilder<> builder(block);
    ::llvm::Value* string_ref = builder.CreateAlloca(str_type);
    ::llvm::Value* data_ptr_ptr =
        builder.CreateStructGEP(str_type, string_ref, 1);
    ::llvm::StringRef val_ref(val);
    ::llvm::Value* str_val = builder.CreateGlobalStringPtr(val_ref);
    ::llvm::Value* cast_data_ptr_ptr = builder.CreatePointerCast(
        data_ptr_ptr, str_val->getType()->getPointerTo());
    builder.CreateStore(str_val, cast_data_ptr_ptr, false);

    ::llvm::Value* size = builder.getInt32(val.size());
    ::llvm::Value* size_ptr = builder.CreateStructGEP(str_type, string_ref, 0);
    ::llvm::Value* cast_type_size_ptr =
        builder.CreatePointerCast(size_ptr, size->getType()->getPointerTo());
    builder.CreateStore(size, cast_type_size_ptr, false);
    *output = string_ref;
    return true;
}

bool BuildGetPtrOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                       ::llvm::Value* ptr, ::llvm::Value* offset,
                       ::llvm::Type* type, ::llvm::Value** outptr) {
    if (outptr == NULL) {
        LOG(WARNING) << "outptr is null";
        return false;
    }

    if (!ptr->getType()->isPointerTy()) {
        LOG(WARNING) << "ptr should be pointer but "
                     << ptr->getType()->getTypeID();
        return false;
    }

    if (!offset->getType()->isIntegerTy()) {
        LOG(WARNING) << "offset should be integer type but "
                     << ptr->getType()->getTypeID();
        return false;
    }

    // cast ptr to int64
    ::llvm::Type* int64_ty = builder.getInt64Ty();
    ::llvm::Value* ptr_int64_ty = builder.CreatePtrToInt(ptr, int64_ty);
    // TODO(wangtaize) no need cast if offset is int64
    ::llvm::Value* offset_int64 =
        builder.CreateIntCast(offset, int64_ty, true, "cast_32_to_64");
    ::llvm::Value* ptr_add_offset =
        builder.CreateAdd(ptr_int64_ty, offset_int64, "ptr_add_offset");
    // todo check the type
    *outptr = builder.CreateIntToPtr(ptr_add_offset, type);
    return true;
}

bool GetFullType(::llvm::Type* type, ::fesql::type::Type* base,
                 ::fesql::type::Type* v1_type, ::fesql::type::Type* v2_type) {
    if (type == NULL || base == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }
    switch (type->getTypeID()) {
        case ::llvm::Type::FloatTyID: {
            *base = ::fesql::type::kFloat;
            return true;
        }
        case ::llvm::Type::DoubleTyID: {
            *base = ::fesql::type::kDouble;
            return true;
        }
        case ::llvm::Type::IntegerTyID: {
            switch (type->getIntegerBitWidth()) {
                case 16: {
                    *base = ::fesql::type::kInt16;
                    return true;
                }
                case 32: {
                    *base = ::fesql::type::kInt32;
                    return true;
                }
                case 64: {
                    *base = ::fesql::type::kInt64;
                    return true;
                }
                default: {
                    LOG(WARNING) << "no mapping type for llvm type";
                    return false;
                }
            }
        }
        case ::llvm::Type::StructTyID:
        case ::llvm::Type::PointerTyID: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }
            if (type->getStructName().equals("fe.list_int16_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kInt16;
                return true;
            } else if (type->getStructName().equals("fe.list_int32_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kInt32;
                return true;
            } else if (type->getStructName().equals("fe.list_int64_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kInt64;
                return true;
            } else if (type->getStructName().equals("fe.list_float_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kFloat;
                return true;
            } else if (type->getStructName().equals("fe.list_double_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kDouble;
                return true;
            } else if (type->getStructName().equals("fe.list_string_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kVarchar;
                return true;
            } else if (type->getStructName().equals("fe.string_ref")) {
                *base = ::fesql::type::kVarchar;
                return true;
            }
            // TODO(chenjing): add map type
            LOG(WARNING) << "no mapping type for llvm type "
                         << type->getStructName().str();
            return false;
        }
        default: {
            LOG(WARNING) << "no mapping type for llvm type";
            return false;
        }
    }
}
bool GetTableType(::llvm::Type* type, ::fesql::type::Type* output) {
    if (type == NULL || output == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }

    switch (type->getTypeID()) {
        case ::llvm::Type::FloatTyID: {
            *output = ::fesql::type::kFloat;
            return true;
        }
        case ::llvm::Type::DoubleTyID: {
            *output = ::fesql::type::kDouble;
            return true;
        }
        case ::llvm::Type::IntegerTyID: {
            switch (type->getIntegerBitWidth()) {
                case 16: {
                    *output = ::fesql::type::kInt16;
                    return true;
                }
                case 32: {
                    *output = ::fesql::type::kInt32;
                    return true;
                }
                case 64: {
                    *output = ::fesql::type::kInt64;
                    return true;
                }
                case 1: {
                    *output = ::fesql::type::kBool;
                    return true;
                }
                default: {
                    LOG(WARNING) << "no mapping type for llvm type";
                    return false;
                }
            }
        }
        case ::llvm::Type::StructTyID:
        case ::llvm::Type::PointerTyID: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }
            if (type->getStructName().startswith_lower("fe.list_")) {
                *output = fesql::type::kList;
                return true;
            } else if (type->getStructName().equals("fe.string_ref")) {
                *output = ::fesql::type::kVarchar;
                return true;
            }
            LOG(WARNING) << "no mapping type for llvm type "
                         << type->getStructName().str();
            return false;
        }
        default: {
            LOG(WARNING) << "no mapping type for llvm type";
            return false;
        }
    }
}

bool BuildLoadOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                     ::llvm::Value* ptr, ::llvm::Value* offset,
                     ::llvm::Type* type, ::llvm::Value** output) {
    if (!ptr->getType()->isPointerTy()) {
        LOG(WARNING) << "ptr should be pointer but "
                     << ptr->getType()->getTypeID();
        return false;
    }

    if (!offset->getType()->isIntegerTy()) {
        LOG(WARNING) << "offset should be integer type but "
                     << ptr->getType()->getTypeID();
        return false;
    }

    // cast ptr to int64
    ::llvm::Type* int64_ty = builder.getInt64Ty();
    ::llvm::Value* ptr_int64_ty = builder.CreatePtrToInt(ptr, int64_ty);
    // TODO(wangtaize) no need cast if offset is int64
    ::llvm::Value* offset_int64 =
        builder.CreateIntCast(offset, int64_ty, true, "cast_32_to_64");
    ::llvm::Value* ptr_add_offset =
        builder.CreateAdd(ptr_int64_ty, offset_int64, "ptr_add_offset");
    // todo check the type
    ::llvm::Value* int64_to_ty_ptr =
        builder.CreateIntToPtr(ptr_add_offset, type->getPointerTo());
    *output = builder.CreateLoad(type, int64_to_ty_ptr, "load_type_value");
    return true;
}

bool BuildStoreOffset(::llvm::IRBuilder<>& builder,  // NOLINT
                      ::llvm::Value* ptr, ::llvm::Value* offset,
                      ::llvm::Value* value) {
    if (ptr == NULL || offset == NULL || value == NULL) {
        LOG(WARNING) << "ptr or offset or value is null";
        return false;
    }
    // TODO(wangtaize) check ptr type match value type
    ::llvm::Value* ptr_with_offset = NULL;
    bool ok =
        BuildGetPtrOffset(builder, ptr, offset,
                          value->getType()->getPointerTo(), &ptr_with_offset);
    if (!ok || ptr_with_offset == NULL) {
        LOG(WARNING) << "fail to get offset ptr";
        return false;
    }
    builder.CreateStore(value, ptr_with_offset, false);
    return true;
}

}  // namespace codegen
}  // namespace fesql
