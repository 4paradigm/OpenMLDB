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

namespace fesql {
namespace codegen {

bool GetLLVMType(::llvm::BasicBlock* block, const ::fesql::type::Type& type,
                 ::llvm::Type** output) {
    if (output == NULL || block == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    switch (type) {
        case ::fesql::type::kInt16: {
            *output = builder.getInt16Ty();
            return true;
        }
        case ::fesql::type::kInt32: {
            *output = builder.getInt32Ty();
            return true;
        }
        case ::fesql::type::kInt64: {
            *output = builder.getInt64Ty();
            return true;
        }
        case ::fesql::type::kFloat: {
            *output = builder.getFloatTy();
            return true;
        }
        case ::fesql::type::kDouble: {
            *output = builder.getDoubleTy();
            return true;
        }
        case ::fesql::type::kVarchar: {
            ::llvm::Module* m = block->getModule();
            std::string name = "fe.string_ref";
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != NULL) {
                *output = stype;
                return true;
            }
            stype = ::llvm::StructType::create(builder.getContext(), name);
            ::llvm::Type* size_ty = builder.getInt32Ty();
            ::llvm::Type* data_ptr_ty = builder.getInt8PtrTy();
            std::vector<::llvm::Type*> elements;
            elements.push_back(size_ty);
            elements.push_back(data_ptr_ty);
            stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
            *output = stype;
            return true;
        }
        case ::fesql::type::kList: {
            ::llvm::Module* m = block->getModule();
            std::string name = "fe.list_ref";
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != NULL) {
                *output = stype;
                return true;
            }
            stype = ::llvm::StructType::create(builder.getContext(), name);
            ::llvm::Type* data_ptr_ty = builder.getInt8PtrTy();
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
bool GetLLVMListType(::llvm::LLVMContext& ctx,
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

    ::llvm::StructType* stype = ::llvm::StructType::create(ctx, name);
    ::llvm::Type* data_ptr_ty = ::llvm::IntegerType::getInt8PtrTy(ctx);
    std::vector<::llvm::Type*> elements;
    elements.push_back(data_ptr_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    *output = stype;
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
        case ::llvm::Type::StructTyID: {
            if (type->getStructName().startswith_lower("fe.list_int16_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kInt16;
                return true;
            } else if (type->getStructName().startswith_lower("fe.list_int32_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kInt32;
                return true;
            } else if (type->getStructName().startswith_lower("fe.list_int64_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kInt64;
                return true;
            } else if (type->getStructName().startswith_lower("fe.list_float_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kFloat;
                return true;
            } else if (type->getStructName().startswith_lower("fe.list_double_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kDouble;
                return true;
            } else if (type->getStructName().startswith_lower("fe.list_string_ref")) {
                *base = fesql::type::kList;
                *v1_type = fesql::type::kVarchar;
                return true;
            } else if (type->getStructName().startswith_lower("fe.string_ref")) {
                *base = ::fesql::type::kVarchar;
                return true;
            }
            // TODO(chenjing): add map type
            LOG(WARNING) << "no mapping type for llvm type "
                         << type->getStructName().str();
            return false;
        }
        case ::llvm::Type::PointerTyID: {
            //TODO(wtx): why is pinter is string type
            *base = ::fesql::type::kVarchar;
            return true;
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
                default: {
                    LOG(WARNING) << "no mapping type for llvm type";
                    return false;
                }
            }
        }
        case ::llvm::Type::StructTyID: {
            if (type->getStructName().equals("fe.list_int16_ref")) {
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
        case ::llvm::Type::PointerTyID: {
            *output = ::fesql::type::kVarchar;
            return true;
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

}  // namespace fesql
}  // namespace fesql
