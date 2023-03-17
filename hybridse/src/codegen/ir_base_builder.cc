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

#include "codegen/ir_base_builder.h"

#include <string>
#include <tuple>
#include <utility>
#include <vector>

#include "codec/list_iterator_codec.h"
#include "codegen/array_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "glog/logging.h"
#include "node/node_manager.h"

namespace hybridse {
namespace codegen {

using base::Status;
using ::hybridse::common::kCodegenError;

bool GetLlvmType(::llvm::BasicBlock* block,
                 const ::hybridse::node::DataType& type,  // NOLINT
                 ::llvm::Type** output) {
    if (output == NULL || block == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    return GetLlvmType(block->getModule(), type, output);
}

bool GetLlvmType(::llvm::BasicBlock* block,
                 const ::hybridse::node::TypeNode* type,  // NOLINT
                 ::llvm::Type** output) {
    if (output == NULL || block == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    return GetLlvmType(block->getModule(), type, output);
}
bool GetLlvmType(::llvm::Module* m, const ::hybridse::node::DataType& type,
                 ::llvm::Type** llvm_type) {
    if (nullptr == m) {
        LOG(WARNING) << "fail to convert data type to llvm type";
        return false;
    }
    switch (type) {
        case node::kVoid:
            *llvm_type = (::llvm::Type::getVoidTy(m->getContext()));
            break;
        case node::kBool:
            *llvm_type = (::llvm::Type::getInt1Ty(m->getContext()));
            break;
        case node::kInt16:
            *llvm_type = (::llvm::Type::getInt16Ty(m->getContext()));
            break;
        case node::kInt32:
            *llvm_type = (::llvm::Type::getInt32Ty(m->getContext()));
            break;
        case node::kInt64:
            *llvm_type = (::llvm::Type::getInt64Ty(m->getContext()));
            break;
        case node::kFloat:
            *llvm_type = (::llvm::Type::getFloatTy(m->getContext()));
            break;
        case node::kDouble:
            *llvm_type = (::llvm::Type::getDoubleTy(m->getContext()));
            break;
        case node::kInt8Ptr:
            *llvm_type = (::llvm::Type::getInt8PtrTy(m->getContext()));
            break;
        case node::kVarchar: {
            StringIRBuilder string_ir_builder(m);
            *llvm_type = string_ir_builder.GetType()->getPointerTo();
            break;
        }
        case node::kTimestamp: {
            TimestampIRBuilder timestamp_ir_builder(m);
            *llvm_type = timestamp_ir_builder.GetType()->getPointerTo();
            return true;
        }
        case node::kDate: {
            DateIRBuilder date_ir_builder(m);
            *llvm_type = date_ir_builder.GetType()->getPointerTo();

            return true;
        }
        case node::kList:
        case node::kMap:
        case node::kIterator: {
            LOG(WARNING) << "fail to convert type" << node::DataTypeName(type)
                         << "without generic types";
            return false;
        }
        case node::kOpaque:
        case node::kRow: {
            *llvm_type = ::llvm::Type::getInt8PtrTy(m->getContext());
            return true;
        }
        default: {
            LOG(WARNING) << "fail to convert hybridse datatype to llvm type: ";
            return false;
        }
    }
    return true;
}

bool GetLlvmColumnSize(::hybridse::node::TypeNode* v_type, uint32_t* size) {
    if (nullptr == size) {
        LOG(WARNING) << "the size ptr is NULL ";
        return false;
    }

    switch (v_type->base_) {
        case ::hybridse::node::kInt16: {
            *size = sizeof(::hybridse::codec::ColumnImpl<int16_t>);
            break;
        }
        case ::hybridse::node::kInt32: {
            *size = sizeof(::hybridse::codec::ColumnImpl<int32_t>);
            break;
        }
        case ::hybridse::node::kInt64: {
            *size = sizeof(::hybridse::codec::ColumnImpl<int64_t>);
            break;
        }
        case ::hybridse::node::kDouble: {
            *size = sizeof(::hybridse::codec::ColumnImpl<double>);
            break;
        }
        case ::hybridse::node::kFloat: {
            *size = sizeof(::hybridse::codec::ColumnImpl<float>);
            break;
        }
        case ::hybridse::node::kVarchar: {
            *size = sizeof(::hybridse::codec::StringColumnImpl);
            break;
        }
        case ::hybridse::node::kTimestamp: {
            *size = sizeof(::hybridse::codec::ColumnImpl<openmldb::base::Timestamp>);
            break;
        }
        case ::hybridse::node::kDate: {
            *size = sizeof(::hybridse::codec::ColumnImpl<openmldb::base::Date>);
            break;
        }
        case ::hybridse::node::kBool: {
            *size = sizeof(::hybridse::codec::ColumnImpl<bool>);
            break;
        }
        default: {
            LOG(WARNING) << "not supported type " << v_type->GetName();
            return false;
        }
    }
    return true;
}  // namespace codegen

bool GetLlvmListType(::llvm::Module* m,
                     const ::hybridse::node::TypeNode* v_type,
                     ::llvm::Type** output) {
    if (output == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    std::string name;
    switch (v_type->base_) {
        case ::hybridse::node::kInt16: {
            name = "fe.list_ref_int16";
            break;
        }
        case ::hybridse::node::kInt32: {
            name = "fe.list_ref_int32";
            break;
        }
        case ::hybridse::node::kInt64: {
            name = "fe.list_ref_int64";
            break;
        }
        case ::hybridse::node::kBool: {
            name = "fe.list_ref_bool";
            break;
        };
        case ::hybridse::node::kTimestamp: {
            name = "fe.list_ref_timestamp";
            break;
        }
        case ::hybridse::node::kDate: {
            name = "fe.list_ref_date";
            break;
        }
        case ::hybridse::node::kFloat: {
            name = "fe.list_ref_float";
            break;
        }
        case ::hybridse::node::kDouble: {
            name = "fe.list_ref_double";
            break;
        }
        case ::hybridse::node::kVarchar: {
            name = "fe.list_ref_string";
            break;
        }
        case ::hybridse::node::kRow: {
            name = "fe.list_ref_row";
            break;
        }
        default: {
            LOG(WARNING) << "not supported list<type> when type is  "
                         << v_type->GetName();
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

bool GetLlvmIteratorType(::llvm::Module* m,
                         const ::hybridse::node::TypeNode* v_type,
                         ::llvm::Type** output) {
    if (output == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    std::string name;
    switch (v_type->base_) {
        case ::hybridse::node::kInt16: {
            name = "fe.iterator_ref_int16";
            break;
        }
        case ::hybridse::node::kInt32: {
            name = "fe.iterator_ref_int32";
            break;
        }
        case ::hybridse::node::kInt64: {
            name = "fe.iterator_ref_int64";
            break;
        }
        case ::hybridse::node::kBool: {
            name = "fe.iterator_ref_bool";
            break;
        }
        case ::hybridse::node::kFloat: {
            name = "fe.iterator_ref_float";
            break;
        }
        case ::hybridse::node::kDouble: {
            name = "fe.iterator_ref_double";
            break;
        }
        case ::hybridse::node::kVarchar: {
            name = "fe.iterator_ref_string";
            break;
        }
        case ::hybridse::node::kTimestamp: {
            name = "fe.iterator_ref_timestamp";
            break;
        }
        case ::hybridse::node::kDate: {
            name = "fe.iterator_ref_date";
            break;
        }
        case ::hybridse::node::kRow: {
            name = "fe.iterator_ref_row";
            break;
        }
        default: {
            LOG(WARNING) << "not supported list<type> when type is  "
                         << v_type->GetName();
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

bool GetLlvmType(::llvm::Module* m, const hybridse::node::TypeNode* data_type,
                 ::llvm::Type** llvm_type) {
    if (nullptr == data_type) {
        LOG(WARNING) << "fail to convert data type to llvm type";
        return false;
    }
    switch (data_type->base_) {
        case hybridse::node::kList: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING) << "fail to convert data type: list generic types "
                                "number is "
                             << data_type->generics_.size();
                return false;
            }
            ::llvm::Type* list_type = nullptr;
            if (false ==
                GetLlvmListType(m, data_type->generics_[0], &list_type)) {
                return false;
            }
            *llvm_type = list_type->getPointerTo();
            return true;
        }
        case hybridse::node::kIterator: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING)
                    << "fail to convert data type: iterator generic types "
                       "number is "
                    << data_type->generics_.size();
                return false;
            }
            ::llvm::Type* list_type = nullptr;
            if (false ==
                GetLlvmIteratorType(m, data_type->generics_[0], &list_type)) {
                return false;
            }
            *llvm_type = list_type->getPointerTo();
            return true;
        }
        case hybridse::node::kMap: {
            LOG(WARNING) << "fail to codegen map type, currently not support";
            break;
        }
        case hybridse::node::kArray: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING) << "array type with element type size != 1";
                return false;
            }
            ::llvm::Type* ele_type = nullptr;
            if (false == GetLlvmType(m, data_type->GetGenericType(0), &ele_type)) {
                LOG(WARNING) << "failed to infer llvm type for array element";
                return false;
            }

            ArrayIRBuilder array_builder(m, ele_type);
            *llvm_type = array_builder.GetType()->getPointerTo();

            return true;
        }
        case hybridse::node::kTuple: {
            std::string name = absl::StrCat("fe.", data_type->GetName());
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != nullptr) {
                *llvm_type = stype;
                return true;
            }
            stype = ::llvm::StructType::create(m->getContext(), name);

            std::vector<::llvm::Type*> fields;
            for (auto field : data_type->generics()) {
                ::llvm::Type* tp = nullptr;
                if (!GetLlvmType(m, field, &tp)) {
                    return false;
                }
                fields.push_back(tp);
            }
            stype->setBody(fields);
            *llvm_type = stype;
            return true;
        }
        default:
            break;
    }
    return GetLlvmType(m, data_type->base_, llvm_type);
}

bool GetConstFeString(const std::string& val, ::llvm::BasicBlock* block,
                      ::llvm::Value** output) {
    if (block == NULL || output == NULL) {
        LOG(WARNING) << "the output ptr or block is NULL ";
        return false;
    }
    StringIRBuilder string_ir_builder(block->getModule());
    return string_ir_builder.NewString(block, val, output);
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

bool GetFullType(node::NodeManager* nm, ::llvm::Type* type,
                 const ::hybridse::node::TypeNode** type_node) {
    if (nm == NULL || type == NULL || type_node == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }
    node::DataType base;
    if (false == GetBaseType(type, &base)) {
        LOG(WARNING) << "Fail to get base type of "
                     << GetLlvmObjectString(type);
        return false;
    }

    switch (base) {
        case hybridse::node::kList: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }
            if (type->getStructName().equals("fe.list_ref_int16")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kInt16));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_int32")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kInt32));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_int64")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kInt64));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_bool")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kBool));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_float")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kFloat));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_double")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kDouble));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_string")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kVarchar));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_timestamp")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kTimestamp));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_date")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kDate));
                return true;
            }
            LOG(WARNING) << "fail to get type of llvm type for "
                         << type->getStructName().str();
            return false;
        }
        case hybridse::node::kIterator: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }
            if (type->getStructName().equals("fe.iterator_ref_int16")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kInt16));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_bool")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kBool));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_int32")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kInt32));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_int64")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kInt64));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_float")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kFloat));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_double")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kDouble));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_string")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kVarchar));
                return true;
            } else if (type->getStructName().equals(
                           "fe.iterator_ref_timestamp")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kTimestamp));
                return true;
            } else if (type->getStructName().equals("fe.iterator_ref_date")) {
                *type_node = nm->MakeTypeNode(
                    base, nm->MakeTypeNode(hybridse::node::kDate));
                return true;
            }
            LOG(WARNING) << "fail to get type of llvm type for "
                         << type->getStructName().str();
            return false;
        }
        case hybridse::node::kMap: {
            LOG(WARNING) << "fail to get type for map";
            return false;
        }
        default: {
            *type_node = nm->MakeTypeNode(base);
            return true;
        }
    }
}

bool IsStringType(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kVarchar;
}
bool GetBaseType(::llvm::Type* type, ::hybridse::node::DataType* output) {
    if (type == NULL || output == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }
    switch (type->getTypeID()) {
        case ::llvm::Type::TokenTyID: {
            *output = ::hybridse::node::kNull;
            return true;
        }
        case ::llvm::Type::FloatTyID: {
            *output = ::hybridse::node::kFloat;
            return true;
        }
        case ::llvm::Type::DoubleTyID: {
            *output = ::hybridse::node::kDouble;
            return true;
        }
        case ::llvm::Type::IntegerTyID: {
            switch (type->getIntegerBitWidth()) {
                case 1: {
                    *output = ::hybridse::node::kBool;
                    return true;
                }
                case 16: {
                    *output = ::hybridse::node::kInt16;
                    return true;
                }
                case 32: {
                    *output = ::hybridse::node::kInt32;
                    return true;
                }
                case 64: {
                    *output = ::hybridse::node::kInt64;
                    return true;
                }
                default: {
                    LOG(WARNING) << "no mapping type for llvm type";
                    return false;
                }
            }
        }
        case ::llvm::Type::PointerTyID: {
            auto pointee_ty = reinterpret_cast<::llvm::PointerType*>(type)
                ->getElementType();

            if (::llvm::Type::StructTyID != pointee_ty->getTypeID()) {
                LOG(WARNING) << "no mapping pointee_ty for llvm pointee_ty";
                return false;
            }

            if (pointee_ty->getStructName().startswith("fe.list_ref_")) {
                *output = hybridse::node::kList;
                return true;
            } else if (pointee_ty->getStructName().startswith("fe.iterator_ref_")) {
                *output = hybridse::node::kIterator;
                return true;
            } else if (pointee_ty->getStructName().equals("fe.string_ref")) {
                *output = hybridse::node::kVarchar;
                return true;
            } else if (pointee_ty->getStructName().equals("fe.timestamp")) {
                *output = hybridse::node::kTimestamp;
                return true;
            } else if (pointee_ty->getStructName().equals("fe.date")) {
                *output = hybridse::node::kDate;
                return true;
            }
            LOG(WARNING) << "no mapping pointee_ty for llvm pointee_ty "
                         << pointee_ty->getStructName().str();
            return false;
        }
        default: {
            LOG(WARNING) << "no mapping type for llvm type: "
                         << GetLlvmObjectString(type);
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
bool DataType2SchemaType(const ::hybridse::node::TypeNode& type,
                         ::hybridse::type::Type* output) {
    switch (type.base_) {
        case ::hybridse::node::kBool: {
            *output = ::hybridse::type::kBool;
            break;
        }
        case ::hybridse::node::kInt16: {
            *output = ::hybridse::type::kInt16;
            break;
        }
        case ::hybridse::node::kInt32: {
            *output = ::hybridse::type::kInt32;
            break;
        }
        case ::hybridse::node::kInt64: {
            *output = ::hybridse::type::kInt64;
            break;
        }
        case ::hybridse::node::kFloat: {
            *output = ::hybridse::type::kFloat;
            break;
        }
        case ::hybridse::node::kDouble: {
            *output = ::hybridse::type::kDouble;
            break;
        }
        case ::hybridse::node::kVarchar: {
            *output = ::hybridse::type::kVarchar;
            break;
        }
        case ::hybridse::node::kTimestamp: {
            *output = ::hybridse::type::kTimestamp;
            break;
        }
        case ::hybridse::node::kDate: {
            *output = ::hybridse::type::kDate;
            break;
        }
        case ::hybridse::node::kNull: {
            // Encode的时候, kNull 就是 kBool
            *output = ::hybridse::type::kBool;
            break;
        }
        default: {
            LOG(WARNING) << "can't convert to schema for type: "
                         << type.GetName();
            return false;
        }
    }
    return true;
}
bool SchemaType2DataType(const ::hybridse::type::Type type,
                         ::hybridse::node::TypeNode* output) {
    if (nullptr == output) {
        LOG(WARNING) << "Fail convert type: input is null";
        return false;
    }
    return SchemaType2DataType(type, &output->base_);
}
bool SchemaType2DataType(const ::hybridse::type::Type type,
                         ::hybridse::node::DataType* output) {
    if (nullptr == output) {
        LOG(WARNING) << "Fail convert type: input is null";
        return false;
    }
    switch (type) {
        case ::hybridse::type::kBool: {
            *output = ::hybridse::node::kBool;
            break;
        }
        case ::hybridse::type::kInt16: {
            *output = ::hybridse::node::kInt16;
            break;
        }
        case ::hybridse::type::kInt32: {
            *output = ::hybridse::node::kInt32;
            break;
        }
        case ::hybridse::type::kInt64: {
            *output = ::hybridse::node::kInt64;
            break;
        }
        case ::hybridse::type::kFloat: {
            *output = ::hybridse::node::kFloat;
            break;
        }
        case ::hybridse::type::kDouble: {
            *output = ::hybridse::node::kDouble;
            break;
        }
        case ::hybridse::type::kVarchar: {
            *output = ::hybridse::node::kVarchar;
            break;
        }
        case ::hybridse::type::kTimestamp: {
            *output = ::hybridse::node::kTimestamp;
            break;
        }
        case ::hybridse::type::kDate: {
            *output = ::hybridse::node::kDate;
            break;
        }
        default: {
            LOG(WARNING) << "unrecognized schema type "
                         << ::hybridse::type::Type_Name(type);
            return false;
        }
    }
    return true;
}
bool TypeIRBuilder::IsTimestampPtr(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!IsStructPtr(type)) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kTimestamp;
}
bool TypeIRBuilder::IsInt64(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kInt64;
}
bool TypeIRBuilder::IsBool(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kBool;
}

bool TypeIRBuilder::IsNull(::llvm::Type* type) { return type->isTokenTy(); }
bool TypeIRBuilder::IsInterger(::llvm::Type* type) {
    return type->isIntegerTy();
}
bool TypeIRBuilder::IsNumber(::llvm::Type* type) {
    return type->isIntegerTy() || type->isFloatingPointTy();
}
bool TypeIRBuilder::isFloatPoint(::llvm::Type* type) {
    return type->isFloatingPointTy();
}
const std::string TypeIRBuilder::TypeName(::llvm::Type* type) {
    node::NodeManager tmp_node_manager;
    const node::TypeNode* type_node = nullptr;
    if (!GetFullType(&tmp_node_manager, type, &type_node)) {
        return "unknow";
    }
    return type_node->GetName();
}

bool TypeIRBuilder::IsDatePtr(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!IsStructPtr(type)) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kDate;
}
bool TypeIRBuilder::IsStringPtr(::llvm::Type* type) {
    ::hybridse::node::DataType data_type;
    if (!type->isPointerTy()) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kVarchar;
}
bool TypeIRBuilder::IsStructPtr(::llvm::Type* type) {
    if (type->getTypeID() == ::llvm::Type::PointerTyID) {
        type = reinterpret_cast<::llvm::PointerType*>(type)->getElementType();
        if (type->isStructTy()) {
            DLOG(INFO) << "Struct Name " << type->getStructName().str();
            return true;
        } else {
            DLOG(INFO) << "Isn't Struct Type";
            return false;
        }
    }
    return false;
}
base::Status TypeIRBuilder::UnaryOpTypeInfer(
    const std::function<base::Status(node::NodeManager*, const node::TypeNode*, const node::TypeNode**)> func,
    ::llvm::Type* lhs) {
    node::NodeManager tmp_node_manager;
    const node::TypeNode* left_type = nullptr;
    CHECK_TRUE(GetFullType(&tmp_node_manager, lhs, &left_type), common::kTypeError, "invalid op type")
    const node::TypeNode* output_type;
    CHECK_STATUS(func(&tmp_node_manager, left_type, &output_type))
    return Status::OK();
}
base::Status TypeIRBuilder::BinaryOpTypeInfer(
    const std::function<base::Status(node::NodeManager*, const node::TypeNode*, const node::TypeNode*,
                                     const node::TypeNode**)>
        func,
    ::llvm::Type* lhs, ::llvm::Type* rhs) {
    const node::TypeNode* left_type = nullptr;
    const node::TypeNode* right_type = nullptr;
    node::NodeManager tmp_node_manager;
    CHECK_TRUE(GetFullType(&tmp_node_manager, lhs, &left_type), common::kTypeError,
               "invalid op type: ", GetLlvmObjectString(lhs))
    CHECK_TRUE(GetFullType(&tmp_node_manager, rhs, &right_type), common::kTypeError,
               "invalid op type: ", GetLlvmObjectString(rhs))
    const node::TypeNode* output_type = nullptr;
    CHECK_STATUS(func(&tmp_node_manager, left_type, right_type, &output_type))
    return Status::OK();
}

static Status ExpandLlvmArgTypes(
    ::llvm::Module* m, const node::TypeNode* dtype, bool nullable,
    std::vector<std::pair<::llvm::Type*, bool>>* output) {
    if (dtype->base() == node::kTuple) {
        CHECK_TRUE(!nullable, kCodegenError, "kTuple should never be nullable");
        for (size_t i = 0; i < dtype->GetGenericSize(); ++i) {
            CHECK_STATUS(
                ExpandLlvmArgTypes(m, dtype->GetGenericType(i),
                                   dtype->IsGenericNullable(i), output),
                kCodegenError);
        }
    } else {
        ::llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLlvmType(m, dtype, &llvm_ty), kCodegenError, "Fail to lower ", dtype->GetName());
        output->emplace_back(llvm_ty, nullable);
    }
    return Status::OK();
}

static Status ExpandLlvmReturnTypes(
    ::llvm::Module* m, const node::TypeNode* dtype, bool nullable,
    std::vector<std::pair<::llvm::Type*, bool>>* output) {
    if (dtype->base() == node::kTuple) {
        CHECK_TRUE(!nullable, kCodegenError, "kTuple should never be nullable");
        for (size_t i = 0; i < dtype->GetGenericSize(); ++i) {
            CHECK_STATUS(ExpandLlvmReturnTypes(m, dtype->GetGenericType(i),
                                               dtype->IsGenericNullable(i),
                                               output));
        }
    } else {
        ::llvm::Type* llvm_ty = nullptr;
        CHECK_TRUE(GetLlvmType(m, dtype, &llvm_ty), kCodegenError,
                   "Fail to lower ", dtype->GetName());
        if (dtype->base() == node::kOpaque) {
            llvm_ty = ::llvm::Type::getInt8Ty(m->getContext());
        }
        output->push_back(std::make_pair(llvm_ty, nullable));
    }
    return Status::OK();
}

Status GetLlvmFunctionType(::llvm::Module* m,
                           const std::vector<const node::TypeNode*>& arg_types,
                           const std::vector<int>& arg_nullable,
                           const node::TypeNode* return_type,
                           bool return_nullable, bool variadic,
                           bool* return_by_arg, ::llvm::FunctionType** output) {
    // expand raw llvm arg types from signature
    std::vector<std::pair<::llvm::Type*, bool>> llvm_arg_types;
    for (size_t i = 0; i < arg_types.size(); ++i) {
        CHECK_STATUS(ExpandLlvmArgTypes(m, arg_types[i], arg_nullable[i],
                                        &llvm_arg_types))
    }

    ::llvm::Type* ret_llvm_ty = nullptr;

    // expand raw llvm return types
    std::vector<std::pair<::llvm::Type*, bool>> llvm_ret_types;
    CHECK_STATUS(
        ExpandLlvmReturnTypes(m, return_type, return_nullable, &llvm_ret_types))
    CHECK_TRUE(llvm_ret_types.size() > 0, kCodegenError);

    if (llvm_ret_types.size() > 1 ||
        TypeIRBuilder::IsStructPtr(llvm_ret_types[0].first) ||
        llvm_ret_types[0].second) {
        // tuple / struct / nullable
        *return_by_arg = true;
    }

    auto bool_ty = ::llvm::Type::getInt1Ty(m->getContext());
    std::vector<::llvm::Type*> expand_llvm_arg_types;
    for (auto& pair : llvm_arg_types) {
        expand_llvm_arg_types.push_back(pair.first);
        if (pair.second) {
            expand_llvm_arg_types.push_back(bool_ty);
        }
    }

    if (*return_by_arg) {
        ret_llvm_ty = ::llvm::Type::getVoidTy(m->getContext());
        for (auto& pair : llvm_ret_types) {
            auto addr_type = pair.first;
            bool is_struct_ptr = TypeIRBuilder::IsStructPtr(addr_type);
            if (!is_struct_ptr) {
                addr_type = addr_type->getPointerTo();
            }
            expand_llvm_arg_types.push_back(addr_type);
            if (pair.second) {
                expand_llvm_arg_types.push_back(bool_ty->getPointerTo());
            }
        }
    } else {
        CHECK_TRUE(return_type->base() != node::kTuple, kCodegenError);
        ret_llvm_ty = llvm_ret_types[0].first;
        if (return_type->base() == node::kOpaque) {
            ret_llvm_ty = ret_llvm_ty->getPointerTo();
        }
    }

    *output =
        llvm::FunctionType::get(ret_llvm_ty, expand_llvm_arg_types, variadic);
    return Status::OK();
}

llvm::Value* CreateAllocaAtHead(llvm::IRBuilder<>* builder, llvm::Type* dtype,
                                const std::string& name, llvm::Value* size) {
    ::llvm::BasicBlock* current_block = builder->GetInsertBlock();
    if (current_block == nullptr) {
        LOG(WARNING) << "Uninitialized builder";
        return nullptr;
    }
    ::llvm::Function* current_func = current_block->getParent();
    if (current_func == nullptr) {
        LOG(WARNING) << "Empty parent function";
        return nullptr;
    }
    ::llvm::BasicBlock* entry_block = &current_func->getEntryBlock();
    ::llvm::IRBuilder<> entry_builder(entry_block);
    if (!entry_block->empty()) {
        auto first_inst = entry_block->getFirstNonPHI();
        entry_builder.SetInsertPoint(first_inst);
    }
    return entry_builder.CreateAlloca(dtype, size, name);
}

}  // namespace codegen
}  // namespace hybridse
