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
#include <utility>
#include <vector>

#include "absl/status/status.h"
#include "absl/strings/substitute.h"
#include "codec/list_iterator_codec.h"
#include "codegen/array_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/map_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "glog/logging.h"
#include "llvm/ADT/StringRef.h"
#include "llvm/IR/GlobalVariable.h"
#include "node/node_manager.h"
#include "proto/fe_type.pb.h"

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
            auto* map_type = data_type->GetAsOrNull<node::MapType>();
            llvm::Type* key_type = nullptr;
            if (false == GetLlvmType(m, map_type->key_type(), &key_type)) {
                return false;
            }
            llvm::Type* value_type = nullptr;
            if (false == GetLlvmType(m, map_type->value_type(), &value_type)) {
                return false;
            }
            MapIRBuilder builder(m, key_type, value_type);
            *llvm_type = builder.GetType()->getPointerTo();
            return true;
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

absl::StatusOr<llvm::Value*> BuildGetPtrOffset(::llvm::IRBuilder<>* builder, ::llvm::Value* ptr,
                                               ::llvm::Value* offset, ::llvm::Type* dst_type) {
    auto* ori_type = ptr->getType();
    if (!ori_type->isPointerTy()) {
        return absl::InvalidArgumentError(
            absl::Substitute("expect pointer type, but got $0", GetLlvmObjectString(ori_type)));
    }

    if (!offset->getType()->isIntegerTy()) {
        return absl::InvalidArgumentError(
            absl::Substitute("expect integer type for offset, but got $0", GetLlvmObjectString(offset->getType())));
    }

    // cast ptr to int64
    ::llvm::Type* int64_ty = builder->getInt64Ty();
    ::llvm::Value* ptr_int64_ty = builder->CreatePtrToInt(ptr, int64_ty);
    ::llvm::Value* offset_int64 = offset;
    if (offset->getType() != int64_ty) {
        offset_int64 = builder->CreateIntCast(offset, int64_ty, true, "cast_32_to_64");
    }
    ::llvm::Value* ptr_add_offset = builder->CreateAdd(ptr_int64_ty, offset_int64, "ptr_add_offset");
    llvm::Type* dst = ori_type;
    if (dst_type != nullptr) {
        if (!dst_type->isPointerTy()) {
            return absl::InvalidArgumentError("dst type is not a pointer");
        }
        dst = dst_type;
    }
    return builder->CreateIntToPtr(ptr_add_offset, dst);
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
            if (type->isPointerTy()) {
                auto type_pointee = type->getPointerElementType();
                if (type_pointee->isStructTy()) {
                    auto* key_type = type_pointee->getStructElementType(1);
                    const node::TypeNode* key = nullptr;
                    if (!key_type->isPointerTy() || !GetFullType(nm, key_type->getPointerElementType(), &key)) {
                        return false;
                    }
                    const node::TypeNode* value = nullptr;
                    auto* value_type = type_pointee->getStructElementType(2);
                    if (!value_type->isPointerTy() || !GetFullType(nm, value_type->getPointerElementType(), &value)) {
                        return false;
                    }

                    *type_node = nm->MakeNode<node::MapType>(key, value);
                    return true;
                }
            }
            return false;
        }
        case hybridse::node::kArray: {
            if (type->isPointerTy()) {
                auto type_pointee = type->getPointerElementType();
                if (type_pointee->isStructTy()) {
                    auto* key_type = type_pointee->getStructElementType(0);
                    const node::TypeNode* key = nullptr;
                    if (!key_type->isPointerTy() || !GetFullType(nm, key_type->getPointerElementType(), &key)) {
                        return false;
                    }

                    *type_node = nm->MakeNode<node::TypeNode>(node::DataType::kArray, key);
                    return true;
                }
            }
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
        case ::llvm::Type::VoidTyID: {
            *output = ::hybridse::node::kVoid;
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

            auto struct_name = pointee_ty->getStructName();
            if (struct_name.startswith("fe.list_ref_")) {
                *output = hybridse::node::kList;
                return true;
            } else if (struct_name.startswith("fe.iterator_ref_")) {
                *output = hybridse::node::kIterator;
                return true;
            } else if (struct_name.equals("fe.string_ref")) {
                *output = hybridse::node::kVarchar;
                return true;
            } else if (struct_name.equals("fe.timestamp")) {
                *output = hybridse::node::kTimestamp;
                return true;
            } else if (struct_name.equals("fe.date")) {
                *output = hybridse::node::kDate;
                return true;
            } else if (struct_name.startswith("fe.array_")) {
                *output = hybridse::node::kArray;
                return true;
            } else if (struct_name.startswith("fe.map_")) {
                *output = hybridse::node::kMap;
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

absl::Status BuildStoreOffset(::llvm::IRBuilder<>* builder, ::llvm::Value* ptr, ::llvm::Value* offset,
                              ::llvm::Value* value) {
    if (ptr == NULL || offset == NULL || value == NULL) {
        return absl::InvalidArgumentError("ptr or offset or value is null");
    }
    auto s = BuildGetPtrOffset(builder, ptr, offset, value->getType()->getPointerTo());
    if (!s.ok()) {
        return s.status();
    }
    builder->CreateStore(value, s.value(), false);
    return absl::OkStatus();
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

static auto CreateBaseDataType2SchemaTypeMap() {
    // type mapping for SQL base types only
    absl::flat_hash_map<node::DataType, type::Type> map = {{node::DataType::kBool, type::kBool},
                                                           {node::DataType::kInt16, type::kInt16},
                                                           {node::DataType::kInt32, type::kInt32},
                                                           {node::DataType::kInt64, type::kInt64},
                                                           {node::DataType::kFloat, type::kFloat},
                                                           {node::DataType::kDouble, type::kDouble},
                                                           {node::DataType::kVarchar, type::kVarchar},
                                                           {node::DataType::kDate, type::kDate},
                                                           {node::DataType::kTimestamp, type::kTimestamp},

                                                           // historic reason, null is bool during encoding
                                                           {node::DataType::kNull, type::kBool},
                                                           {node::DataType::kVoid, type::kBool}};
    return map;
}

static const auto& GetBaseDataType2SchemaTypeMap() {
    static const absl::flat_hash_map<node::DataType, type::Type>& map = *new auto(CreateBaseDataType2SchemaTypeMap());
    return map;
}
static auto CreateSchemaType2BaseDataTypeMap() {
    // type mapping for SQL base types only
    absl::flat_hash_map<type::Type, node::DataType> map = {
        {type::kBool, node::DataType::kBool},          {type::kInt16, node::DataType::kInt16},
        {type::kInt32, node::DataType::kInt32},        {type::kInt64, node::DataType::kInt64},
        {type::kFloat, node::DataType::kFloat},        {type::kDouble, node::DataType::kDouble},
        {type::kVarchar, node::DataType::kVarchar},    {type::kDate, node::DataType::kDate},
        {type::kTimestamp, node::DataType::kTimestamp}};
    return map;
}

static const auto& GetSchemaType2BaseTypeMap() {
    static const absl::flat_hash_map<type::Type, node::DataType>& map = *new auto(CreateSchemaType2BaseDataTypeMap());
    return map;
}

absl::Status Type2ColumnSchema(const node::TypeNode* type, type::ColumnSchema* mut_schema) {
    if (type->IsMap()) {
        assert(type->GetGenericSize() == 2);
        auto* mut_map_type = mut_schema->mutable_map_type();
        auto* mut_map_key_type = mut_map_type->mutable_key_type();
        auto* mut_map_value_type = mut_map_type->mutable_value_type();
        auto s = Type2ColumnSchema(type->GetGenericType(0), mut_map_key_type);
        s.Update(Type2ColumnSchema(type->GetGenericType(1), mut_map_value_type));
        return s;
    } else if (type->IsArray()) {
        assert(type->GetGenericSize() == 1);
        auto* mut_array_type = mut_schema->mutable_array_type();
        auto* mut_array_ele_type = mut_array_type->mutable_ele_type();
        return Type2ColumnSchema(type->GetGenericType(0), mut_array_ele_type);
    }

    // simple type
    auto& map = GetBaseDataType2SchemaTypeMap();
    auto it = map.find(type->base());
    if (it == map.end()) {
        return absl::UnimplementedError(absl::StrCat("unable to convert from ", type->DebugString()));
    }
    mut_schema->set_base_type(it->second);
    return absl::OkStatus();
}

absl::StatusOr<node::TypeNode*> ColumnSchema2Type(const type::ColumnSchema& schema, node::NodeManager* tmp_nm) {
    if (schema.has_map_type()) {
        auto& map_type = schema.map_type();
        auto s1 = ColumnSchema2Type(map_type.key_type(), tmp_nm);
        if (!s1.ok()) {
            return s1.status();
        }
        auto s2 = ColumnSchema2Type(map_type.value_type(), tmp_nm);
        if (!s2.ok()) {
            return s2.status();
        }

        return tmp_nm->MakeNode<node::MapType>(s1.value(), s2.value());
    } else if (schema.has_array_type()) {
        auto& arr_type = schema.array_type();
        auto s = ColumnSchema2Type(arr_type.ele_type(), tmp_nm);
        if (!s.ok()) {
            return s.status();
        }
        return tmp_nm->MakeNode<node::TypeNode>(node::kArray, s.value());
    } else if (schema.has_base_type()) {
        auto& map = GetSchemaType2BaseTypeMap();
        auto it = map.find(schema.base_type());
        if (it == map.end()) {
            return absl::UnimplementedError(absl::StrCat("column schema to type node: ", schema.DebugString()));
        }

        return tmp_nm->MakeNode<node::TypeNode>(it->second);
    }

    return absl::UnimplementedError(absl::StrCat("unknown type: ", schema.DebugString()));
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

llvm::Value* CodecSizeForPrimitive(llvm::IRBuilder<>* builder, llvm::Type* type) {
    return builder->getInt32((type->getPrimitiveSizeInBits() + 7) / 8);
}

std::string GetIRTypeName(llvm::Type* type)  {
    std::string res;
    llvm::raw_string_ostream ss(res);
    type->print(ss, false, true);
    return ss.str();
}

void PrintLog(llvm::LLVMContext* context, llvm::Module* module, llvm::IRBuilder<>* builder, absl::string_view toPrint,
              bool useGlobal) {
    llvm::FunctionCallee printFunct =
        module->getOrInsertFunction("printLog", builder->getVoidTy(), builder->getInt8PtrTy());

    llvm::Value* stringVar;
    llvm::Constant* stringConstant =
        llvm::ConstantDataArray::getString(*context, llvm::StringRef(toPrint.data(), toPrint.size()));

    // array[i8] type
    if (useGlobal) {
        stringVar = builder->CreateGlobalString(llvm::StringRef(toPrint.data(), toPrint.size()));
        // Note: Does not work without allocation
        // stringVar = new llvm::GlobalVariable(*module, stringConstant->getType(), true,
        //                                      llvm::GlobalValue::PrivateLinkage, stringConstant, "");
    } else {
        stringVar = builder->CreateAlloca(stringConstant->getType());
        builder->CreateStore(stringConstant, stringVar);
    }

    llvm::Value* cast = builder->CreatePointerCast(stringVar, builder->getInt8PtrTy());
    builder->CreateCall(printFunct, cast);
}

}  // namespace codegen
}  // namespace hybridse
