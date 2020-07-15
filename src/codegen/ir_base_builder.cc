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
#include "codec/list_iterator_codec.h"
#include "codegen/date_ir_builder.h"
#include "codegen/string_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

bool GetLLVMType(::llvm::BasicBlock* block,
                 const ::fesql::node::DataType& type,  // NOLINT
                 ::llvm::Type** output) {
    if (output == NULL || block == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    return GetLLVMType(block->getModule(), type, output);
}

bool GetLLVMType(::llvm::BasicBlock* block,
                 const ::fesql::node::TypeNode* type,  // NOLINT
                 ::llvm::Type** output) {
    if (output == NULL || block == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    return GetLLVMType(block->getModule(), type, output);
}
bool GetLLVMType(::llvm::Module* m, const ::fesql::node::DataType& type,
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
            LOG(WARNING) << "fail to convert fesql datatype to llvm type: ";
            return false;
        }
    }
    return true;
}

bool GetLLVMColumnSize(::fesql::node::TypeNode* v_type, uint32_t* size) {
    if (nullptr == size) {
        LOG(WARNING) << "the size ptr is NULL ";
        return false;
    }

    switch (v_type->base_) {
        case ::fesql::node::kInt16: {
            *size = sizeof(::fesql::codec::ColumnImpl<int16_t>);
            break;
        }
        case ::fesql::node::kInt32: {
            *size = sizeof(::fesql::codec::ColumnImpl<int32_t>);
            break;
        }
        case ::fesql::node::kInt64: {
            *size = sizeof(::fesql::codec::ColumnImpl<int64_t>);
            break;
        }
        case ::fesql::node::kDouble: {
            *size = sizeof(::fesql::codec::ColumnImpl<double>);
            break;
        }
        case ::fesql::node::kFloat: {
            *size = sizeof(::fesql::codec::ColumnImpl<float>);
            break;
        }
        case ::fesql::node::kVarchar: {
            *size = sizeof(::fesql::codec::StringColumnImpl);
            break;
        }
        case ::fesql::node::kTimestamp: {
            *size = sizeof(::fesql::codec::ColumnImpl<codec::Timestamp>);
            break;
        }
        case ::fesql::node::kDate: {
            *size = sizeof(::fesql::codec::ColumnImpl<codec::Date>);
            break;
        }
        default: {
            LOG(WARNING) << "not supported type " << v_type->GetName();
            return false;
        }
    }
    return true;
}  // namespace codegen

bool GetLLVMListType(::llvm::Module* m,
                     ::fesql::node::TypeNode* v_type,  // NOLINT
                     ::llvm::Type** output) {
    if (output == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    std::string name;
    switch (v_type->base_) {
        case ::fesql::node::kInt16: {
            name = "fe.list_ref_int16";
            break;
        }
        case ::fesql::node::kInt32: {
            name = "fe.list_ref_int32";
            break;
        }
        case ::fesql::node::kInt64: {
            name = "fe.list_ref_int64";
            break;
        }
        case ::fesql::node::kTimestamp: {
            name = "fe.list_ref_timestamp";
            break;
        }
        case ::fesql::node::kDate: {
            name = "fe.list_ref_date";
            break;
        }
        case ::fesql::node::kFloat: {
            name = "fe.list_ref_float";
            break;
        }
        case ::fesql::node::kDouble: {
            name = "fe.list_ref_double";
            break;
        }
        case ::fesql::node::kVarchar: {
            name = "fe.list_ref_string";
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

bool GetLLVMIteratorType(::llvm::Module* m,
                         ::fesql::node::TypeNode* v_type,  // NOLINT
                         ::llvm::Type** output) {
    if (output == NULL) {
        LOG(WARNING) << "the output ptr is NULL ";
        return false;
    }
    std::string name;
    switch (v_type->base_) {
        case ::fesql::node::kInt16: {
            name = "fe.iterator_ref_int16";
            break;
        }
        case ::fesql::node::kInt32: {
            name = "fe.iterator_ref_int32";
            break;
        }
        case ::fesql::node::kInt64: {
            name = "fe.iterator_ref_int64";
            break;
        }
        case ::fesql::node::kFloat: {
            name = "fe.iterator_ref_float";
            break;
        }
        case ::fesql::node::kDouble: {
            name = "fe.iterator_ref_double";
            break;
        }
        case ::fesql::node::kVarchar: {
            name = "fe.iterator_ref_string";
            break;
        }
        case ::fesql::node::kTimestamp: {
            name = "fe.iterator_ref_timestamp";
            break;
        }
        case ::fesql::node::kDate: {
            name = "fe.iterator_ref_date";
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

bool GetLLVMType(::llvm::Module* m, const fesql::node::TypeNode* data_type,
                 ::llvm::Type** llvm_type) {
    if (nullptr == data_type) {
        LOG(WARNING) << "fail to convert data type to llvm type";
        return false;
    }
    switch (data_type->base_) {
        case fesql::node::kList: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING) << "fail to convert data type: list generic types "
                                "number is "
                             << data_type->generics_.size();
                return false;
            }
            ::llvm::Type* list_type = nullptr;
            if (false ==
                GetLLVMListType(m, data_type->generics_[0], &list_type)) {
                return false;
            }
            *llvm_type = list_type->getPointerTo();
            return true;
        }
        case fesql::node::kIterator: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING)
                    << "fail to convert data type: iterator generic types "
                       "number is "
                    << data_type->generics_.size();
                return false;
            }
            ::llvm::Type* list_type = nullptr;
            if (false ==
                GetLLVMIteratorType(m, data_type->generics_[0], &list_type)) {
                return false;
            }
            *llvm_type = list_type->getPointerTo();
            return true;
        }
        case fesql::node::kMap: {
            LOG(WARNING) << "fail to codegen map type, currently not support";
        }
        default: {
            return GetLLVMType(m, data_type->base_, llvm_type);
        }
    }
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
bool GetFullType(::llvm::Type* type, ::fesql::node::TypeNode* type_node) {
    if (type == NULL || type_node == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }
    if (false == GetBaseType(type, &type_node->base_)) {
        LOG(WARNING) << "Fail to get base type of "
                     << GetLLVMObjectString(type);
        return false;
    }

    switch (type_node->base_) {
        case fesql::node::kList: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }
            if (type->getStructName().equals("fe.list_ref_int16")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kInt16));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_int32")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kInt32));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_int64")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kInt64));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_timestamp")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kTimestamp));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_float")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kFloat));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_double")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kDouble));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_string")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kVarchar));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_timestamp")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kTimestamp));
                return true;
            } else if (type->getStructName().equals("fe.list_ref_date")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kDate));
                return true;
            }
            LOG(WARNING) << "fail to get type of llvm type for "
                         << type->getStructName().str();
            return false;
        }
        case fesql::node::kIterator: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }
            if (type->getStructName().equals("fe.iterator_ref_int16")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kInt16));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_int32")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kInt32));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_int64")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kInt64));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_float")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kFloat));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_double")) {
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kDouble));
                return true;

            } else if (type->getStructName().equals("fe.iterator_ref_string")) {
                type_node->base_ = fesql::node::kIterator;
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kVarchar));
                return true;
            } else if (type->getStructName().equals(
                           "fe.iterator_ref_timestamp")) {
                type_node->base_ = fesql::node::kIterator;
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kTimestamp));
                return true;
            } else if (type->getStructName().equals("fe.iterator_ref_date")) {
                type_node->base_ = fesql::node::kIterator;
                type_node->generics_.push_back(
                    new node::TypeNode(fesql::node::kDate));
                return true;
            }
            LOG(WARNING) << "fail to get type of llvm type for "
                         << type->getStructName().str();
            return false;
        }
        case fesql::node::kMap: {
            LOG(WARNING) << "fail to get type for map";
            return false;
        }
        default: {
            return true;
        }
    }
}

bool IsStringType(::llvm::Type* type) {
    ::fesql::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kVarchar;
}
bool GetBaseType(::llvm::Type* type, ::fesql::node::DataType* output) {
    if (type == NULL || output == NULL) {
        LOG(WARNING) << "type or output is null";
        return false;
    }
    switch (type->getTypeID()) {
        case ::llvm::Type::FloatTyID: {
            *output = ::fesql::node::kFloat;
            return true;
        }
        case ::llvm::Type::DoubleTyID: {
            *output = ::fesql::node::kDouble;
            return true;
        }
        case ::llvm::Type::IntegerTyID: {
            switch (type->getIntegerBitWidth()) {
                case 1: {
                    *output = ::fesql::node::kBool;
                    return true;
                }
                case 16: {
                    *output = ::fesql::node::kInt16;
                    return true;
                }
                case 32: {
                    *output = ::fesql::node::kInt32;
                    return true;
                }
                case 64: {
                    *output = ::fesql::node::kInt64;
                    return true;
                }
                default: {
                    LOG(WARNING) << "no mapping type for llvm type";
                    return false;
                }
            }
        }
        case ::llvm::Type::PointerTyID: {
            if (type->getTypeID() == ::llvm::Type::PointerTyID) {
                type = reinterpret_cast<::llvm::PointerType*>(type)
                           ->getElementType();
            }

            if (::llvm::Type::StructTyID != type->getTypeID()) {
                LOG(WARNING) << "no mapping type for llvm type";
                return false;
            }

            if (type->getStructName().startswith("fe.list_ref_")) {
                *output = fesql::node::kList;
                return true;
            } else if (type->getStructName().startswith("fe.iterator_ref_")) {
                *output = fesql::node::kIterator;
                return true;
            } else if (type->getStructName().equals("fe.string_ref")) {
                *output = fesql::node::kVarchar;
                return true;
            } else if (type->getStructName().equals("fe.timestamp")) {
                *output = fesql::node::kTimestamp;
                return true;
            } else if (type->getStructName().equals("fe.date")) {
                *output = fesql::node::kDate;
                return true;
            }
            LOG(WARNING) << "no mapping type for llvm type "
                         << type->getStructName().str();
            return false;
        }
        default: {
            LOG(WARNING) << "no mapping type for llvm type: "
                         << GetLLVMObjectString(type);
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
bool DataType2SchemaType(const ::fesql::node::TypeNode& type,
                         ::fesql::type::Type* output) {
    switch (type.base_) {
        case ::fesql::node::kBool: {
            *output = ::fesql::type::kBool;
            break;
        }
        case ::fesql::node::kInt16: {
            *output = ::fesql::type::kInt16;
            break;
        }
        case ::fesql::node::kInt32: {
            *output = ::fesql::type::kInt32;
            break;
        }
        case ::fesql::node::kInt64: {
            *output = ::fesql::type::kInt64;
            break;
        }
        case ::fesql::node::kFloat: {
            *output = ::fesql::type::kFloat;
            break;
        }
        case ::fesql::node::kDouble: {
            *output = ::fesql::type::kDouble;
            break;
        }
        case ::fesql::node::kVarchar: {
            *output = ::fesql::type::kVarchar;
            break;
        }
        case ::fesql::node::kTimestamp: {
            *output = ::fesql::type::kTimestamp;
            break;
        }
        case ::fesql::node::kDate: {
            *output = ::fesql::type::kDate;
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
bool SchemaType2DataType(const ::fesql::type::Type type,
                         ::fesql::node::TypeNode* output) {
    if (nullptr == output) {
        LOG(WARNING) << "Fail convert type: input is null";
        return false;
    }
    return SchemaType2DataType(type, &output->base_);
}
bool SchemaType2DataType(const ::fesql::type::Type type,
                         ::fesql::node::DataType* output) {
    if (nullptr == output) {
        LOG(WARNING) << "Fail convert type: input is null";
        return false;
    }
    switch (type) {
        case ::fesql::type::kInt16: {
            *output = ::fesql::node::kInt16;
            break;
        }
        case ::fesql::type::kInt32: {
            *output = ::fesql::node::kInt32;
            break;
        }
        case ::fesql::type::kInt64: {
            *output = ::fesql::node::kInt64;
            break;
        }
        case ::fesql::type::kFloat: {
            *output = ::fesql::node::kFloat;
            break;
        }
        case ::fesql::type::kDouble: {
            *output = ::fesql::node::kDouble;
            break;
        }
        case ::fesql::type::kVarchar: {
            *output = ::fesql::node::kVarchar;
            break;
        }
        case ::fesql::type::kTimestamp: {
            *output = ::fesql::node::kTimestamp;
            break;
        }
        case ::fesql::type::kDate: {
            *output = ::fesql::node::kDate;
            break;
        }
        default: {
            LOG(WARNING) << "unrecognized schema type "
                         << ::fesql::type::Type_Name(type);
            return false;
        }
    }
    return true;
}
bool TypeIRBuilder::IsTimestampPtr(::llvm::Type* type) {
    ::fesql::node::DataType data_type;
    if (!IsStructPtr(type)) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kTimestamp;
}
bool TypeIRBuilder::IsInt64(::llvm::Type* type) {
    ::fesql::node::DataType data_type;
    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kInt64;
}
bool TypeIRBuilder::IsInterger(::llvm::Type* type) {
    return type->isIntegerTy();
}
const std::string TypeIRBuilder::TypeName(::llvm::Type* type) {
    node::TypeNode type_node;
    if (!GetTypeNode(type, &type_node)) {
        return "unknow";
    }
    return type_node.GetName();
}
const bool TypeIRBuilder::GetTypeNode(::llvm::Type* type,
                                      node::TypeNode* type_node) {
    return GetFullType(type, type_node);
}
bool TypeIRBuilder::IsDatePtr(::llvm::Type* type) {
    ::fesql::node::DataType data_type;
    if (!IsStructPtr(type)) {
        return false;
    }

    if (!GetBaseType(type, &data_type)) {
        return false;
    }
    return data_type == node::kDate;
}
bool TypeIRBuilder::IsStringPtr(::llvm::Type* type) {
    ::fesql::node::DataType data_type;
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

}  // namespace codegen
}  // namespace fesql
