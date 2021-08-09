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

#ifndef SRC_CODEGEN_TYPE_IR_BUILDER_H_
#define SRC_CODEGEN_TYPE_IR_BUILDER_H_

#include <string>
#include <vector>
#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "codegen/ir_base_builder.h"
#include "node/node_enum.h"
#include "node/sql_node.h"
#include "node/type_node.h"

namespace hybridse {
namespace codegen {

class TypeIRBuilder {
 public:
    TypeIRBuilder() {}
    virtual ~TypeIRBuilder() {}
    static bool IsTimestampPtr(::llvm::Type* type);
    static bool IsDatePtr(::llvm::Type* type);
    static bool IsStringPtr(::llvm::Type* type);
    static bool IsStructPtr(::llvm::Type* type);
    static bool IsInt64(::llvm::Type* type);
    static bool IsBool(::llvm::Type* type);
    static bool IsNull(::llvm::Type* type);
    static bool IsInterger(::llvm::Type* type);
    static bool IsNumber(::llvm::Type* type);
    static bool isFloatPoint(::llvm::Type* type);
    static const std::string TypeName(::llvm::Type* type);

    static base::Status UnaryOpTypeInfer(
        const std::function<base::Status(
            node::NodeManager*, const node::TypeNode*, const node::TypeNode**)>,
        ::llvm::Type* lhs);
    static base::Status BinaryOpTypeInfer(
        const std::function<
            base::Status(node::NodeManager*, const node::TypeNode*,
                         const node::TypeNode*, const node::TypeNode**)>,
        ::llvm::Type* lhs, ::llvm::Type* rhs);
};

class Int64IRBuilder : public TypeIRBuilder {
 public:
    Int64IRBuilder() : TypeIRBuilder() {}
    ~Int64IRBuilder() {}
    static ::llvm::Type* GetType(::llvm::Module* m) {
        return ::llvm::Type::getInt64Ty(m->getContext());
    }
};

class Int32IRBuilder : public TypeIRBuilder {
 public:
    Int32IRBuilder() : TypeIRBuilder() {}
    ~Int32IRBuilder() {}
    static ::llvm::Type* GetType(::llvm::Module* m) {
        return ::llvm::Type::getInt32Ty(m->getContext());
    }
};
class Int16IRBuilder : public TypeIRBuilder {
 public:
    Int16IRBuilder() : TypeIRBuilder() {}
    ~Int16IRBuilder() {}
    static ::llvm::Type* GetType(::llvm::Module* m) {
        return ::llvm::Type::getInt16Ty(m->getContext());
    }
};

class BoolIRBuilder : public TypeIRBuilder {
 public:
    BoolIRBuilder() : TypeIRBuilder() {}
    ~BoolIRBuilder() {}
    static ::llvm::Type* GetType(::llvm::Module* m) {
        return ::llvm::Type::getInt1Ty(m->getContext());
    }
};
inline const bool ConvertHybridSeType2LlvmType(const node::TypeNode* data_type,
                                               ::llvm::Module* m,  // NOLINT
                                               ::llvm::Type** llvm_type) {
    if (nullptr == data_type) {
        LOG(WARNING) << "fail to convert data type to llvm type";
        return false;
    }
    switch (data_type->base_) {
        case hybridse::node::kVoid:
            *llvm_type = (::llvm::Type::getVoidTy(m->getContext()));
            break;
        case hybridse::node::kInt16:
            *llvm_type = (::llvm::Type::getInt16Ty(m->getContext()));
            break;
        case hybridse::node::kInt32:
            *llvm_type = (::llvm::Type::getInt32Ty(m->getContext()));
            break;
        case hybridse::node::kInt64:
            *llvm_type = (::llvm::Type::getInt64Ty(m->getContext()));
            break;
        case hybridse::node::kFloat:
            *llvm_type = (::llvm::Type::getFloatTy(m->getContext()));
            break;
        case hybridse::node::kDouble:
            *llvm_type = (::llvm::Type::getDoubleTy(m->getContext()));
            break;
        case hybridse::node::kInt8Ptr:
            *llvm_type = (::llvm::Type::getInt8PtrTy(m->getContext()));
            break;
        case hybridse::node::kVarchar: {
            std::string name = "fe.string_ref";
            ::llvm::StringRef sr(name);
            ::llvm::StructType* stype = m->getTypeByName(sr);
            if (stype != NULL) {
                *llvm_type = stype;
                return true;
            }
            stype = ::llvm::StructType::create(m->getContext(), name);
            ::llvm::Type* size_ty = (::llvm::Type::getInt32Ty(m->getContext()));
            ::llvm::Type* data_ptr_ty =
                (::llvm::Type::getInt8PtrTy(m->getContext()));
            std::vector<::llvm::Type*> elements;
            elements.push_back(size_ty);
            elements.push_back(data_ptr_ty);
            stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
            *llvm_type = stype;
            return true;
        }
        case hybridse::node::kList: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING) << "fail to convert data type: list generic types "
                                "number is " +
                                    std::to_string(data_type->generics_.size());
                return false;
            }
            std::string name;
        }
        default: {
            LOG(WARNING) << "fail to convert hybridse datatype to llvm type: "
                         << data_type;
            return false;
        }
    }
    return true;
}

}  // namespace codegen
}  // namespace hybridse
#endif  // SRC_CODEGEN_TYPE_IR_BUILDER_H_
