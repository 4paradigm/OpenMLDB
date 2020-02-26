/*
 * type_ir_builder.h
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

#ifndef SRC_CODEGEN_TYPE_IR_BUILDER_H_
#define SRC_CODEGEN_TYPE_IR_BUILDER_H_

#include <node/node_enum.h>
#include <string>
#include <vector>
#include "codegen/ir_base_builder.h"
namespace fesql {
namespace codegen {

struct StringRef {
    int32_t size;
    char* data;
};

struct String {
    int32_t size;
    char* data;
};

inline const bool ConvertFeSQLType2LLVMType(const node::TypeNode* data_type,
                                            ::llvm::Module* m,  // NOLINT
                                            ::llvm::Type** llvm_type) {
    if (nullptr == data_type) {
        LOG(WARNING) << "fail to convert data type to llvm type";
        return false;
    }
    switch (data_type->base_) {
        case fesql::node::kVoid:
            *llvm_type = (::llvm::Type::getVoidTy(m->getContext()));
            break;
        case fesql::node::kInt16:
            *llvm_type = (::llvm::Type::getInt16Ty(m->getContext()));
            break;
        case fesql::node::kInt32:
            *llvm_type = (::llvm::Type::getInt32Ty(m->getContext()));
            break;
        case fesql::node::kInt64:
            *llvm_type = (::llvm::Type::getInt64Ty(m->getContext()));
            break;
        case fesql::node::kFloat:
            *llvm_type = (::llvm::Type::getFloatTy(m->getContext()));
            break;
        case fesql::node::kDouble:
            *llvm_type = (::llvm::Type::getDoubleTy(m->getContext()));
            break;
        case fesql::node::kInt8Ptr:
            *llvm_type = (::llvm::Type::getInt8PtrTy(m->getContext()));
            break;
        case fesql::node::kVarchar: {
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
        case fesql::node::kList: {
            if (data_type->generics_.size() != 1) {
                LOG(WARNING) << "fail to convert data type: list generic types "
                                "number is " +
                                    data_type->generics_.size();
                return false;
            }
            std::string name;
        }
        default: {
            LOG(WARNING) << "fail to convert fesql datatype to llvm type: "
                         << data_type;
            return false;
        }
    }
    return true;
}

struct Timestamp {
    int64_t ts;
};

}  // namespace codegen
}  // namespace fesql
#endif  // SRC_CODEGEN_TYPE_IR_BUILDER_H_
