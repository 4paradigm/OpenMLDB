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

#ifndef HYBRIDSE_SRC_CODEGEN_TYPE_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_TYPE_IR_BUILDER_H_

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

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_TYPE_IR_BUILDER_H_
