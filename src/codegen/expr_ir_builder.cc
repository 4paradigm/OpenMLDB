/*
 * expr_ir_builder.cc
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

#include "codegen/expr_ir_builder.h"
#include <string>
#include <vector>
#include "codegen/fn_ir_builder.h"
#include "codegen/ir_base_builder.h"
#include "codegen/type_ir_builder.h"
#include "codegen/buf_ir_builder.h"
#include "codegen/window_ir_builder.h"
#include "glog/logging.h"
#include "proto/common.pb.h"

namespace fesql {
namespace codegen {

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var)
    : block_(block),
      sv_(scope_var),
      row_mode_(true),
      row_ptr_name_(""),
      module_(nullptr),
      row_ir_builder_(),
      window_ir_builder_(){}

ExprIRBuilder::ExprIRBuilder(::llvm::BasicBlock* block, ScopeVar* scope_var,
                             const vm::Schema& schema,
                             const bool row_mode,
                             const std::string& row_ptr_name,
                             const std::string& row_size_name,
                             ::llvm::Module* module)
    : block_(block),
      sv_(scope_var),
      schema_(schema),
      row_mode_(row_mode),
      row_ptr_name_(row_ptr_name),
      row_size_name_(row_size_name),
      module_(module),
      row_ir_builder_(new BufNativeIRBuilder(schema, block, scope_var)),
      window_ir_builder_(new MemoryWindowDecodeIRBuilder(schema, block))
      {
}

ExprIRBuilder::~ExprIRBuilder() {}

::llvm::Function* ExprIRBuilder::GetFuncion(const std::string& fn_name,
                                            const ::fesql::node::DataType& type,
                                            common::Status& status) {
    ::llvm::Function* fn = module_->getFunction(fn_name);

    if (nullptr == fn) {
        if (::fesql::node::kTypeVoid != type) {
            const std::string suffix = fesql::node::DataTypeName(type);
            fn = module_->getFunction(fn_name + "_" + suffix);
            if (nullptr == fn) {
                status.set_code(common::kCallMethodError);
                status.set_msg("fail to find func with name " + fn_name + "_" +
                               suffix);
                return fn;
            }
        } else {
            status.set_code(common::kCallMethodError);
            status.set_msg("fail to find func with name " + fn_name);
            return fn;
        }
    }
    return fn;
}

bool ExprIRBuilder::Build(const ::fesql::node::ExprNode* node,
                          ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "node or output is null";
        return false;
    }
    ::llvm::IRBuilder<> builder(block_);
    switch (node->GetExprType()) {
        case ::fesql::node::kExprColumnRef: {
            const ::fesql::node::ColumnRefNode* n =
                (const ::fesql::node::ColumnRefNode*)node;
            return BuildColumnRef(n, output);
        }
        case ::fesql::node::kExprCall: {
            const ::fesql::node::CallExprNode* fn =
                (const ::fesql::node::CallExprNode*)node;
            return BuildCallFn(fn, output);
        }
        case ::fesql::node::kExprPrimary: {
            ::fesql::node::ConstNode* const_node =
                (::fesql::node::ConstNode*)node;

            switch (const_node->GetDataType()) {
                case ::fesql::node::kTypeInt16:
                    *output = builder.getInt16(const_node->GetSmallInt());
                    return true;
                case ::fesql::node::kTypeInt32:
                    *output = builder.getInt32(const_node->GetInt());
                    return true;
                case ::fesql::node::kTypeInt64:
                    *output = builder.getInt64(const_node->GetLong());
                    return true;
                case ::fesql::node::kTypeFloat:
                    return GetConstFloat(block_->getContext(),
                                         const_node->GetFloat(), output);
                case ::fesql::node::kTypeDouble:
                    return GetConstDouble(block_->getContext(),
                                          const_node->GetDouble(), output);
                case ::fesql::node::kTypeString: {
                    std::string val(const_node->GetStr(),
                                    strlen(const_node->GetStr()));
                    return GetConstFeString(val, block_, output);
                }
                default:
                    return false;
            }
        }
        case ::fesql::node::kExprId: {
            ::fesql::node::ExprIdNode* id_node =
                (::fesql::node::ExprIdNode*)node;
            ::llvm::Value* ptr = NULL;
            bool ok = sv_->FindVar(id_node->GetName(), &ptr);
            if (!ok || ptr == NULL) {
                LOG(WARNING) << "fail to find var " << id_node->GetName();
                return false;
            }
            if (ptr->getType()->isPointerTy()) {
                *output = builder.CreateLoad(ptr, id_node->GetName().c_str());
            } else {
                *output = ptr;
            }
            return true;
        }
        case ::fesql::node::kExprBinary: {
            return BuildBinaryExpr((::fesql::node::BinaryExpr*)node, output);
        }
        case ::fesql::node::kExprUnary: {
            return Build(node->children[0], output);
        }
        case ::fesql::node::kExprStruct: {
            return BuildStructExpr((fesql::node::StructExpr*)node, output);
        }
        default: {
            LOG(WARNING) << "not supported";
            return false;
        }
    }
}

bool ExprIRBuilder::BuildCallFn(const ::fesql::node::CallExprNode* call_fn,
                                ::llvm::Value** output) {
    // TODO(chenjing): return status;
    common::Status status;
    if (call_fn == NULL || output == NULL) {
        LOG(WARNING) << "call fn or output is null";
        status.set_code(common::kNullPointer);
        status.set_msg("null pointer");
        return false;
    }

    ::llvm::IRBuilder<> builder(block_);
    ::llvm::StringRef name(call_fn->GetFunctionName());

    std::vector<::llvm::Value*> llvm_args;
    const std::vector<::fesql::node::SQLNode*>& args = call_fn->GetArgs();
    std::vector<::fesql::node::SQLNode*>::const_iterator it = args.cbegin();
    ::fesql::node::DataType list_value_type = ::fesql::node::kTypeVoid;
    for (; it != args.cend(); ++it) {
        const ::fesql::node::ExprNode* arg = dynamic_cast<node::ExprNode*>(*it);
        ::llvm::Value* llvm_arg = NULL;
        // TODO(chenjing): remove out_name
        if (Build(arg, &llvm_arg)) {
            if (nullptr != call_fn->GetOver()) {
                fesql::type::Type base;
                fesql::type::Type v1_type;
                fesql::type::Type v2_type;
                if (false == GetFullType(llvm_arg->getType(), &base, &v1_type,
                                         &v2_type)) {
                    LOG(WARNING) << "fail to handle arg type";
                    return false;
                }
                // handle list type
                if (fesql::type::kList == base) {
                    if (false ==
                        ConvertFeSQLType2DataType(v1_type, list_value_type)) {
                        return false;
                    }
                    ::llvm::Type* i8_ptr_ty = builder.getInt8PtrTy();
                    llvm_arg = builder.CreatePointerCast(llvm_arg, i8_ptr_ty);
                }
            }
            llvm_args.push_back(llvm_arg);
        } else {
            LOG(WARNING) << "faild to build args: " << *arg;
            return false;
        }
    }

    ::llvm::Function* fn = GetFuncion(name, list_value_type, status);

    if (common::kOk != status.code()) {
        LOG(WARNING) << status.msg();
        return false;
    }

    if (args.size() != fn->arg_size()) {
        status.set_msg("Incorrect arguments passed");
        status.set_code(common::kCallMethodError);
        return false;
    }

    ::llvm::ArrayRef<::llvm::Value*> array_ref(llvm_args);
    *output = builder.CreateCall(fn->getFunctionType(), fn, array_ref);
    return true;
}

/**
 * Build Struct Expr IR:
 * TODO(chenjing): support method memeber
 * @param node
 * @param output
 * @return
 */
bool ExprIRBuilder::BuildStructExpr(const ::fesql::node::StructExpr* node,
                                    ::llvm::Value** output) {
    std::vector<::llvm::Type*> members;
    if (nullptr != node->GetFileds() && !node->GetFileds()->children.empty()) {
        for (auto each : node->GetFileds()->children) {
            node::FnParaNode* field = dynamic_cast<node::FnParaNode*>(each);
            ::llvm::Type* type;
            if (ConvertFeSQLType2LLVMType(field->GetParaType(), module_,
                                          &type)) {
                members.push_back(type);
            } else {
                LOG(WARNING)
                    << "Invalid struct with unacceptable field type: " +
                           ::fesql::node::DataTypeName(field->GetParaType());
                return false;
            }
        }
    }
    ::llvm::StringRef name(node->GetName());
    ::llvm::StructType* llvm_struct =
        ::llvm::StructType::create(module_->getContext(), name);
    ::llvm::ArrayRef<::llvm::Type*> array_ref(members);
    llvm_struct->setBody(array_ref);
    *output = (::llvm::Value*)llvm_struct;
    return true;
}
bool ExprIRBuilder::BuildColumnRef(const ::fesql::node::ColumnRefNode* node,
                                   ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "column ref node is null";
        return false;
    }

    if (row_mode_) {
        return BuildColumnItem(node->GetColumnName(), output);
    } else {
        return BuildColumnIterator(node->GetColumnName(), output);
    }
}

bool ExprIRBuilder::BuildColumnItem(const std::string& col,
                                    ::llvm::Value** output) {
    ::llvm::Value* row_ptr = NULL;
    bool ok = sv_->FindVar(row_ptr_name_, &row_ptr);

    if (!ok || row_ptr == NULL) {
        LOG(WARNING) << "fail to find row ptr with name " << row_ptr_name_;
        return false;
    }

    ::llvm::Value* row_size = NULL;
    ok = sv_->FindVar(row_size_name_, &row_size);
    if (!ok || row_size == NULL) {
        LOG(WARNING) << "fail to find row size with name " << row_size_name_;
        return false;
    }

    ::llvm::Value* value = NULL;
    ok = sv_->FindVar(col, &value);
    DLOG(INFO) << "get table column " << col;
    // not found
    if (!ok) {
        // TODO(wangtaize) buf ir builder add build get field ptr
        ok = row_ir_builder_->BuildGetField(col, row_ptr, row_size, &value);
        if (!ok || value == NULL) {
            LOG(WARNING) << "fail to find column " << col;
            return false;
        }
        ok = sv_->AddVar(col, value);
        if (ok) {
            *output = value;
        }

        return ok;
    } else {
        *output = value;
    }
    return true;
}

/**
 * Get col with given col name
 * set iterator struct pointer into output
 * TODO(chenjing): list ref manage
 * @param col
 * @param output
 * @return
 */
bool ExprIRBuilder::BuildColumnIterator(const std::string& col,
                                        ::llvm::Value** output) {
    ::llvm::Value* row_ptr = NULL;
    bool ok = sv_->FindVar(row_ptr_name_, &row_ptr);

    if (!ok || row_ptr == NULL) {
        LOG(WARNING) << "fail to find row ptr with name " << row_ptr_name_;
        return false;
    }

    ::llvm::Value* row_size = NULL;
    ok = sv_->FindVar(row_size_name_, &row_size);
    if (!ok || row_size == NULL) {
        LOG(WARNING) << "fail to find row size with name " << row_size_name_;
        return false;
    }

    ::llvm::Value* value = NULL;
    DLOG(INFO) << "get table column " << col;
    // NOT reuse for iterator
    ok = window_ir_builder_->BuildGetCol(col, row_ptr, &value);
    if (!ok || value == NULL) {
        LOG(WARNING) << "fail to find column " << col;
        return false;
    }
    *output = value;
    return ok;
}

bool ExprIRBuilder::BuildUnaryExpr(const ::fesql::node::UnaryExpr* node,
                                   ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }

    if (node->children.size() != 1) {
        LOG(WARNING) << "invalid unary expr node ";
        return false;
    }

    DLOG(INFO) << "build unary"
              << ::fesql::node::ExprTypeName(node->GetExprType());
    ::llvm::Value* left = NULL;
    bool ok = Build(node->children[0], &left);
    if (!ok) {
        LOG(WARNING) << "fail to build unary child";
        return false;
    }
    LOG(WARNING) << "can't support unary yet";
    return false;
}

bool ExprIRBuilder::BuildBinaryExpr(const ::fesql::node::BinaryExpr* node,
                                    ::llvm::Value** output) {
    if (node == NULL || output == NULL) {
        LOG(WARNING) << "input node or output is null";
        return false;
    }

    if (node->children.size() != 2) {
        LOG(WARNING) << "invalid binary expr node ";
        return false;
    }

    DLOG(INFO) << "build binary " << ::fesql::node::FnNodeName(node->GetType());
    ::llvm::Value* left = NULL;
    bool ok = Build(node->children[0], &left);
    if (!ok) {
        LOG(WARNING) << "fail to build left node";
        return false;
    }

    ::llvm::Value* right = NULL;
    ok = Build(node->children[1], &right);
    if (!ok) {
        LOG(WARNING) << "fail to build right node";
        return false;
    }

    if (right->getType()->isIntegerTy() && left->getType()->isIntegerTy()) {
        ::llvm::IRBuilder<> builder(block_);
        // TODO(wangtaize) type check
        switch (node->GetOp()) {
            case ::fesql::node::kFnOpAdd: {
                *output = builder.CreateAdd(left, right, "expr_add");
                return true;
            }
            case ::fesql::node::kFnOpMulti: {
                *output = builder.CreateMul(left, right, "expr_mul");
                return true;
            }
            case ::fesql::node::kFnOpMinus: {
                *output = builder.CreateSub(left, right, "expr_sub");
                return true;
            }
            default:
                LOG(WARNING) << "invalid op ";
                return false;
        }
    } else {
        LOG(WARNING) << "left mismatch right type";
        return false;
    }
}

}  // namespace codegen
}  // namespace fesql
