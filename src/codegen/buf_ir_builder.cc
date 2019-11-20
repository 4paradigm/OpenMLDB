/*
 * buf_ir_builder.cc
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

#include "codegen/buf_ir_builder.h"

#include <string>
#include <utility>
#include "codegen/ir_base_builder.h"
#include "glog/logging.h"

namespace fesql {
namespace codegen {

BufIRBuilder::BufIRBuilder(::fesql::type::TableDef* table,
        ::llvm::BasicBlock* block,
        ScopeVar* scope_var):table_(table), block_(block), sv_(scope_var),
    types_() {
    // two byte header
    int32_t offset = 2;
    for (int32_t i = 0; i < table_->columns_size(); i++) {
        const ::fesql::type::ColumnDef& column = table_->columns(i);
        types_.insert(std::make_pair(column.name(),
                    std::make_pair(column.type(), offset)));
        DLOG(INFO) << "add column " << column.name() << " with type " 
            << ::fesql::type::Type_Name(column.type()) << " offset " << offset;
        switch (column.type()) {
            case ::fesql::type::kInt16:
                {
                    offset += 2;
                    break;
                }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat:
                {
                    offset += 4;
                    break;
                }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble:
                {
                    offset += 8;
                    break;
                }
            default:
                {
                    LOG(WARNING) << "not support type "
                        << ::fesql::type::Type_Name(column.type());
                }
        }
    }
}

BufIRBuilder::~BufIRBuilder() {}

bool BufIRBuilder::BuildGetField(const std::string& name,
       ::llvm::Value* row_ptr,
       ::llvm::Value** output) {

    if (output == NULL) {
        LOG(WARNING) << "output is null";
        return false;
    }

    Types::iterator it = types_.find(name);
    if (it == types_.end()) {
        LOG(WARNING) << "no column " << name << " in table " << table_->name();
        return false;
    }

    ::fesql::type::Type& fe_type =  it->second.first;
    int32_t offset = it->second.second;
    ::llvm::IRBuilder<> builder(block_);
    ::llvm::Type* llvm_type = NULL;

    bool ok = GetLLVMType(builder, fe_type, &llvm_type);
    if (!ok) {
        LOG(WARNING) << "fail to convert fe type to llvm type ";
        return false;
    }

    ::llvm::ConstantInt* llvm_offse = builder.getInt32(offset);
    return BuildLoadOffset(builder, row_ptr, llvm_offse, llvm_type, output);
}


}  // namespace codegen
}  // namespace fesql



