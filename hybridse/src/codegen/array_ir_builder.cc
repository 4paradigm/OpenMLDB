/*
 * Copyright 2022 4Paradigm authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "codegen/array_ir_builder.h"

#include <vector>
#include <string>

namespace hybridse {
namespace codegen {

void ArrayIRBuilder::InitStructType() {
    ::llvm::StringRef sr("fe.array");
    ::llvm::StructType* stype = m_->getTypeByName(sr);
    if (stype != NULL) {
        struct_type_ = stype;
        return;
    }
    stype = ::llvm::StructType::create(m_->getContext(), name);
    ::llvm::Type* ts_ty = (::llvm::Type::getInt64Ty(m_->getContext()));
    std::vector<::llvm::Type*> elements;
    elements.push_back(ts_ty);
    stype->setBody(::llvm::ArrayRef<::llvm::Type*>(elements));
    struct_type_ = stype;
}
}  // namespace codegen
}  // namespace hybridse
