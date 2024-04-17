/**
 * Copyright (c) 2024 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_SRC_CODEGEN_INSERT_ROW_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_INSERT_ROW_BUILDER_H_

#include <memory>

#include "absl/status/statusor.h"
#include "codec/fe_row_codec.h"
#include "codegen/context.h"
#include "llvm/IR/Function.h"
#include "node/sql_node.h"
#include "vm/jit_wrapper.h"

namespace hybridse {
namespace codegen {

// Row builder that can output encode row from ExprNodes:
// schema + [ExprNode] -> row buf
//
// Common usage:
// - by insert statement that encode insert values into row bufs
// - by any row builder who want to build the row buf manually
class InsertRowBuilder {
 public:
    InsertRowBuilder(vm::HybridSeJitWrapper*, const codec::Schema*) ABSL_ATTRIBUTE_NONNULL();

    // compute the encoded row result for insert statement's single values expression list
    //
    // currently, expressions in insert values do not expect external source, so unsupported expressions
    // will simply fail on resolving.
    absl::StatusOr<std::shared_ptr<int8_t>> ComputeRow(absl::Span<node::ExprNode* const> values);

    absl::StatusOr<std::shared_ptr<int8_t>> ComputeRow(const node::ExprListNode* values);

    // compute the encoded row result for insert statement's single values expression list
    //
    // returns a pointer to encoded buf, you must manually delete the pointer after use
    absl::StatusOr<int8_t*> ComputeRowUnsafe(absl::Span<node::ExprNode* const> values);

 private:
    // build the function the will output the row from single insert values
    //
    // the function is just equivalent to C: `void fn(int8_t**)`.
    // BuildFn returns different function with different name on every invocation
    absl::StatusOr<llvm::Function*> BuildFn(CodeGenContext* ctx, llvm::StringRef fn_name,
                                            absl::Span<node::ExprNode* const>);

    const codec::Schema* schema_;
    vm::HybridSeJitWrapper* jit_;
    std::atomic<uint32_t> fn_counter_ = 0;
};
}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_INSERT_ROW_BUILDER_H_
