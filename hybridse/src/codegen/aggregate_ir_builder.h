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

#ifndef HYBRIDSE_SRC_CODEGEN_AGGREGATE_IR_BUILDER_H_
#define HYBRIDSE_SRC_CODEGEN_AGGREGATE_IR_BUILDER_H_
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include "codegen/expr_ir_builder.h"
#include "codegen/variable_ir_builder.h"
#include "llvm/IR/IRBuilder.h"
#include "node/plan_node.h"
#include "proto/fe_type.pb.h"
#include "vm/catalog.h"
#include "vm/schemas_context.h"

namespace hybridse {
namespace codegen {

struct AggColumnInfo {
    ::hybridse::node::ColumnRefNode* col;
    node::DataType col_type;
    size_t schema_idx;
    size_t col_idx;
    size_t offset;

    std::vector<std::string> agg_funcs;
    std::vector<size_t> output_idxs;

    AggColumnInfo() : col(nullptr) {}

    AggColumnInfo(::hybridse::node::ColumnRefNode* col,
                  const node::DataType& col_type, size_t schema_idx,
                  size_t col_idx, size_t offset)
        : col(col),
          col_type(col_type),
          schema_idx(schema_idx),
          col_idx(col_idx),
          offset(offset) {}

    const std::string GetColKey() const {
        return col->GetRelationName() + "." + col->GetColumnName();
    }

    void AddAgg(const std::string& fname, size_t output_idx) {
        agg_funcs.emplace_back(fname);
        output_idxs.emplace_back(output_idx);
    }

    size_t GetOutputNum() const { return output_idxs.size(); }

    std::string DebugString() const {
        std::stringstream ss;
        ss << DataTypeName(col_type) << " " << GetColKey() << ": [";
        for (size_t i = 0; i < GetOutputNum(); ++i) {
            ss << agg_funcs[i] << " -> " << output_idxs[i];
            if (i != GetOutputNum() - 1) {
                ss << ", ";
            }
        }
        ss << "]";
        return ss.str();
    }
};

class AggregateIRBuilder {
 public:
    AggregateIRBuilder(const vm::SchemasContext*, ::llvm::Module* module,
                       const node::FrameNode* frame_node, uint32_t id);

    // TODO(someone): remove temporary implementations for row-wise agg
    static bool EnableColumnAggOpt();

    bool CollectAggColumn(const node::ExprNode* expr, size_t output_idx,
                          ::hybridse::type::Type* col_type);

    static llvm::Type* GetOutputLlvmType(
        ::llvm::LLVMContext& llvm_ctx,  // NOLINT
        const std::string& fname, const node::DataType& node_type);

    base::Status BuildMulti(const std::string& base_funcname,
                    ExprIRBuilder* expr_ir_builder,
                    VariableIRBuilder* variable_ir_builder,
                    ::llvm::BasicBlock* cur_block,
                    const std::string& output_ptr_name,
                    const vm::Schema& output_schema);

    bool empty() const { return agg_col_infos_.empty(); }

 private:
    bool IsAggFuncName(absl::string_view fname) const;

    // schema context of input node
    const vm::SchemasContext* schema_context_;

    ::llvm::Module* module_;
    const node::FrameNode* frame_node_;
    uint32_t id_;
    std::unordered_map<std::string, AggColumnInfo> agg_col_infos_;
};

}  // namespace codegen
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_CODEGEN_AGGREGATE_IR_BUILDER_H_
