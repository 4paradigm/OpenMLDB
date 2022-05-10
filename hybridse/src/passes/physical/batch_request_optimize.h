/*
 * Copyright 2021 4Paradigm
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

#include <map>
#include <memory>
#include <set>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "passes/physical/physical_pass.h"
#include "vm/physical_op.h"

#ifndef HYBRIDSE_SRC_PASSES_PHYSICAL_BATCH_REQUEST_OPTIMIZE_H_
#define HYBRIDSE_SRC_PASSES_PHYSICAL_BATCH_REQUEST_OPTIMIZE_H_

namespace hybridse {
namespace passes {

using hybridse::base::Status;
using hybridse::vm::PhysicalAggregationNode;
using hybridse::vm::PhysicalDataProviderNode;
using hybridse::vm::PhysicalProjectNode;
using hybridse::vm::PhysicalRenameNode;
using hybridse::vm::PhysicalRequestJoinNode;
using hybridse::vm::PhysicalRequestProviderNode;
using hybridse::vm::PhysicalRequestUnionNode;
using hybridse::vm::PhysicalSimpleProjectNode;

/**
 * Split op with common columns to common and non-common parts.
 */
class CommonColumnOptimize : public PhysicalPass {
 public:
    explicit CommonColumnOptimize(const std::set<size_t> common_column_indices);

    ~CommonColumnOptimize() {}
    Status Apply(PhysicalPlanContext* ctx, PhysicalOpNode* input,
                 PhysicalOpNode** out) override;

    void ExtractCommonNodeSet(std::set<size_t>* output);

    const std::set<size_t>& GetOutputCommonColumnIndices() const {
        return output_common_column_indices_;
    }

 private:
    void Init();

    // mapping from origin op -> new ops
    struct BuildOpState {
        PhysicalOpNode* common_op = nullptr;
        PhysicalOpNode* non_common_op = nullptr;
        PhysicalOpNode* concat_op = nullptr;
        PhysicalOpNode* reordered_op = nullptr;
        std::set<size_t> common_column_indices;

        void AddCommonIdx(size_t idx) { common_column_indices.insert(idx); }

        void SetAllCommon(PhysicalOpNode* op) {
            common_op = op;
            non_common_op = nullptr;
            for (size_t i = 0; i < op->GetOutputSchemaSize(); ++i) {
                common_column_indices.insert(i);
            }
        }

        void SetAllNonCommon(PhysicalOpNode* op) {
            common_op = nullptr;
            non_common_op = op;
            common_column_indices.clear();
        }

        bool IsInitialized() const {
            return common_op != nullptr || non_common_op != nullptr;
        }
    };

    /**
     * For each original op, split into op product common part and op
     * produce non-common part. Null on one side indicates there is no
     * common part or no non-common part outputs.
     */
    Status GetOpState(PhysicalPlanContext* ctx, PhysicalOpNode* input,
                      BuildOpState** state);

    /**
     * Get concat op of common and non-common part.
     */
    Status GetConcatOp(PhysicalPlanContext* ctx, PhysicalOpNode* input,
                       PhysicalOpNode** out);

    /**
     * Get concat op of common and non-common part with original column order.
     * The output op take the same schema slice count with original op.
     */
    Status GetReorderedOp(PhysicalPlanContext* ctx, PhysicalOpNode* input,
                          PhysicalOpNode** out);

    Status ProcessRequest(PhysicalPlanContext*, PhysicalRequestProviderNode*,
                          BuildOpState*);
    Status ProcessData(PhysicalPlanContext*, PhysicalDataProviderNode*,
                       BuildOpState*);
    Status ProcessSimpleProject(PhysicalPlanContext*,
                                PhysicalSimpleProjectNode*, BuildOpState*);
    Status ProcessProject(PhysicalPlanContext*, PhysicalProjectNode*,
                          BuildOpState*);
    Status ProcessWindow(PhysicalPlanContext*, PhysicalAggregationNode*,
                         BuildOpState*);
    Status ProcessJoin(PhysicalPlanContext*, PhysicalRequestJoinNode*,
                       BuildOpState*);
    Status ProcessConcat(PhysicalPlanContext*, PhysicalRequestJoinNode*,
                         BuildOpState*);
    Status ProcessRename(PhysicalPlanContext*, PhysicalRenameNode*,
                         BuildOpState*);
    Status ProcessTrivial(PhysicalPlanContext* ctx, PhysicalOpNode* op,
                          BuildOpState*);
    Status ProcessRequestUnion(PhysicalPlanContext*, PhysicalRequestUnionNode*,
                               const std::vector<PhysicalOpNode*>& path,
                               PhysicalOpNode** out,
                               BuildOpState** agg_request_state);

    /**
     * Find a non-agg op sequence ends with request union.
     */
    bool FindRequestUnionPath(PhysicalOpNode*, std::vector<PhysicalOpNode*>*);

    void SetAllCommon(PhysicalOpNode*);

    // input common column indices
    std::set<size_t> common_column_indices_;

    std::set<size_t> output_common_column_indices_;

    std::unordered_map<size_t, BuildOpState> build_dict_;
};

}  // namespace passes
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_PASSES_PHYSICAL_BATCH_REQUEST_OPTIMIZE_H_
