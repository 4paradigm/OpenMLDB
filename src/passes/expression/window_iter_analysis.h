/*
 * Copyright (c) 2021 4Paradigm
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

#ifndef SRC_PASSES_EXPRESSION_WINDOW_ITER_ANALYSIS_H_
#define SRC_PASSES_EXPRESSION_WINDOW_ITER_ANALYSIS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "node/expr_node.h"
#include "node/sql_node.h"
#include "udf/udf_library.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace passes {

using base::Status;

struct WindowIterRank {
    // nth window iter required
    size_t rank = 0;
    // should compute in {rank}th window iter
    bool is_iter = false;
};

/**
 * Compute expression rank about it's dependency to full window data.
 */
class WindowIterAnalysis {
 public:
    explicit WindowIterAnalysis(node::ExprAnalysisContext* ctx)
        : ctx_(ctx), scope_cache_list_(1) {}

    Status VisitFunctionLet(const node::ExprIdNode* row_arg,
                            const node::ExprIdNode* window_arg,
                            const node::ExprNode* body);

    // result query interface
    bool GetRank(const node::ExprNode* expr, WindowIterRank* rank) const;

 private:
    // cache
    struct ScopeCache {
        std::unordered_map<size_t, WindowIterRank> expr_dict;
        std::unordered_map<size_t, WindowIterRank> arg_dict;
    };

    Status VisitExpr(node::ExprNode* expr, WindowIterRank* rank);

    Status VisitCall(node::FnDefNode* fn,
                     const std::vector<WindowIterRank>& arg_ranks,
                     WindowIterRank* rank);

    Status VisitLambdaCall(node::LambdaNode* lambda,
                           const std::vector<WindowIterRank>& arg_ranks,
                           WindowIterRank* rank);

    Status VisitUDAF(node::UDAFDefNode* udaf, WindowIterRank* rank);

    node::ExprAnalysisContext* ctx_;

    // state management
    void EnterLambdaScope();
    void ExitLambdaScope();
    void SetRank(const node::ExprNode* expr, const WindowIterRank& rank);

    std::vector<ScopeCache> scope_cache_list_;

    const node::ExprNode* row_arg_ = nullptr;
    const node::ExprNode* window_arg_ = nullptr;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_EXPRESSION_WINDOW_ITER_ANALYSIS_H_
