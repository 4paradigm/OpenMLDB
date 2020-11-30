/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * expr_pass.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_EXPRESSION_EXPR_PASS_H_
#define SRC_PASSES_EXPRESSION_EXPR_PASS_H_

#include <string>
#include <unordered_map>
#include <unordered_set>
#include <vector>

#include "node/expr_node.h"
#include "node/sql_node.h"

namespace fesql {
namespace passes {

/**
 * Utility class to replace child expr in root expression tree inplace.
 */
class ExprReplacer {
 public:
    void AddReplacement(const node::ExprIdNode* arg, node::ExprNode* repl);
    void AddReplacement(const node::ExprNode* expr, node::ExprNode* repl);
    void AddReplacement(size_t column_id, node::ExprNode* repl);
    void AddReplacement(const std::string& relation_name,
                        const std::string& column_name, node::ExprNode* repl);

    fesql::base::Status Replace(node::ExprNode* root,
                                node::ExprNode** output) const;

 private:
    fesql::base::Status DoReplace(node::ExprNode* root,
                                  std::unordered_set<size_t>* visited,
                                  node::ExprNode** output) const;

    std::unordered_map<size_t, node::ExprNode*> arg_id_map_;
    std::unordered_map<size_t, node::ExprNode*> node_id_map_;
    std::unordered_map<size_t, node::ExprNode*> column_id_map_;
    std::unordered_map<std::string, node::ExprNode*> column_name_map_;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_EXPRESSION_EXPR_PASS_H_
