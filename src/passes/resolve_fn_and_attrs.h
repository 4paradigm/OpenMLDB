/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * resolve_fn_and_attrs.h
 *--------------------------------------------------------------------------
 **/
#ifndef SRC_PASSES_RESOLVE_FN_AND_ATTRS_H_
#define SRC_PASSES_RESOLVE_FN_AND_ATTRS_H_

#include <string>
#include <unordered_map>
#include <vector>

#include "node/expr_node.h"
#include "node/plan_node.h"
#include "node/sql_node.h"
#include "udf/udf_library.h"
#include "vm/schemas_context.h"

namespace fesql {
namespace passes {

using base::Status;

class ResolveFnAndAttrs {
 public:
    ResolveFnAndAttrs(node::NodeManager* nm, udf::UDFLibrary* library,
                      const vm::SchemaSourceList& input_schemas)
        : nm_(nm),
          library_(library),
          input_schemas_(input_schemas),
          schemas_context_(input_schemas),
          analysis_context_(nm, &schemas_context_) {}

    Status VisitFnDef(node::FnDefNode* fn,
                      const std::vector<const node::TypeNode*>& arg_types,
                      node::FnDefNode** output);

    Status VisitLambda(node::LambdaNode* lambda,
                       const std::vector<const node::TypeNode*>& arg_types,
                       node::LambdaNode** output);

    Status VisitUDFDef(node::UDFDefNode* lambda,
                       const std::vector<const node::TypeNode*>& arg_types,
                       node::UDFDefNode** output);

    Status VisitUDAFDef(node::UDAFDefNode* lambda,
                        const std::vector<const node::TypeNode*>& arg_types,
                        node::UDAFDefNode** output);

    Status VisitExpr(node::ExprNode* expr, node::ExprNode** output);

 private:
    Status CheckSignature(node::FnDefNode* fn,
                          const std::vector<const node::TypeNode*>& arg_types);

    node::NodeManager* nm_;
    udf::UDFLibrary* library_;
    const vm::SchemaSourceList& input_schemas_;

    vm::SchemasContext schemas_context_;
    node::ExprAnalysisContext analysis_context_;

    std::unordered_map<node::ExprNode*, node::ExprNode*> cache_;
};

}  // namespace passes
}  // namespace fesql
#endif  // SRC_PASSES_RESOLVE_FN_AND_ATTRS_H_
