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

#ifndef INCLUDE_NODE_EXPR_NODE_H_
#define INCLUDE_NODE_EXPR_NODE_H_

#include <string>
#include <vector>

#include "base/fe_status.h"

// fwd
namespace hybridse::vm {
class SchemasContext;
}
namespace hybridse::udf {
class UdfLibrary;
}

namespace hybridse {
namespace node {

class NodeManager;
class ExprNode;
class TypeNode;

using hybridse::base::Status;

/**
 * Summarize runtime attribute of expression
 */
class ExprAttrNode {
 public:
    ExprAttrNode(const node::TypeNode* dtype, bool nullable)
        : type_(dtype), nullable_(nullable) {}

    const node::TypeNode* type() const { return type_; }
    bool nullable() const { return nullable_; }

    void SetType(const node::TypeNode* dtype) { type_ = dtype; }
    void SetNullable(bool flag) { nullable_ = flag; }

 private:
    const node::TypeNode* type_;
    bool nullable_;
};

class ExprAnalysisContext {
 public:
    ExprAnalysisContext(node::NodeManager* nm, const udf::UdfLibrary* library,
                        const vm::SchemasContext* schemas_context)
        : nm_(nm), library_(library), schemas_context_(schemas_context) {}

    node::NodeManager* node_manager() { return nm_; }

    const udf::UdfLibrary* library() const { return library_; }

    const vm::SchemasContext* schemas_context() const {
        return schemas_context_;
    }

    Status InferAsUdf(node::ExprNode* expr, const std::string& name);

 private:
    node::NodeManager* nm_;
    const udf::UdfLibrary* library_;
    const vm::SchemasContext* schemas_context_;
};

}  // namespace node
}  // namespace hybridse
#endif  // INCLUDE_NODE_EXPR_NODE_H_
