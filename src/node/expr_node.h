/*
 * parser/node.h
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
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

#ifndef SRC_NODE_EXPR_NODE_H_
#define SRC_NODE_EXPR_NODE_H_

#include "base/fe_status.h"

// fwd
namespace fesql::vm {
class SchemasContext;
}

namespace fesql {
namespace node {

class NodeManager;

using fesql::base::Status;

class ExprAnalysisContext {
 public:
    ExprAnalysisContext(node::NodeManager* nm,
                        const vm::SchemasContext* schemas_context)
        : nm_(nm), schemas_context_(schemas_context) {}

    node::NodeManager* node_manager() { return nm_; }
    const vm::SchemasContext* schemas_context() const {
        return schemas_context_;
    }

 private:
    node::NodeManager* nm_;
    const vm::SchemasContext* schemas_context_;
};

}  // namespace node
}  // namespace fesql
#endif  // SRC_NODE_EXPR_NODE_H_
