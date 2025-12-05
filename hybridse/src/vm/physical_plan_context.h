/**
 * Copyright 2021 4paradigm
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
#ifndef HYBRIDSE_SRC_VM_PHYSICAL_PLAN_CONTEXT_H_
#define HYBRIDSE_SRC_VM_PHYSICAL_PLAN_CONTEXT_H_

#include <map>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include "base/fe_status.h"
#include "node/node_manager.h"
#include "udf/udf_library.h"
#include "vm/engine_context.h"

namespace hybridse {
namespace vm {

using hybridse::base::Status;

class PhysicalPlanContext {
 public:
    PhysicalPlanContext(node::NodeManager* nm, const udf::UdfLibrary* library, const std::string& db,
                        const std::shared_ptr<Catalog>& catalog, const codec::Schema* parameter_types,
                        bool enable_expr_opt, const std::unordered_map<std::string, std::string>* options = nullptr,
                        std::shared_ptr<IndexHintHandler> index_hints = nullptr)
        : nm_(nm),
          library_(library),
          db_(db),
          catalog_(catalog),
          parameter_types_(parameter_types),
          enable_expr_opt_(enable_expr_opt),
          options_(options),
          index_hints_(index_hints) {}
    ~PhysicalPlanContext() {}

    /**
     * Get unique column id by named column from table.
     */
    Status GetSourceID(const std::string& db_name,
                       const std::string& table_name,
                       const std::string& column_name, size_t* column_id);
    Status GetRequestSourceID(const std::string& db_name,
                              const std::string& table_name,
                              const std::string& column_name,
                              size_t* column_id);

    const auto& GetRequestColumnIDMapping() const {
        return request_column_id_to_source_id_;
    }

    /**
     * Get new unique column id computed by expression.
     */
    size_t GetNewColumnID();

    /**
     * Generate function def for expression list.
     */
    Status InitFnDef(const ColumnProjects& projects,
                     const SchemasContext* schemas_ctx, bool is_row_project,
                     FnComponent* fn_component);
    Status InitFnDef(const node::ExprListNode* projects,
                     const SchemasContext* schemas_ctx, bool is_row_project,
                     FnComponent* fn_component);
    template <typename Op, typename... Args>
    Status CreateOp(Op** result_op, Args&&... args) {
        Op* op = new Op(std::forward<Args>(args)...);
        auto status = op->InitSchema(this);
        if (!status.isOK()) {
            delete op;
            return status;
        }
        op->FinishSchema();
        *result_op = nm_->RegisterNode(op);
        return Status::OK();
    }

    template <typename Op>
    Status WithNewChildren(Op* input,
                           const std::vector<PhysicalOpNode*>& children,
                           Op** out) {
        PhysicalOpNode* new_op = nullptr;
        CHECK_STATUS(input->WithNewChildren(nm_, children, &new_op));
        auto status = new_op->InitSchema(this);
        if (!status.isOK()) {
            return status;
        }
        new_op->FinishSchema();
        new_op->SetLimitCnt(input->GetLimitCnt());
        *out = dynamic_cast<Op*>(new_op);
        return Status::OK();
    }

    template <typename Op>
    Status WithNewChild(Op* input, size_t idx, Op* new_child, Op** out) {
        std::vector<PhysicalOpNode*> children;
        for (size_t i = 0; i < input->producers().size(); ++i) {
            if (i == idx) {
                children.push_back(new_child);
            } else {
                children.push_back(input->GetProducer(i));
            }
        }
        return WithNewChildren(input, children, out);
    }

    const std::unordered_map<std::string, std::string>* GetOptions() const {
        return options_;
    }

    node::NodeManager* node_manager() const { return nm_; }
    const udf::UdfLibrary* library() const { return library_; }
    const std::string& db() { return db_; }
    std::shared_ptr<Catalog> catalog() { return catalog_; }
    const codec::Schema* parameter_types() const { return parameter_types_; }
    // temp dict for legacy udf
    // TODO(xxx): support udf type infer
    std::map<std::string, type::Type> legacy_udf_dict_;

    std::shared_ptr<IndexHintHandler> index_hints() { return index_hints_; }

 private:
    node::NodeManager* nm_;
    const udf::UdfLibrary* library_;
    const std::string db_;
    std::shared_ptr<Catalog> catalog_;

    Status InitializeSourceIdMappings(const std::string& db_name, const std::string& table_name);

    // manage column unique ids
    size_t column_id_counter_ = 1;
    std::map<std::string, std::map<std::string, std::map<std::string, size_t>>> db_table_column_id_map_;
    // TODO(xxx): pass in request name
    std::map<std::string, std::map<std::string, size_t>> request_column_id_map_;

    // request column id -> source table column id
    std::map<size_t, size_t> request_column_id_to_source_id_;

    // source column id -> column info
    std::map<size_t, std::pair<std::string, std::string>> column_id_to_name_;

    // parameters types
    const codec::Schema* parameter_types_;
    // unique id counter for codegen function name id
    size_t codegen_func_id_counter_ = 0;

    bool enable_expr_opt_ = false;
    const std::unordered_map<std::string, std::string>* options_ = nullptr;

    // possible index suggestion to optimize the query performance
    // not standardized, maybe Diagnostic info ?
    std::shared_ptr<IndexHintHandler> index_hints_;
};
}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_PHYSICAL_PLAN_CONTEXT_H_
