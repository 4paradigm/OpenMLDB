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

#include "vm/physical_op.h"

#include <set>

#include "absl/container/flat_hash_map.h"
#include "absl/strings/substitute.h"
#include "passes/physical/physical_pass.h"

namespace hybridse {
namespace vm {

using hybridse::base::Status;

const char INDENT[] = "  ";

static absl::flat_hash_map<PhysicalOpType, absl::string_view> CreatePhysicalOpTypeNamesMap() {
    absl::flat_hash_map<PhysicalOpType, absl::string_view> map = {
        {kPhysicalOpDataProvider, "DATA_PROVIDER"},
        {kPhysicalOpGroupBy, "GROUP_BY"},
        {kPhysicalOpSortBy, "SORT_BY"},
        {kPhysicalOpFilter, "FILTER_BY"},
        {kPhysicalOpProject, "PROJECT"},
        {kPhysicalOpSimpleProject, "SIMPLE_PROJECT"},
        {kPhysicalOpConstProject, "CONST_PROJECT"},
        {kPhysicalOpAggregate, "AGGREGATE"},
        {kPhysicalOpLimit, "LIMIT"},
        {kPhysicalOpRename, "RENAME"},
        {kPhysicalOpDistinct, "DISTINCT"},
        {kPhysicalOpWindow, "WINDOW"},
        {kPhysicalOpJoin, "JOIN"},
        {kPhysicalOpSetOperation, "SET_OPERATION"},
        {kPhysicalOpPostRequestUnion, "POST_REQUEST_UNION"},
        {kPhysicalOpRequestUnion, "REQUEST_UNION"},
        {kPhysicalOpRequestAggUnion, "REQUEST_AGG_UNION"},
        {kPhysicalOpRequestJoin, "REQUEST_JOIN"},
        {kPhysicalOpIndexSeek, "INDEX_SEEK"},
        {kPhysicalOpLoadData, "LOAD_DATA"},
        {kPhysicalOpDelete, "DELETE"},
        {kPhysicalOpSelectInto, "SELECT_INTO"},
        {kPhysicalOpRequestGroup, "REQUEST_GROUP"},
        {kPhysicalOpRequestGroupAndSort, "REQUEST_GROUP__SORT"},
        {kPhysicalOpInsert, "INSERT"},
        {kPhysicalCreateTable, "CREATE_TABLE"},
    };
    for (auto kind = 0; kind < kPhysicalOpLast; ++kind) {
        DCHECK(map.find(static_cast<PhysicalOpType>(kind)) != map.end());
    }
    return map;
}

static const absl::flat_hash_map<PhysicalOpType, absl::string_view>& GetPhysicalOpNamesMap() {
  static const absl::flat_hash_map<PhysicalOpType, std::string_view>& map = *new auto(CreatePhysicalOpTypeNamesMap());
  return map;
}

absl::string_view PhysicalOpTypeName(PhysicalOpType type) {
    auto& map = GetPhysicalOpNamesMap();
    auto it = map.find(type);
    if (it != map.end()) {
        return it->second;
    }
    return "UNKNOWN";
}

void printOptionsMap(std::ostream &output, const node::OptionsMap* value, const std::string_view item_name) {
    output << ", " << item_name << "=";
    if (value == nullptr || value->empty()) {
        output << "<nil>";
    } else {
        output << "(";
        for (auto it = value->begin(); it != value->end(); ++it) {
            output << it->first << ":" << it->second->GetExprString() << ",";
        }
        output << ")";
    }
}

template<typename T>
void PrintOptional(std::ostream& output, const absl::string_view key_name, const std::optional<T>& val) {
    if (val.has_value()) {
        output << ", " << key_name << "=" << val.value();
    }
}

void PhysicalOpNode::Print(std::ostream& output, const std::string& tab) const {
    output << tab << PhysicalOpTypeName(type_);
}

bool PhysicalOpNode::IsSameSchema(const codec::Schema* schema, const codec::Schema* exp_schema) {
    if (schema->size() != exp_schema->size()) {
        LOG(WARNING) << "Schemas size aren't consistent: "
                     << "expect size " << exp_schema->size() << ", real size " << schema->size();
        return false;
    }
    for (int i = 0; i < schema->size(); i++) {
        if (schema->Get(i).name() != exp_schema->Get(i).name()) {
            LOG(WARNING) << "Schemas aren't consistent:\n"
                         << exp_schema->Get(i).DebugString() << "vs:\n"
                         << schema->Get(i).DebugString();
            return false;
        }
        if (schema->Get(i).type() != exp_schema->Get(i).type()) {
            LOG(WARNING) << "Schemas aren't consistent:\n"
                         << exp_schema->Get(i).DebugString() << "vs:\n"
                         << schema->Get(i).DebugString();
            return false;
        }
    }
    return true;
}

base::Status PhysicalOpNode::SchemaStartWith(const vm::Schema& lhs, const vm::Schema& rhs) const {
    CHECK_TRUE(lhs.size() >= rhs.size(), common::kPlanError, "lhs size less than rhs");

    for (int i = 0; i < rhs.size(); ++i) {
        CHECK_TRUE(lhs.Get(i).name() == rhs.Get(i).name() && lhs.Get(i).type() == rhs.Get(i).type(), common::kPlanError,
                   absl::Substitute("$0th column inconsistent:\n$1 vs\n$2", i, lhs.Get(i).DebugString(),
                                    rhs.Get(i).DebugString()));
    }
    return base::Status::OK();
}

void PhysicalOpNode::Print() const { this->Print(std::cout, "    "); }

void PhysicalOpNode::PrintChildren(std::ostream& output, const std::string& tab) const {
    for (size_t i = 0; i < producers_.size(); ++i) {
        producers_[i]->Print(output, tab + INDENT);
        if (i + i < producers_.size()) {
            output << "\n";
        }
    }
}
void PhysicalUnaryNode::PrintChildren(std::ostream& output, const std::string& tab) const {
    if (producers_.empty() || nullptr == producers_[0]) {
        LOG(WARNING) << "empty producers";
        return;
    }
    producers_[0]->Print(output, tab + INDENT);
}
void PhysicalUnaryNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    if (limit_cnt_ .has_value()) {
        output << "(limit=" << limit_cnt_.value() << ")";
    }
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalUnaryNode::InitSchema(PhysicalPlanContext* ctx) {
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.Merge(0, GetProducer(0)->schemas_ctx());
    return Status::OK();
}

void PhysicalBinaryNode::PrintChildren(std::ostream& output, const std::string& tab) const {
    if (2 != producers_.size() || nullptr == producers_[0] || nullptr == producers_[1]) {
        LOG(WARNING) << "fail to print children";
        return;
    }
    producers_[0]->Print(output, tab + INDENT);
    output << "\n";
    producers_[1]->Print(output, tab + INDENT);
}
void PhysicalBinaryNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalTableProviderNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(table=" << table_handler_->GetName() << ")";
}

void PhysicalRequestProviderNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(request=" << table_handler_->GetName() << ")";
}

void PhysicalRequestProviderNodeWithCommonColumn::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(request=" << table_handler_->GetName() << ", common_column_indices=(";
    size_t i = 0;
    for (size_t column_idx : common_column_indices_) {
        output << column_idx;
        if (i < common_column_indices_.size() - 1) {
            output << ", ";
        }
        i += 1;
    }
    output << "))";
}

void PhysicalPartitionProviderNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << DataProviderTypeName(provider_type_) << ", table=" << table_handler_->GetName()
           << ", index=" << index_name_ << ")";
}

Status PhysicalGroupNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                          PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    std::vector<const node::ExprNode*> depend_columns;
    group_.ResolvedRelatedColumns(&depend_columns);

    auto new_group_op = nm->RegisterNode(new PhysicalGroupNode(children[0], group_.keys()));

    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        CHECK_STATUS(
            BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer));
    }
    CHECK_STATUS(group_.ReplaceExpr(replacer, nm, &new_group_op->group_));
    *out = new_group_op;
    return Status::OK();
}

void PhysicalGroupNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "("
           << "group_" << group_.ToString() << ")";
    output << "\n";
    PrintChildren(output, tab);
}
PhysicalGroupNode* PhysicalGroupNode::CastFrom(PhysicalOpNode* node) { return dynamic_cast<PhysicalGroupNode*>(node); }

void ColumnProjects::Add(const std::string& name, const node::ExprNode* expr, const node::FrameNode* frame) {
    if (name.empty()) {
        LOG(WARNING) << "Append empty column name into projects";
        return;
    } else if (expr == nullptr) {
        LOG(WARNING) << "Column project expr is null";
        return;
    }
    names_.push_back(name);
    exprs_.push_back(expr);
    frames_.push_back(frame);
}

void ColumnProjects::Clear() {
    names_.clear();
    exprs_.clear();
    frames_.clear();
    primary_frame_ = nullptr;
}

Status ColumnProjects::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm,
                                   ColumnProjects* out) const {
    out->Clear();
    for (size_t i = 0; i < this->size(); ++i) {
        auto expr = GetExpr(i)->ShadowCopy(nm);
        node::ExprNode* new_expr = nullptr;
        CHECK_STATUS(replacer.Replace(expr, &new_expr));
        out->Add(GetName(i), new_expr, GetFrame(i));
        out->SetPrimaryFrame(GetPrimaryFrame());
    }
    return Status::OK();
}

void PhysicalProjectNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

/**
 * Resolve unique column id list from project expressions.
 * (1) If expr is column reference:
 *    - Resolve column id from input schemas context.
 * (2) Else:
 *    - Allocate new column id since it a newly computed column.
 *
 *  `schemas_ctx` used to resolve column and `plan_ctx` use to alloc unique id for non-column-reference column
 */
static Status InitProjectSchemaSource(const ColumnProjects& projects, const SchemasContext* depend_schemas_ctx,
                                      PhysicalPlanContext* plan_ctx, SchemaSource* project_source) {
    const FnInfo& fn_info = projects.fn_info();
    CHECK_TRUE(fn_info.IsValid(), common::kPlanError, "Project node's function info is not valid");
    project_source->SetSchema(fn_info.fn_schema());

    for (size_t i = 0; i < projects.size(); ++i) {
        // set column id and source info
        size_t column_id;
        const node::ExprNode* expr = projects.GetExpr(i);
        CHECK_TRUE(expr != nullptr, common::kPlanError);
        CHECK_TRUE(expr->GetExprType() != node::kExprAll, common::kPlanError,
                   "* should be extend before generate projects");
        if (expr->GetExprType() == node::kExprColumnRef) {
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(expr);
            CHECK_STATUS(depend_schemas_ctx->ResolveColumnID(col_ref->GetDBName(), col_ref->GetRelationName(),
                                                      col_ref->GetColumnName(), &column_id));
            project_source->SetColumnID(i, column_id);
            project_source->SetSource(i, 0, column_id);
        } else if (expr->GetExprType() == node::kExprColumnId) {
            auto col_ref = dynamic_cast<const node::ColumnIdNode*>(expr);
            size_t column_id = col_ref->GetColumnID();
            project_source->SetColumnID(i, column_id);
            project_source->SetSource(i, 0, column_id);
        } else {
            column_id = plan_ctx->GetNewColumnID();
            project_source->SetColumnID(i, column_id);
            project_source->SetNonSource(i);
        }
    }
    return Status::OK();
}

Status PhysicalProjectNode::InitSchema(PhysicalPlanContext* ctx) {
    auto input_schemas_ctx = GetProducer(0)->schemas_ctx();
    bool is_row_project = !IsAggProjectType(project_type_);

    // init project fn
    CHECK_STATUS(ctx->InitFnDef(project_, input_schemas_ctx, is_row_project, &project_),
                 "Fail to initialize function info of project node");

    // init project schema
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    SchemaSource* project_source = schemas_ctx_.AddSource();
    return InitProjectSchemaSource(project_, input_schemas_ctx, ctx, project_source);
}

Status PhysicalConstProjectNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                 PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 0, common::kPlanError);
    *out = nm->RegisterNode(new PhysicalConstProjectNode(project_));
    return Status::OK();
}

Status PhysicalConstProjectNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_STATUS(ctx->InitFnDef(project_, &empty_schemas_ctx_, true, &project_),
                 "Fail to initialize function def of const project node");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    SchemaSource* project_source = schemas_ctx_.AddSource();

    CHECK_STATUS(InitProjectSchemaSource(project_, &empty_schemas_ctx_, ctx, project_source));
    return Status::OK();
}

int PhysicalSimpleProjectNode::GetSelectSourceIndex() const {
    int cur_schema_idx = -1;
    auto input_schemas_ctx = GetProducer(0)->schemas_ctx();
    for (size_t i = 0; i < project_.size(); ++i) {
        Status status;
        size_t schema_idx;
        size_t col_idx;
        auto expr = project_.GetExpr(i);
        switch (expr->GetExprType()) {
            case node::kExprColumnId: {
                status = input_schemas_ctx->ResolveColumnIndexByID(
                    dynamic_cast<const node::ColumnIdNode*>(expr)->GetColumnID(), &schema_idx, &col_idx);
                break;
            }
            case node::kExprColumnRef: {
                status = input_schemas_ctx->ResolveColumnRefIndex(dynamic_cast<const node::ColumnRefNode*>(expr),
                                                                  &schema_idx, &col_idx);
                break;
            }
            default:
                return -1;
        }
        if (!status.isOK()) {
            return -1;
        }
        if (project_.size() != input_schemas_ctx->GetSchemaSource(schema_idx)->size()) {
            return -1;
        }
        if (i == 0) {
            cur_schema_idx = schema_idx;
        } else if (cur_schema_idx != static_cast<int>(schema_idx)) {
            return -1;
        }
        if (col_idx != i) {
            return -1;
        }
    }
    return cur_schema_idx;
}

Status PhysicalSimpleProjectNode::InitSchema(PhysicalPlanContext* ctx) {
    auto input_schemas_ctx = GetProducer(0)->schemas_ctx();
    // init project fn
    CHECK_STATUS(ctx->InitFnDef(project_, input_schemas_ctx, true, &project_),
                 "Fail to initialize function info of simple project node");

    // init project schema
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    SchemaSource* project_source = schemas_ctx_.AddSource();
    return InitProjectSchemaSource(project_, input_schemas_ctx, ctx, project_source);
}

Status PhysicalSimpleProjectNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                  PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    passes::ExprReplacer replacer;
    for (size_t i = 0; i < project_.size(); ++i) {
        auto expr = project_.GetExpr(i);
        CHECK_STATUS(
            BuildColumnReplacement(expr, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer));
    }
    ColumnProjects new_projects;
    CHECK_STATUS(project_.ReplaceExpr(replacer, nm, &new_projects));
    *out = nm->RegisterNode(new PhysicalSimpleProjectNode(children[0], new_projects));
    return Status::OK();
}

Status PhysicalProjectNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                            PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    // Renew Expressions in SELECT list with new child's schema context
    ColumnProjects new_projects;
    {
        passes::ExprReplacer replacer;
        for (size_t i = 0; i < project_.size(); ++i) {
            auto expr = project_.GetExpr(i);
            CHECK_STATUS(
                BuildColumnReplacement(expr, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer));
        }
        CHECK_STATUS(project_.ReplaceExpr(replacer, nm, &new_projects));
    }
    PhysicalProjectNode* op;
    switch (project_type_) {
        case kRowProject: {
            op = new PhysicalRowProjectNode(children[0], new_projects);
            break;
        }
        case kTableProject: {
            op = new PhysicalTableProjectNode(children[0], new_projects);
            break;
        }
        case kAggregation: {
            // Renew Having Condition
            ConditionFilter new_having_condition;
            {
                auto& having_condition =
                    dynamic_cast<PhysicalAggregationNode*>(this)->having_condition_;
                std::vector<const node::ExprNode*> having_condition_depend_columns;
                having_condition.ResolvedRelatedColumns(&having_condition_depend_columns);
                passes::ExprReplacer having_replacer;
                for (auto col_expr : having_condition_depend_columns) {
                    CHECK_STATUS(BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(),
                                                        children[0]->schemas_ctx(), nm, &having_replacer));
                }
                CHECK_STATUS(having_condition.ReplaceExpr(having_replacer, nm, &new_having_condition));
            }

            auto* agg_prj = new PhysicalAggregationNode(children[0], new_projects, new_having_condition.condition());
            op = agg_prj;
            break;
        }
        case kGroupAggregation: {
            // Renew Having Condition
            ConditionFilter new_having_condition;
            {
                auto& having_condition =
                    dynamic_cast<PhysicalGroupAggrerationNode*>(this)->having_condition_;
                std::vector<const node::ExprNode*> having_condition_depend_columns;
                having_condition.ResolvedRelatedColumns(&having_condition_depend_columns);
                passes::ExprReplacer having_replacer;
                for (auto col_expr : having_condition_depend_columns) {
                    CHECK_STATUS(BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(),
                                                        children[0]->schemas_ctx(), nm, &having_replacer));
                }
                CHECK_STATUS(having_condition.ReplaceExpr(having_replacer, nm, &new_having_condition));
            }

            Key new_group;
            // Renew Group Keys
            {
                auto& group = dynamic_cast<PhysicalGroupAggrerationNode*>(this)->group_;
                std::vector<const node::ExprNode*> group_depend_columns;
                group.ResolvedRelatedColumns(&group_depend_columns);
                passes::ExprReplacer group_replacer;
                for (auto col_expr : group_depend_columns) {
                    CHECK_STATUS(BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(),
                                                        children[0]->schemas_ctx(), nm, &group_replacer));
                }
                CHECK_STATUS(group.ReplaceExpr(group_replacer, nm, &new_group));
            }
            op = new PhysicalGroupAggrerationNode(
                children[0], new_projects, new_having_condition.condition(), new_group.keys());
            break;
        }
        default:
            return Status(common::kPlanError, "Unknown project type: " + ProjectTypeName(project_type_));
    }
    *out = nm->RegisterNode(op);
    return Status::OK();
}

PhysicalProjectNode* PhysicalProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalProjectNode*>(node);
}

PhysicalRowProjectNode* PhysicalRowProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRowProjectNode*>(node);
}

PhysicalTableProjectNode* PhysicalTableProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalTableProjectNode*>(node);
}

PhysicalConstProjectNode* PhysicalConstProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalConstProjectNode*>(node);
}

PhysicalSimpleProjectNode* PhysicalSimpleProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalSimpleProjectNode*>(node);
}

PhysicalSetOperationNode* PhysicalSetOperationNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalSetOperationNode*>(node);
}

PhysicalPostRequestUnionNode* PhysicalPostRequestUnionNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalPostRequestUnionNode*>(node);
}

PhysicalSortNode* PhysicalSortNode::CastFrom(PhysicalOpNode* node) { return dynamic_cast<PhysicalSortNode*>(node); }

PhysicalFilterNode* PhysicalFilterNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalFilterNode*>(node);
}

PhysicalLimitNode* PhysicalLimitNode::CastFrom(PhysicalOpNode* node) { return dynamic_cast<PhysicalLimitNode*>(node); }
PhysicalRenameNode* PhysicalRenameNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRenameNode*>(node);
}
void PhysicalConstProjectNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
}
void PhysicalSimpleProjectNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    output << "sources=(";
    for (size_t i = 0; i < project_.size(); ++i) {
        auto expr = project_.GetExpr(i);
        std::string expr_name = expr->GetExprString();
        std::string col_name = project_.GetName(i);
        output << expr_name;
        auto col_ref = dynamic_cast<const node::ColumnRefNode*>(expr);
        if ((col_ref == nullptr && expr_name != col_name) ||
            (col_ref != nullptr && col_ref->GetColumnName() != col_name)) {
            output << " -> " << col_name;
        }
        if (i < project_.size() - 1) {
            output << ", ";
        }
    }
    output << ")";
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";

    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalWindowAggrerationNode::WithNewChildren(node::NodeManager* nm,
                                                      const std::vector<PhysicalOpNode*>& children,
                                                      PhysicalOpNode** out) {
    return Status(common::kPlanError, "Not supported");
}

PhysicalWindowAggrerationNode* PhysicalWindowAggrerationNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalWindowAggrerationNode*>(node);
}


void PhysicalAggregationNode::Print(std::ostream& output,
                                         const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    if (having_condition_.ValidCondition()) {
        output << ", having_" << having_condition_.ToString();
    }
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalReduceAggregationNode::InitSchema(PhysicalPlanContext* ctx) {
    // init reduce project schema
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    SchemaSource* project_source = schemas_ctx_.AddSource();
    project_source->SetSchema(orig_aggr_->GetOutputSchema());
    for (size_t i = 0; i < project_.size(); i++) {
        auto column_id = ctx->GetNewColumnID();
        project_source->SetColumnID(i, column_id);
        project_source->SetNonSource(i);
    }
    return Status();
}

void PhysicalReduceAggregationNode::Print(std::ostream& output,
                                          const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    output << ": ";
    for (size_t i = 0; i < project_.size(); i++) {
        output << project_.GetExpr(i)->GetExprString();
        if (project_.GetFrame(i)) {
            output << " (" << project_.GetFrame(i)->GetExprString() << ")";
        }
        if (i < project_.size() - 1) output << ", ";
    }
    if (having_condition_.ValidCondition()) {
        output << ", having_" << having_condition_.ToString();
    }
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalGroupAggrerationNode::Print(std::ostream& output,
                                         const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_) << ", "
           << "group_" << group_.ToString();
    if (having_condition_.ValidCondition()) {
        output << ", having_" << having_condition_.ToString();
    }
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

PhysicalGroupAggrerationNode* PhysicalGroupAggrerationNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalGroupAggrerationNode*>(node);
}

Status Key::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm, Key* out) const {
    if (keys_ == nullptr) {
        return Status::OK();
    }
    auto origin_key = keys_->ShadowCopy(nm);
    node::ExprNode* new_key = nullptr;
    CHECK_STATUS(replacer.Replace(origin_key, &new_key));
    out->keys_ = dynamic_cast<node::ExprListNode*>(new_key);
    return Status::OK();
}

Status Sort::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm, Sort* out) const {
    if (orders_ == nullptr) {
        out->orders_ = nullptr;
        return Status::OK();
    }
    auto new_order = orders_->ShadowCopy(nm);
    if (new_order->order_expressions_ == nullptr) {
        out->orders_ = new_order;
        return Status::OK();
    }
    auto origin_key = orders_->order_expressions_->ShadowCopy(nm);
    node::ExprNode* new_key = nullptr;
    CHECK_STATUS(replacer.Replace(origin_key, &new_key));
    new_order->order_expressions_ = dynamic_cast<node::ExprListNode*>(new_key);
    out->orders_ = new_order;
    return Status::OK();
}

Status Filter::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm, Filter* out) const {
    CHECK_STATUS(condition_.ReplaceExpr(replacer, nm, &out->condition_));
    CHECK_STATUS(left_key_.ReplaceExpr(replacer, nm, &out->left_key_));
    CHECK_STATUS(right_key_.ReplaceExpr(replacer, nm, &out->right_key_));
    CHECK_STATUS(index_key_.ReplaceExpr(replacer, nm, &out->index_key_));
    return Status::OK();
}

Status ConditionFilter::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm,
                                    ConditionFilter* out) const {
    if (condition_ == nullptr) {
        return Status::OK();
    }
    auto origin_condition = condition_->ShadowCopy(nm);
    node::ExprNode* new_condition = nullptr;
    CHECK_STATUS(replacer.Replace(origin_condition, &new_condition));
    out->condition_ = new_condition;
    return Status::OK();
}

Status Join::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm, Join* out) const {
    CHECK_STATUS(this->Filter::ReplaceExpr(replacer, nm, out));
    out->join_type_ = join_type_;
    CHECK_STATUS(right_sort_.ReplaceExpr(replacer, nm, &out->right_sort_));
    return Status::OK();
}

Status Range::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm, Range* out) const {
    if (range_key_ == nullptr) {
        return Status::OK();
    }
    auto origin_key = range_key_->ShadowCopy(nm);
    node::ExprNode* new_key = nullptr;
    CHECK_STATUS(replacer.Replace(origin_key, &new_key));
    out->range_key_ = new_key;
    out->frame_ = frame_;
    return Status::OK();
}

void Range::ResolvedRelatedColumns(std::vector<const node::ExprNode*>* columns) const {
    node::ColumnOfExpression(range_key_, columns);
}

void WindowOp::ResolvedRelatedColumns(std::vector<const node::ExprNode*>* columns) const {
    partition_.ResolvedRelatedColumns(columns);
    sort_.ResolvedRelatedColumns(columns);
    range_.ResolvedRelatedColumns(columns);
}

Status WindowOp::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm, WindowOp* out) const {
    CHECK_STATUS(partition_.ReplaceExpr(replacer, nm, &out->partition_));
    CHECK_STATUS(sort_.ReplaceExpr(replacer, nm, &out->sort_));
    CHECK_STATUS(range_.ReplaceExpr(replacer, nm, &out->range_));
    return Status::OK();
}

void RequestWindowOp::ResolvedRelatedColumns(std::vector<const node::ExprNode*>* columns) const {
    this->WindowOp::ResolvedRelatedColumns(columns);
    index_key_.ResolvedRelatedColumns(columns);
}

Status RequestWindowOp::ReplaceExpr(const passes::ExprReplacer& replacer, node::NodeManager* nm,
                                    RequestWindowOp* out) const {
    CHECK_STATUS(this->WindowOp::ReplaceExpr(replacer, nm, out));
    CHECK_STATUS(index_key_.ReplaceExpr(replacer, nm, &out->index_key_));
    return Status::OK();
}

void PhysicalWindowAggrerationNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    if (exclude_current_time()) {
        output << ", EXCLUDE_CURRENT_TIME";
    }
    if (exclude_current_row()) {
        output << ", EXCLUDE_CURRENT_ROW";
    }
    if (instance_not_in_window()) {
        output << ", INSTANCE_NOT_IN_WINDOW";
    }
    if (need_append_input()) {
        output << ", NEED_APPEND_INPUT";
    }
    PrintOptional(output, "limit", limit_cnt_);
    output << ")\n";

    output << tab << INDENT << "+-WINDOW(" << window_.ToString() << ")";

    if (!window_joins_.Empty()) {
        for (auto window_join : window_joins_.window_joins_) {
            output << "\n";
            output << tab << INDENT << "+-JOIN(" << window_join.second.ToString() << ")\n";
            window_join.first->Print(output, tab + INDENT + INDENT + INDENT);
        }
    }

    if (!window_unions_.Empty()) {
        for (auto window_union : window_unions_.window_unions_) {
            output << "\n";
            output << tab << INDENT << "+-UNION(" << window_union.second.ToString() << ")\n";
            window_union.first->Print(output, tab + INDENT + INDENT + INDENT);
        }
    }
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalWindowAggrerationNode::InitSchema(PhysicalPlanContext* ctx) {
    // output row as 'append row (if need_append_input) + window project rows'

    CHECK_STATUS(InitJoinList(ctx));
    auto input = GetProducer(0);
    const vm::SchemasContext* input_schemas_ctx;
    if (joined_op_list_.empty()) {
        input_schemas_ctx = input->schemas_ctx();
    } else {
        input_schemas_ctx = joined_op_list_.back()->schemas_ctx();
    }

    // init fn info
    bool is_row_project = !IsAggProjectType(project_type_);
    CHECK_STATUS(ctx->InitFnDef(project_, input_schemas_ctx, is_row_project, &project_),
                 "Fail to initialize function def of project node");

    // init output schema
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());

    // window agg may inherit input row
    if (need_append_input()) {
        schemas_ctx_.Merge(0, input->schemas_ctx());
    }

    auto project_source = schemas_ctx_.AddSource();
    CHECK_STATUS(InitProjectSchemaSource(project_, input_schemas_ctx, ctx, project_source));
    return Status::OK();
}

Status PhysicalWindowAggrerationNode::InitJoinList(PhysicalPlanContext* plan_ctx) {
    auto& window_joins = window_joins_.window_joins();
    if (window_joins.size() == joined_op_list_.size()) {
        return Status::OK();
    }
    joined_op_list_.clear();
    PhysicalOpNode* cur = this->GetProducer(0);
    for (auto& pair : window_joins) {
        PhysicalJoinNode* joined = nullptr;
        CHECK_STATUS(plan_ctx->CreateOp<PhysicalJoinNode>(&joined, cur, pair.first, pair.second));
        joined_op_list_.push_back(joined);
        cur = joined;
    }
    return Status::OK();
}

std::vector<PhysicalOpNode*> PhysicalWindowAggrerationNode::GetDependents() const {
    auto list = GetProducers();
    for (auto& [node, window] : window_unions_.window_unions_) {
        list.push_back(node);
    }
    return list;
}

bool PhysicalWindowAggrerationNode::AddWindowUnion(PhysicalOpNode* node) {
    if (nullptr == node) {
        LOG(WARNING) << "Fail to add window union : table is null";
        return false;
    }
    if (producers_.empty() || nullptr == producers_[0]) {
        LOG(WARNING) << "Fail to add window union : producer is empty or null";
        return false;
    }

    // verify producer and union source has the same schema, two situation considered:
    // 1. producer is window agg node, for batch mode, multiple window ops are serialized, where
    //    each producer window op outputs its producer row + project row. In this case, it expect
    //    producer schema starts with union schema
    // 2. otherwise, always expec producer schema equals union schema
    if (producers_[0]->GetOpType() == kPhysicalOpProject &&
        dynamic_cast<PhysicalProjectNode*>(producers_[0])->project_type_ == kWindowAggregation &&
        dynamic_cast<PhysicalWindowAggrerationNode*>(producers_[0])->need_append_input()) {
        auto s = SchemaStartWith(*producers_[0]->GetOutputSchema(), *node->GetOutputSchema());
        if (!s.isOK()) {
            LOG(WARNING) << s;
            return false;
        }
    } else {
        if (!IsSameSchema(node->GetOutputSchema(), producers_[0]->GetOutputSchema())) {
            LOG(WARNING) << "Union Table and window input schema aren't consistent";
            return false;
        }
    }
    window_unions_.AddWindowUnion(node, window_);
    WindowOp& window_union = window_unions_.window_unions_.back().second;
    fn_infos_.push_back(&window_union.partition_.fn_info());
    fn_infos_.push_back(&window_union.sort_.fn_info());
    fn_infos_.push_back(&window_union.range_.fn_info());
    return true;
}

void PhysicalJoinNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    if (output_right_only_) {
        output << "OUTPUT_RIGHT_ONLY, ";
    }
    if (join_.join_type() == node::kJoinTypeConcat) {
        output << "type=kJoinTypeConcat";
    } else {
        output << join_.ToString();
    }
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalJoinNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(2 == producers_.size() && nullptr != producers_[0] && nullptr != producers_[1], common::kPlanError,
               "InitSchema fail: producers size isn't 2 or left/right "
               "producer is null");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    if (!output_right_only_) {
        schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    }
    if (join_.join_type() == node::kJoinTypeConcat) {
        schemas_ctx_.Merge(1, producers_[1]->schemas_ctx());
    } else {
        schemas_ctx_.MergeWithNewID(1, producers_[1]->schemas_ctx(), ctx);
    }

    // join input schema context
    joined_schemas_ctx_.Clear();
    joined_schemas_ctx_.SetDefaultDBName(ctx->db());
    joined_schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    joined_schemas_ctx_.Merge(1, producers_[1]->schemas_ctx());
    joined_schemas_ctx_.Build();
    return Status::OK();
}

Status PhysicalJoinNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                         PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 2, common::kPlanError);
    std::vector<const node::ExprNode*> depend_columns;
    join_.ResolvedRelatedColumns(&depend_columns);

    auto new_join_op = new PhysicalJoinNode(children[0], children[1], join_, output_right_only_);

    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        Status status;
        status =
            BuildColumnReplacement(col_expr, this->joined_schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer);
        if (status.isOK()) {
            continue;
        }
        CHECK_STATUS(
            BuildColumnReplacement(col_expr, this->joined_schemas_ctx(), children[1]->schemas_ctx(), nm, &replacer));
    }
    CHECK_STATUS(join_.ReplaceExpr(replacer, nm, &new_join_op->join_));
    *out = nm->RegisterNode(new_join_op);
    return Status::OK();
}

PhysicalJoinNode* PhysicalJoinNode::CastFrom(PhysicalOpNode* node) { return dynamic_cast<PhysicalJoinNode*>(node); }

Status PhysicalSortNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                         PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    std::vector<const node::ExprNode*> depend_columns;
    sort_.ResolvedRelatedColumns(&depend_columns);

    auto new_sort_op = new PhysicalSortNode(children[0], sort_);

    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        CHECK_STATUS(
            BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer));
    }
    CHECK_STATUS(sort_.ReplaceExpr(replacer, nm, &new_sort_op->sort_));
    *out = nm->RegisterNode(new_sort_op);
    return Status::OK();
}

void PhysicalSortNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << sort_.ToString();
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalDistinctNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                             PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    *out = nm->RegisterNode(new PhysicalDistinctNode(children[0]));
    return Status::OK();
}

Status PhysicalLimitNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                          PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    auto new_limit_op = nm->RegisterNode(new PhysicalLimitNode(children[0], limit_cnt_.value()));
    new_limit_op->SetLimitOptimized(limit_optimized_);
    *out = new_limit_op;
    return Status::OK();
}

void PhysicalLimitNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(limit=" << (!limit_cnt_.has_value() ? "null" : std::to_string(limit_cnt_.value()))
           << (limit_optimized_ ? ", optimized" : "") << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalRenameNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                           PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    *out = nm->RegisterNode(new PhysicalRenameNode(children[0], name_));
    return Status::OK();
}

void PhysicalRenameNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(name=" << name_ << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalFilterNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                           PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 1, common::kPlanError);
    std::vector<const node::ExprNode*> depend_columns;
    filter_.ResolvedRelatedColumns(&depend_columns);

    auto new_filter_op = nm->RegisterNode(new PhysicalFilterNode(children[0], filter_.condition_.condition()));

    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        CHECK_STATUS(
            BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer));
    }
    CHECK_STATUS(filter_.ReplaceExpr(replacer, nm, &new_filter_op->filter_));
    *out = new_filter_op;
    return Status::OK();
}

void PhysicalFilterNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << filter_.ToString();
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

PhysicalDataProviderNode* PhysicalDataProviderNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalDataProviderNode*>(node);
}

const std::string& PhysicalDataProviderNode::GetName() const { return table_handler_->GetName(); }

Status PhysicalDataProviderNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(table_handler_ != nullptr, common::kPlanError, "InitSchema fail: table handler is null");
    const std::string db_name = table_handler_->GetDatabase();
    const std::string table_name = table_handler_->GetName();
    auto schema = table_handler_->GetSchema();
    CHECK_TRUE(schema != nullptr, common::kPlanError, "InitSchema fail: table schema of ", table_name, " is null");

    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.SetDBAndRelationName(db_name, table_name);
    auto table_source = schemas_ctx_.AddSource();

    // set table source
    table_source->SetSchema(schema);
    table_source->SetSourceDBAndTableName(db_name, table_name);
    for (auto i = 0; i < schema->size(); ++i) {
        size_t column_id;
        CHECK_STATUS(ctx->GetSourceID(db_name, table_name, schema->Get(i).name(), &column_id),
                     "Get source column id from table \"", table_name, "\" failed");
        table_source->SetColumnID(i, column_id);
    }
    return Status::OK();
}

bool PhysicalDataProviderNode::Equals(const PhysicalOpNode *other) const {
    if (other == nullptr) {
        return false;
    }

    if (other->GetOpType() != kConcreteNodeKind) {
        return false;
    }

    auto* rhs = dynamic_cast<const PhysicalDataProviderNode*>(other);
    return rhs != nullptr && GetDb() == rhs->GetDb() && GetName() == rhs->GetName();
}

Status PhysicalRequestProviderNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(table_handler_ != nullptr, common::kPlanError, "InitSchema fail: table handler is null");
    const std::string request_name = table_handler_->GetName();
    const std::string db_name = table_handler_->GetDatabase();
    auto schema = table_handler_->GetSchema();
    CHECK_TRUE(schema != nullptr, common::kPlanError, "InitSchema fail: table schema of", request_name, " is null");

    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.SetDBAndRelationName(db_name, request_name);
    auto request_source = schemas_ctx_.AddSource();

    // set request source
    request_source->SetSchema(schema);
    request_source->SetSourceDBAndTableName(db_name, request_name);
    for (auto i = 0; i < schema->size(); ++i) {
        size_t column_id;
        CHECK_STATUS(ctx->GetRequestSourceID(db_name, request_name, schema->Get(i).name(), &column_id),
                     "Get source column id from table \"", request_name, "\" failed");
        request_source->SetColumnID(i, column_id);
    }
    return Status::OK();
}

Status PhysicalRequestProviderNodeWithCommonColumn::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(table_handler_ != nullptr, common::kPlanError, "InitSchema fail: table handler is null");
    const std::string request_name = table_handler_->GetName();
    const std::string db_name = table_handler_->GetDatabase();
    auto schema = table_handler_->GetSchema();
    size_t schema_size = static_cast<size_t>(schema->size());
    CHECK_TRUE(schema != nullptr, common::kPlanError, "InitSchema fail: table schema of", request_name, " is null");

    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.SetDBAndRelationName(db_name, request_name);

    std::vector<size_t> column_ids(schema_size);
    for (size_t i = 0; i < schema_size; ++i) {
        CHECK_STATUS(ctx->GetRequestSourceID(db_name, request_name, schema->Get(i).name(), &column_ids[i]),
                     "Get column id from ", request_name, " failed");
    }

    bool share_common = common_column_indices_.size() > 0 && common_column_indices_.size() < schema_size;
    if (share_common) {
        auto common_source = schemas_ctx_.AddSource();
        auto non_common_source = schemas_ctx_.AddSource();
        common_source->SetSourceDBAndTableName(db_name, request_name);
        non_common_source->SetSourceDBAndTableName(db_name, request_name);

        common_schema_.Clear();
        non_common_schema_.Clear();
        for (size_t i = 0; i < schema_size; ++i) {
            if (common_column_indices_.find(i) != common_column_indices_.end()) {
                *common_schema_.Add() = schema->Get(i);
            } else {
                *non_common_schema_.Add() = schema->Get(i);
            }
        }
        common_source->SetSchema(&common_schema_);
        non_common_source->SetSchema(&non_common_schema_);

        size_t lsize = 0;
        size_t rsize = 0;
        for (size_t i = 0; i < schema_size; ++i) {
            if (common_column_indices_.find(i) != common_column_indices_.end()) {
                common_source->SetColumnID(lsize, column_ids[i]);
                lsize += 1;
            } else {
                non_common_source->SetColumnID(rsize, column_ids[i]);
                rsize += 1;
            }
        }
    } else {
        auto request_source = schemas_ctx_.AddSource();
        request_source->SetSourceDBAndTableName(db_name, request_name);
        request_source->SetSchema(schema);
        for (size_t i = 0; i < schema_size; ++i) {
            request_source->SetColumnID(i, column_ids[i]);
        }
    }
    return Status::OK();
}

Status PhysicalRequestProviderNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                    PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 0, common::kPlanError);
    *out = nm->RegisterNode(new PhysicalRequestProviderNode(table_handler_));
    return Status::OK();
}

Status PhysicalTableProviderNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                  PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 0, common::kPlanError);
    *out = nm->RegisterNode(new PhysicalTableProviderNode(table_handler_));
    return Status::OK();
}

Status PhysicalPartitionProviderNode::WithNewChildren(node::NodeManager* nm,
                                                      const std::vector<PhysicalOpNode*>& children,
                                                      PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 0, common::kPlanError);
    *out = nm->RegisterNode(new PhysicalPartitionProviderNode(this, index_name_));
    return Status::OK();
}

void PhysicalOpNode::PrintSchema() const { std::cout << SchemaToString("") << std::endl; }

std::string PhysicalOpNode::SchemaToString(const std::string& tab) const {
    std::stringstream ss;
    ss << tab << "[";
    if (!schemas_ctx_.GetName().empty()) {
        ss << "name=" << schemas_ctx_.GetName() << ", ";
    }
    ss << "type=" << PhysicalOpTypeName(type_);
    ss << ", sources=" << GetOutputSchemaSourceSize();
    ss << ", columns=" << GetOutputSchema()->size();
    ss << "]\n";

    for (size_t i = 0; i < GetOutputSchemaSourceSize(); ++i) {
        ss << tab << "{\n";
        const SchemaSource* schema_source = GetOutputSchemaSource(i);
        const auto* schema = schema_source->GetSchema();
        for (int32_t j = 0; j < schema->size(); j++) {
            ss << tab << "    ";
            const type::ColumnDef& column = schema->Get(j);
            ss << "#" << schema_source->GetColumnID(j) << " " << column.name() << " " << type::Type_Name(column.type());
            if (schema_source->IsSourceColumn(j)) {
                ss << " <- #" << schema_source->GetSourceColumnID(j) << " (from ["
                   << schema_source->GetSourceChildIdx(j) << "])";
            }
            ss << "\n";
        }
        ss << tab << "} ";
    }
    return ss.str();
}

std::vector<PhysicalOpNode*> PhysicalOpNode::GetDependents() const { return GetProducers(); }

Status PhysicalSetOperationNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty union");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.MergeWithNewID(0, producers_[0]->schemas_ctx(), ctx);
    return Status::OK();
}

Status PhysicalSetOperationNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                 PhysicalOpNode** out) {
    absl::Span<PhysicalOpNode* const> sp = absl::MakeSpan(children);
    *out = nm->RegisterNode(new PhysicalSetOperationNode(set_type_, sp, distinct_));
    return Status::OK();
}

void PhysicalSetOperationNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << node::SetOperatorName(set_type_, distinct_) << ")\n";
    PrintChildren(output, tab);
}

Status PhysicalPostRequestUnionNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty union");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.MergeWithNewID(0, producers_[0]->schemas_ctx(), ctx);
    return Status::OK();
}

Status PhysicalPostRequestUnionNode::WithNewChildren(node::NodeManager* nm,
                                                     const std::vector<PhysicalOpNode*>& children,
                                                     PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 2, common::kPlanError);

    Range new_request_ts;
    CHECK_STATUS(ReplaceComponentExpr(request_ts_, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm,
                                      &new_request_ts));
    *out = nm->RegisterNode(new PhysicalPostRequestUnionNode(children[0], children[1], new_request_ts));
    return Status::OK();
}

void PhysicalPostRequestUnionNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalRequestUnionNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                 PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 2, common::kPlanError);
    auto new_union_op = new PhysicalRequestUnionNode(children[0], children[1], window_, instance_not_in_window_,
                                                     exclude_current_time_, output_request_row_);
    std::vector<const node::ExprNode*> depend_columns;
    window_.ResolvedRelatedColumns(&depend_columns);
    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        CHECK_STATUS(
            BuildColumnReplacement(col_expr, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer));
    }
    CHECK_STATUS(window_.ReplaceExpr(replacer, nm, &new_union_op->window_));

    for (auto& pair : window_unions_.window_unions_) {
        new_union_op->AddWindowUnion(pair.first);
        auto window_ptr = &new_union_op->window_unions_.window_unions_.back().second;
        CHECK_STATUS(ReplaceComponentExpr(pair.second, GetProducer(0)->schemas_ctx(), children[0]->schemas_ctx(), nm,
                                          window_ptr));
    }
    *out = nm->RegisterNode(new_union_op);
    return Status::OK();
}

void PhysicalRequestUnionNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    if (!output_request_row_) {
        output << "EXCLUDE_REQUEST_ROW, ";
    }
    if (exclude_current_time_) {
        output << "EXCLUDE_CURRENT_TIME, ";
    }
    if (exclude_current_row()) {
        output << "EXCLUDE_CURRENT_ROW, ";
    }
    if (instance_not_in_window_) {
        output << "INSTANCE_NOT_IN_WINDOW, ";
    }
    output << window_.ToString() << ")";
    if (!window_unions_.Empty()) {
        for (auto window_union : window_unions_.window_unions_) {
            output << "\n";
            output << tab << INDENT << "+-UNION(" << window_union.second.ToString() << ")\n";
            window_union.first->Print(output, tab + INDENT + INDENT + INDENT);
        }
    }
    //    if (!window_joins_.Empty()) {
    //        for (auto window_join : window_joins_.window_joins_) {
    //            output << "\n";
    //            output << tab << INDENT << "+-JOIN("
    //                   << window_join.second.ToString() << ")\n";
    //            window_join.first->Print(output, tab + INDENT + INDENT +
    //            INDENT);
    //        }
    //    }
    output << "\n";
    PrintChildren(output, tab);
}

std::vector<PhysicalOpNode*> PhysicalRequestUnionNode::GetDependents() const {
    auto list = GetProducers();
    for (auto& [node, window] : window_unions().window_unions_) {
        list.push_back(node);
    }
    return list;
}

base::Status PhysicalRequestUnionNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty request union");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    if (output_request_row()) {
        schemas_ctx_.MergeWithNewID(0, producers_[0]->schemas_ctx(), ctx);
    } else {
        schemas_ctx_.Merge(0, producers_[1]->schemas_ctx());
    }
    return Status::OK();
}

void PhysicalRequestAggUnionNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    if (!output_request_row_) {
        output << "EXCLUDE_REQUEST_ROW, ";
    }
    if (exclude_current_time_) {
        output << "EXCLUDE_CURRENT_TIME, ";
    }
    output << window_.ToString() << ")";
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalRequestAggUnionNode::PrintChildren(std::ostream& output, const std::string& tab) const {
    if (3 != producers_.size() || nullptr == producers_[0] || nullptr == producers_[1] || nullptr == producers_[2]) {
        LOG(WARNING) << "fail to print PhysicalRequestAggUnionNode children";
        return;
    }
    producers_[0]->Print(output, tab + INDENT);
    for (size_t i = 1; i < producers_.size(); i++) {
        output << "\n";
        producers_[i]->Print(output, tab + INDENT);
    }
}

base::Status PhysicalRequestAggUnionNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty request union");
    schemas_ctx_.Clear();
    agg_schema_.Clear();

    schemas_ctx_.SetDefaultDBName(ctx->db());
    auto source = schemas_ctx_.AddSource();
    if (parent_schema_context_) {
        source->SetSchema(parent_schema_context_->GetOutputSchema());
    } else {
        auto column = agg_schema_.Add();
        column->set_type(::hybridse::type::kVarchar);
        column->set_name("agg_val");
        source->SetSchema(&agg_schema_);
    }

    source->SetColumnID(0, ctx->GetNewColumnID());
    source->SetNonSource(0);
    return Status::OK();
}

PhysicalRequestAggUnionNode* PhysicalRequestAggUnionNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRequestAggUnionNode*>(node);
}

base::Status PhysicalRenameNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty procedures");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    schemas_ctx_.SetDBAndRelationName(ctx->db(), name_);
    schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    return Status::OK();
}

PhysicalRequestUnionNode* PhysicalRequestUnionNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRequestUnionNode*>(node);
}

void PhysicalRequestJoinNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    if (output_right_only_) {
        output << "OUTPUT_RIGHT_ONLY, ";
    }
    if (join_.join_type() == node::kJoinTypeConcat) {
        output << "type=kJoinTypeConcat";
    } else {
        output << join_.ToString();
    }
    PrintOptional(output, "limit", limit_cnt_);
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

PhysicalRequestJoinNode* PhysicalRequestJoinNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRequestJoinNode*>(node);
}

Status PhysicalRequestJoinNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                                PhysicalOpNode** out) {
    CHECK_TRUE(children.size() == 2, common::kPlanError);
    std::vector<const node::ExprNode*> depend_columns;
    join_.ResolvedRelatedColumns(&depend_columns);

    auto new_join_op =
        nm->RegisterNode(new PhysicalRequestJoinNode(children[0], children[1], join_, output_right_only_));

    passes::ExprReplacer replacer;
    for (auto col_expr : depend_columns) {
        Status status;
        status =
            BuildColumnReplacement(col_expr, this->joined_schemas_ctx(), children[0]->schemas_ctx(), nm, &replacer);
        if (status.isOK()) {
            continue;
        }
        CHECK_STATUS(
            BuildColumnReplacement(col_expr, this->joined_schemas_ctx(), children[1]->schemas_ctx(), nm, &replacer));
    }
    CHECK_STATUS(join_.ReplaceExpr(replacer, nm, &new_join_op->join_));
    *out = new_join_op;
    return Status::OK();
}

Status PhysicalRequestJoinNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(2 == producers_.size() && nullptr != producers_[0] && nullptr != producers_[1], common::kPlanError,
               "InitSchema fail: producers size isn't 2 or left/right "
               "producer is null");
    schemas_ctx_.Clear();
    schemas_ctx_.SetDefaultDBName(ctx->db());
    if (!output_right_only_) {
        schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    }
    if (join_.join_type() == node::kJoinTypeConcat) {
        schemas_ctx_.Merge(1, producers_[1]->schemas_ctx());
    } else {
        schemas_ctx_.MergeWithNewID(1, producers_[1]->schemas_ctx(), ctx);
    }

    // join input schema context
    joined_schemas_ctx_.Clear();
    joined_schemas_ctx_.SetDefaultDBName(ctx->db());
    joined_schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    joined_schemas_ctx_.Merge(1, producers_[1]->schemas_ctx());
    joined_schemas_ctx_.Build();
    return Status::OK();
}

Status PhysicalSelectIntoNode::InitSchema(PhysicalPlanContext* ctx) { return Status::OK(); }
Status PhysicalSelectIntoNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                               PhysicalOpNode** out) {
    return {common::kPlanError, "no children"};
}

void PhysicalSelectIntoNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << "out_file=" << OutFile();

    if (options_) {
        printOptionsMap(output, options_.get(), "options");
    }
    if (config_options_) {
        printOptionsMap(output, config_options_.get(), "config_options");
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

PhysicalSelectIntoNode* PhysicalSelectIntoNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalSelectIntoNode*>(node);
}

Status PhysicalLoadDataNode::InitSchema(PhysicalPlanContext* ctx) { return Status::OK(); }
Status PhysicalLoadDataNode::WithNewChildren(node::NodeManager* nm, const std::vector<PhysicalOpNode*>& children,
                                             PhysicalOpNode** out) {
    return {common::kPlanError, "no children"};
}

void PhysicalLoadDataNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "("
           << "file=" << File() << ", db=" << Db() << ", table=" << Table();

    if (options_) {
        printOptionsMap(output, options_.get(), "options");
    }
    if (config_options_) {
        printOptionsMap(output, config_options_.get(), "config_options");
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalDeleteNode::Print(std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(target=" << node::DeleteTargetString(GetTarget()) << ", job_id=" << GetJobId() << ")";
}

void PhysicalInsertNode::Print(std::ostream &output, const std::string &tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(db=" << GetInsertStmt()->db_name_ << ", table=" << GetInsertStmt()->table_name_
           << ", is_all=" << (GetInsertStmt()->is_all_ ? "true" : "false") << ")";
}

void PhysicalCreateTableNode::Print(std::ostream &output, const std::string &tab) const {
    PhysicalOpNode::Print(output, tab);
}

PhysicalCreateTableNode* PhysicalCreateTableNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalCreateTableNode*>(node);
}

PhysicalLoadDataNode* PhysicalLoadDataNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalLoadDataNode*>(node);
}

Status BuildColumnReplacement(const node::ExprNode* expr, const SchemasContext* origin_schema,
                              const SchemasContext* rebase_schema, node::NodeManager* nm,
                              passes::ExprReplacer* replacer) {
    // Find all column expressions the expr depend on
    std::vector<const node::ExprNode*> origin_columns;
    CHECK_STATUS(origin_schema->ResolveExprDependentColumns(expr, &origin_columns));

    // Build possible replacement
    for (auto col_expr : origin_columns) {
        if (col_expr->GetExprType() == node::kExprColumnRef) {
            auto col_ref = dynamic_cast<const node::ColumnRefNode*>(col_expr);
            size_t origin_schema_idx;
            size_t origin_col_idx;
            CHECK_STATUS(origin_schema->ResolveColumnRefIndex(col_ref, &origin_schema_idx, &origin_col_idx));
            size_t column_id = origin_schema->GetSchemaSource(origin_schema_idx)->GetColumnID(origin_col_idx);

            size_t new_schema_idx;
            size_t new_col_idx;
            Status status = rebase_schema->ResolveColumnIndexByID(column_id, &new_schema_idx, &new_col_idx);

            // (1) the column is inherited with same column id
            if (status.isOK()) {
                replacer->AddReplacement(col_ref->GetRelationName(), col_ref->GetColumnName(),
                                         nm->MakeColumnIdNode(column_id));
                continue;
            }

            // (2) the column is with same name
            status = rebase_schema->ResolveColumnRefIndex(col_ref, &new_schema_idx, &new_col_idx);
            if (status.isOK()) {
                size_t new_column_id = rebase_schema->GetSchemaSource(new_schema_idx)->GetColumnID(new_col_idx);
                replacer->AddReplacement(col_ref->GetRelationName(), col_ref->GetColumnName(),
                                         nm->MakeColumnIdNode(new_column_id));
                continue;
            }

            // (3) pick the column at the same index
            size_t total_idx = origin_col_idx;
            for (size_t i = 0; i < origin_schema_idx; ++i) {
                total_idx += origin_schema->GetSchemaSource(i)->size();
            }
            bool index_is_valid = false;
            for (size_t i = 0; i < rebase_schema->GetSchemaSourceSize(); ++i) {
                auto source = rebase_schema->GetSchemaSource(i);
                if (total_idx < source->size()) {
                    auto col_id_node = nm->MakeColumnIdNode(source->GetColumnID(total_idx));
                    replacer->AddReplacement(col_ref->GetRelationName(), col_ref->GetColumnName(), col_id_node);
                    replacer->AddReplacement(column_id, col_id_node);
                    index_is_valid = true;
                    break;
                }
                total_idx -= source->size();
            }

            // (3) can not build replacement
            CHECK_TRUE(index_is_valid, common::kPlanError, "Build replacement failed: " + col_ref->GetExprString());

        } else if (col_expr->GetExprType() == node::kExprColumnId) {
            auto column_id = dynamic_cast<const node::ColumnIdNode*>(col_expr)->GetColumnID();
            size_t origin_schema_idx;
            size_t origin_col_idx;
            CHECK_STATUS(origin_schema->ResolveColumnIndexByID(column_id, &origin_schema_idx, &origin_col_idx));

            size_t new_schema_idx;
            size_t new_col_idx;
            Status status = rebase_schema->ResolveColumnIndexByID(column_id, &new_schema_idx, &new_col_idx);

            // (1) the column is inherited with same column id
            if (status.isOK()) {
                continue;
            }

            // (2) pick the column at the same index
            size_t total_idx = origin_col_idx;
            for (size_t i = 0; i < origin_schema_idx; ++i) {
                total_idx += origin_schema->GetSchemaSource(i)->size();
            }
            bool index_is_valid = false;
            for (size_t i = 0; i < rebase_schema->GetSchemaSourceSize(); ++i) {
                auto source = rebase_schema->GetSchemaSource(i);
                if (total_idx < source->size()) {
                    replacer->AddReplacement(column_id, nm->MakeColumnIdNode(source->GetColumnID(total_idx)));
                    index_is_valid = true;
                    break;
                }
                total_idx -= source->size();
            }

            // (3) can not build replacement
            CHECK_TRUE(index_is_valid, common::kPlanError, "Build replacement failed: " + col_expr->GetExprString());
        } else {
            return Status(common::kPlanError, "Invalid column expression type");
        }
    }
    return Status::OK();
}

absl::StatusOr<ColProducerTraceInfo> PhysicalOpNode::TraceColID(size_t col_id) const {
    size_t sc_idx;
    size_t col_idx;
    auto s = schemas_ctx()->ResolveColumnIndexByID(col_id, &sc_idx, &col_idx);
    if (!s.isOK()) {
        return absl::NotFoundError(s.msg);
    }

    auto source = schemas_ctx()->GetSchemaSource(sc_idx);
    auto path_idx = source->GetSourceChildIdx(col_idx);
    int child_col_id = source->GetSourceColumnID(col_idx);

    return ColProducerTraceInfo{{path_idx, child_col_id}};
}
absl::StatusOr<ColProducerTraceInfo> PhysicalOpNode::TraceColID(absl::string_view col_name) const {
    size_t sc_idx;
    size_t col_idx;
    auto s = schemas_ctx()->ResolveColumnIndexByName("", "", std::string(col_name), &sc_idx, &col_idx);
    if (!s.isOK()) {
        return absl::NotFoundError(s.msg);
    }

    auto source = schemas_ctx()->GetSchemaSource(sc_idx);
    auto path_idx = source->GetSourceChildIdx(col_idx);
    int child_col_id = source->GetSourceColumnID(col_idx);

    return ColProducerTraceInfo{{path_idx, child_col_id}};
}
absl::StatusOr<ColLastDescendantTraceInfo> PhysicalOpNode::TraceLastDescendants(size_t col_id) const {
    ColLastDescendantTraceInfo trace_info;
    auto info = TraceColID(col_id);
    if (!info.ok()) {
        return info.status();
    }
    for (auto entry : info.value()) {
        if (entry.first < 0) {
            trace_info.emplace_back(this, col_id);
            continue;
        }

        auto res = GetProducer(entry.first)->TraceLastDescendants(entry.second);
        if (!res.ok()) {
            return res.status();
        }

        for (auto& e : res.value()) {
            trace_info.emplace_back(e.first, e.second);
        }
    }

    return trace_info;
}
absl::StatusOr<ColProducerTraceInfo> PhysicalSetOperationNode::TraceColID(size_t col_id) const {
    std::string col_name;
    auto s = schemas_ctx()->ResolveColumnNameByID(col_id, &col_name);
    if (!s.isOK()) {
        return absl::NotFoundError(s.msg);
    }

    return TraceColID(col_name);
}
absl::StatusOr<ColProducerTraceInfo> PhysicalSetOperationNode::TraceColID(absl::string_view col_name) const {
    ColProducerTraceInfo vec;

    // every producer node in set operation is able to backtrace columns in current node context
    for (int i = 0; i < GetProducerCnt(); ++i) {
        size_t child_col_id = 0;
        auto s = GetProducer(i)->schemas_ctx()->ResolveColumnID("", "", std::string(col_name), &child_col_id);
        if (!s.isOK()) {
            return absl::NotFoundError(s.msg);
        }
        vec.emplace_back(i, child_col_id);
    }

    return vec;
}
}  // namespace vm
}  // namespace hybridse
