/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_op.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "vm/physical_op.h"
#include <set>
#include "passes/physical/physical_pass.h"

namespace fesql {
namespace vm {

using fesql::base::Status;

const char INDENT[] = "  ";
void PhysicalOpNode::Print(std::ostream& output, const std::string& tab) const {
    output << tab << PhysicalOpTypeName(type_);
}

bool PhysicalOpNode::IsSameSchema(const vm::Schema& schema,
                                  const vm::Schema& exp_schema) const {
    if (schema.size() != exp_schema.size()) {
        LOG(WARNING) << "Schemas size aren't consistent: "
                     << "expect size " << exp_schema.size() << ", real size "
                     << schema.size();
        return false;
    }
    for (int i = 0; i < schema.size(); i++) {
        if (schema.Get(i).name() != exp_schema.Get(i).name()) {
            LOG(WARNING) << "Schemas aren't consistent:\n"
                         << exp_schema.Get(i).DebugString() << "vs:\n"
                         << schema.Get(i).DebugString();
            return false;
        }
        if (schema.Get(i).type() != exp_schema.Get(i).type()) {
            LOG(WARNING) << "Schemas aren't consistent:\n"
                         << exp_schema.Get(i).DebugString() << "vs:\n"
                         << schema.Get(i).DebugString();
            return false;
        }
    }
    return true;
}

void PhysicalOpNode::Print() const { this->Print(std::cout, "    "); }

void PhysicalOpNode::PrintChildren(std::ostream& output,
                                   const std::string& tab) const {}
void PhysicalOpNode::UpdateProducer(int i, PhysicalOpNode* producer) {
    producers_[i] = producer;
}
void PhysicalUnaryNode::PrintChildren(std::ostream& output,
                                      const std::string& tab) const {
    if (producers_.empty() || nullptr == producers_[0]) {
        LOG(WARNING) << "empty producers";
        return;
    }
    producers_[0]->Print(output, tab + INDENT);
}
void PhysicalUnaryNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    if (limit_cnt_ > 0) {
        output << "(limit=" << limit_cnt_ << ")";
    }
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalUnaryNode::InitSchema(PhysicalPlanContext*) {
    schemas_ctx_.Clear();
    schemas_ctx_.Merge(0, GetProducer(0)->schemas_ctx());
    return Status::OK();
}

void PhysicalBinaryNode::PrintChildren(std::ostream& output,
                                       const std::string& tab) const {
    if (2 != producers_.size() || nullptr == producers_[0] ||
        nullptr == producers_[1]) {
        LOG(WARNING) << "fail to print children";
        return;
    }
    producers_[0]->Print(output, tab + INDENT);
    output << "\n";
    producers_[1]->Print(output, tab + INDENT);
}
void PhysicalBinaryNode::Print(std::ostream& output,
                               const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalTableProviderNode::Print(std::ostream& output,
                                      const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(table=" << table_handler_->GetName() << ")";
}

void PhysicalRequestProviderNode::Print(std::ostream& output,
                                        const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(request=" << table_handler_->GetName() << ")";
}

void PhysicalRequestProviderNodeWithCommonColumn::Print(
    std::ostream& output, const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(request=" << table_handler_->GetName()
           << ", common_column_num=" << common_column_indices_.size() << ")";
}

void PhysicalPartitionProviderNode::Print(std::ostream& output,
                                          const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << DataProviderTypeName(provider_type_)
           << ", table=" << table_handler_->GetName()
           << ", index=" << index_name_ << ")";
}

void PhysicalGroupNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "("
           << "group_" << group_.ToString() << ")";
    output << "\n";
    PrintChildren(output, tab);
}
PhysicalGroupNode* PhysicalGroupNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalGroupNode*>(node);
}

void ColumnProjects::Add(const std::string& name, const node::ExprNode* expr,
                         const node::FrameNode* frame) {
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

void PhysicalProjectNode::Print(std::ostream& output,
                                const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
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
 */
static Status InitProjectSchemaSource(const ColumnProjects& projects,
                                      const SchemasContext* schemas_ctx,
                                      PhysicalPlanContext* plan_ctx,
                                      SchemaSource* project_source) {
    const FnInfo& fn_info = projects.fn_info();
    CHECK_TRUE(fn_info.IsValid(), common::kPlanError,
               "Project node's function info is not valid");
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
            CHECK_STATUS(schemas_ctx->ResolveColumnID(
                col_ref->GetRelationName(), col_ref->GetColumnName(),
                &column_id));
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
    CHECK_STATUS(
        ctx->InitFnDef(project_, input_schemas_ctx, is_row_project, &project_),
        "Fail to initialize function info of project node");

    // init project schema
    schemas_ctx_.Clear();
    SchemaSource* project_source = schemas_ctx_.AddSource();
    return InitProjectSchemaSource(project_, input_schemas_ctx, ctx,
                                   project_source);
}

Status PhysicalConstProjectNode::InitSchema(PhysicalPlanContext* ctx) {
    SchemasContext empty_ctx;
    CHECK_STATUS(ctx->InitFnDef(project_, &empty_ctx, true, &project_),
                 "Fail to initialize function def of const project node");
    schemas_ctx_.Clear();
    SchemaSource* project_source = schemas_ctx_.AddSource();

    SchemasContext empty_schemas_ctx;
    return InitProjectSchemaSource(project_, &empty_schemas_ctx, ctx,
                                   project_source);
}

Status PhysicalSimpleProjectNode::InitSchema(PhysicalPlanContext* ctx) {
    auto input_schemas_ctx = GetProducer(0)->schemas_ctx();
    // init project fn
    CHECK_STATUS(ctx->InitFnDef(project_, input_schemas_ctx, true, &project_),
                 "Fail to initialize function info of simple project node");

    // init project schema
    schemas_ctx_.Clear();
    SchemaSource* project_source = schemas_ctx_.AddSource();
    return InitProjectSchemaSource(project_, input_schemas_ctx, ctx,
                                   project_source);
}

PhysicalProjectNode* PhysicalProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalProjectNode*>(node);
}

PhysicalRowProjectNode* PhysicalRowProjectNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRowProjectNode*>(node);
}

PhysicalTableProjectNode* PhysicalTableProjectNode::CastFrom(
    PhysicalOpNode* node) {
    return dynamic_cast<PhysicalTableProjectNode*>(node);
}

PhysicalConstProjectNode* PhysicalConstProjectNode::CastFrom(
    PhysicalOpNode* node) {
    return dynamic_cast<PhysicalConstProjectNode*>(node);
}

PhysicalSimpleProjectNode* PhysicalSimpleProjectNode::CastFrom(
    PhysicalOpNode* node) {
    return dynamic_cast<PhysicalSimpleProjectNode*>(node);
}

PhysicalUnionNode* PhysicalUnionNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalUnionNode*>(node);
}

PhysicalSortNode* PhysicalSortNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalSortNode*>(node);
}

PhysicalFilterNode* PhysicalFilterNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalFilterNode*>(node);
}

PhysicalLimitNode* PhysicalLimitNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalLimitNode*>(node);
}
PhysicalRenameNode* PhysicalRenameNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalRenameNode*>(node);
}
void PhysicalConstProjectNode::Print(std::ostream& output,
                                     const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
}
void PhysicalSimpleProjectNode::Print(std::ostream& output,
                                      const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    output << "sources=(";
    if (schemas_ctx_.GetSchemaSourceSize() > 0) {
        output << schemas_ctx_.GetSchemaSource(0)->ToString();
    }
    output << ")";
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";

    output << "\n";
    PrintChildren(output, tab);
}
PhysicalWindowAggrerationNode* PhysicalWindowAggrerationNode::CastFrom(
    PhysicalOpNode* node) {
    return dynamic_cast<PhysicalWindowAggrerationNode*>(node);
}

void PhysicalGroupAggrerationNode::Print(std::ostream& output,
                                         const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_) << ", "
           << "group_" << group_.ToString();
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

PhysicalGroupAggrerationNode* PhysicalGroupAggrerationNode::CastFrom(
    PhysicalOpNode* node) {
    return dynamic_cast<PhysicalGroupAggrerationNode*>(node);
}

void PhysicalWindowAggrerationNode::Print(std::ostream& output,
                                          const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    if (instance_not_in_window_) {
        output << ", INSTANCE_NOT_IN_WINDOW";
    }
    if (need_append_input()) {
        output << ", NEED_APPEND_INPUT";
    }
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")\n";

    output << tab << INDENT << "+-WINDOW(" << window_.ToString() << ")";

    if (!window_joins_.Empty()) {
        for (auto window_join : window_joins_.window_joins_) {
            output << "\n";
            output << tab << INDENT << "+-JOIN("
                   << window_join.second.ToString() << ")\n";
            window_join.first->Print(output, tab + INDENT + INDENT + INDENT);
        }
    }

    if (!window_unions_.Empty()) {
        for (auto window_union : window_unions_.window_unions_) {
            output << "\n";
            output << tab << INDENT << "+-UNION("
                   << window_union.second.ToString() << ")\n";
            window_union.first->Print(output, tab + INDENT + INDENT + INDENT);
        }
    }
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalWindowAggrerationNode::InitSchema(PhysicalPlanContext* ctx) {
    auto input = GetProducer(0);
    auto input_schemas_ctx = input->schemas_ctx();

    // init fn info
    bool is_row_project = !IsAggProjectType(project_type_);
    CHECK_STATUS(
        ctx->InitFnDef(project_, input_schemas_ctx, is_row_project, &project_),
        "Fail to initialize function def of project node");

    // init output schema
    schemas_ctx_.Clear();
    auto project_source = schemas_ctx_.AddSource();
    CHECK_STATUS(InitProjectSchemaSource(project_, input_schemas_ctx, ctx,
                                         project_source));

    // window agg may inherit input row
    if (need_append_input()) {
        schemas_ctx_.Merge(0, input_schemas_ctx);
    }
    return Status::OK();
}

Status PhysicalWindowAggrerationNode::InitJoinList(
    PhysicalPlanContext* plan_ctx) {
    auto& window_joins = window_joins_.window_joins();
    if (window_joins.size() == joined_op_list_.size()) {
        return Status::OK();
    }
    joined_op_list_.clear();
    PhysicalOpNode* cur = this->GetProducer(0);
    for (auto& pair : window_joins) {
        PhysicalJoinNode* joined = nullptr;
        CHECK_STATUS(plan_ctx->CreateOp<PhysicalJoinNode>(
            &joined, cur, pair.first, pair.second));
        joined_op_list_.push_back(joined);
        cur = joined;
    }
    return Status::OK();
}

void PhysicalJoinNode::Print(std::ostream& output,
                             const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << join_.ToString();
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalJoinNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(2 == producers_.size() && nullptr != producers_[0] &&
                   nullptr != producers_[1],
               common::kPlanError,
               "InitSchema fail: producers size isn't 2 or left/right "
               "producer is null");
    schemas_ctx_.Clear();
    schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    schemas_ctx_.MergeWithNewID(1, producers_[1]->schemas_ctx(), ctx);

    // join input schema context
    joined_schemas_ctx_.Clear();
    joined_schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    joined_schemas_ctx_.Merge(1, producers_[1]->schemas_ctx());
    joined_schemas_ctx_.Build();
    return Status::OK();
}

PhysicalJoinNode* PhysicalJoinNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalJoinNode*>(node);
}

void PhysicalSortNode::Print(std::ostream& output,
                             const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << sort_.ToString();
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalLimitNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(limit=" << std::to_string(limit_cnt_)
           << (limit_optimized_ ? ", optimized" : "") << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalRenameNode::Print(std::ostream& output,
                               const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(name=" << name_ << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalFilterNode::Print(std::ostream& output,
                               const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << filter_.ToString();
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

PhysicalDataProviderNode* PhysicalDataProviderNode::CastFrom(
    PhysicalOpNode* node) {
    return dynamic_cast<PhysicalDataProviderNode*>(node);
}

const std::string& PhysicalDataProviderNode::GetName() const {
    return table_handler_->GetName();
}

Status PhysicalDataProviderNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(table_handler_ != nullptr, common::kPlanError,
               "InitSchema fail: table handler is null");
    const std::string table_name = table_handler_->GetName();
    auto schema = table_handler_->GetSchema();
    CHECK_TRUE(schema != nullptr, common::kPlanError,
               "InitSchema fail: table schema of ", table_name, " is null");

    schemas_ctx_.Clear();
    schemas_ctx_.SetName(table_name);
    auto table_source = schemas_ctx_.AddSource();

    // set table source
    table_source->SetSchema(schema);
    table_source->SetSourceName(table_name);
    for (auto i = 0; i < schema->size(); ++i) {
        size_t column_id;
        CHECK_STATUS(
            ctx->GetSourceID(table_name, schema->Get(i).name(), &column_id),
            "Get source column id from table \"", table_name, "\" failed");
        table_source->SetColumnID(i, column_id);
    }
    return Status::OK();
}

Status PhysicalRequestProviderNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(table_handler_ != nullptr, common::kPlanError,
               "InitSchema fail: table handler is null");
    const std::string request_name = table_handler_->GetName();
    auto schema = table_handler_->GetSchema();
    CHECK_TRUE(schema != nullptr, common::kPlanError,
               "InitSchema fail: table schema of", request_name, " is null");

    schemas_ctx_.Clear();
    schemas_ctx_.SetName(request_name);
    auto request_source = schemas_ctx_.AddSource();

    // set request source
    request_source->SetSchema(schema);
    request_source->SetSourceName(request_name);
    for (auto i = 0; i < schema->size(); ++i) {
        size_t column_id;
        CHECK_STATUS(ctx->GetRequestSourceID(request_name,
                                             schema->Get(i).name(), &column_id),
                     "Get source column id from table \"", request_name,
                     "\" failed");
        request_source->SetColumnID(i, column_id);
    }
    return Status::OK();
}

Status PhysicalRequestProviderNodeWithCommonColumn::InitSchema(
    PhysicalPlanContext* ctx) {
    CHECK_TRUE(table_handler_ != nullptr, common::kPlanError,
               "InitSchema fail: table handler is null");
    const std::string request_name = table_handler_->GetName();
    auto schema = table_handler_->GetSchema();
    size_t schema_size = static_cast<size_t>(schema->size());
    CHECK_TRUE(schema != nullptr, common::kPlanError,
               "InitSchema fail: table schema of", request_name, " is null");

    schemas_ctx_.Clear();
    schemas_ctx_.SetName(request_name);

    std::vector<size_t> column_ids(schema_size);
    for (size_t i = 0; i < schema_size; ++i) {
        CHECK_STATUS(ctx->GetRequestSourceID(
                         request_name, schema->Get(i).name(), &column_ids[i]),
                     "Get column id from ", request_name, " failed");
    }

    bool share_common = common_column_indices_.size() > 0 &&
                        common_column_indices_.size() < schema_size;
    if (share_common) {
        auto common_source = schemas_ctx_.AddSource();
        auto non_common_source = schemas_ctx_.AddSource();
        common_source->SetSourceName(request_name);
        non_common_source->SetSourceName(request_name);

        common_schema_.Clear();
        non_common_schema_.Clear();
        for (size_t i = 0; i < schema_size; ++i) {
            if (common_column_indices_.find(i) !=
                common_column_indices_.end()) {
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
            if (common_column_indices_.find(i) !=
                common_column_indices_.end()) {
                common_source->SetColumnID(lsize, column_ids[i]);
                lsize += 1;
            } else {
                non_common_source->SetColumnID(rsize, column_ids[i]);
                rsize += 1;
            }
        }
    } else {
        auto request_source = schemas_ctx_.AddSource();
        request_source->SetSourceName(request_name);
        request_source->SetSchema(schema);
        for (size_t i = 0; i < schema_size; ++i) {
            request_source->SetColumnID(i, column_ids[i]);
        }
    }
    return Status::OK();
}

void PhysicalOpNode::PrintSchema() const {
    std::stringstream ss;
    ss << "[";
    if (!schemas_ctx_.GetName().empty()) {
        ss << "name=" << schemas_ctx_.GetName() << ", ";
    }
    ss << "type=" << PhysicalOpTypeName(type_);
    ss << ", sources=" << GetOutputSchemaSourceSize();
    ss << ", columns=" << GetOutputSchema()->size();
    ss << "]\n";

    for (size_t i = 0; i < GetOutputSchemaSourceSize(); ++i) {
        ss << "{\n";
        const SchemaSource* schema_source = GetOutputSchemaSource(i);
        const auto* schema = schema_source->GetSchema();
        for (int32_t j = 0; j < schema->size(); j++) {
            ss << "    ";
            const type::ColumnDef& column = schema->Get(j);
            ss << "#" << schema_source->GetColumnID(j) << " " << column.name()
               << " " << type::Type_Name(column.type());
            if (schema_source->IsSourceColumn(j)) {
                ss << " <- #" << schema_source->GetSourceColumnID(j)
                   << " (from [" << schema_source->GetSourceChildIdx(j) << "])";
            }
            ss << "\n";
        }
        ss << "} ";
    }
    std::cout << ss.str() << std::endl;
}

base::Status PhysicalUnionNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty union");
    schemas_ctx_.Clear();
    schemas_ctx_.MergeWithNewID(0, producers_[0]->schemas_ctx(), ctx);
    return Status::OK();
}

void PhysicalUnionNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalRequestUnionNode::Print(std::ostream& output,
                                     const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << window_.ToString() << ")";
    if (!window_unions_.Empty()) {
        for (auto window_union : window_unions_.window_unions_) {
            output << "\n";
            output << tab << INDENT << "+-UNION("
                   << window_union.second.ToString() << ")\n";
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

base::Status PhysicalRequestUnionNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty request union");
    schemas_ctx_.Clear();
    schemas_ctx_.MergeWithNewID(0, producers_[0]->schemas_ctx(), ctx);
    return Status::OK();
}

base::Status PhysicalRenameNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(!producers_.empty(), common::kPlanError, "Empty request union");
    schemas_ctx_.Clear();
    schemas_ctx_.SetName(name_);
    schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    return Status::OK();
}

void PhysicalRequestJoinNode::Print(std::ostream& output,
                                    const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(";
    if (output_right_only_) {
        output << "OUTPUT_RIGHT_ONLY, ";
    }
    output << join_.ToString();
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

Status PhysicalRequestJoinNode::InitSchema(PhysicalPlanContext* ctx) {
    CHECK_TRUE(2 == producers_.size() && nullptr != producers_[0] &&
                   nullptr != producers_[1],
               common::kPlanError,
               "InitSchema fail: producers size isn't 2 or left/right "
               "producer is null");
    schemas_ctx_.Clear();
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
    joined_schemas_ctx_.Merge(0, producers_[0]->schemas_ctx());
    joined_schemas_ctx_.Merge(1, producers_[1]->schemas_ctx());
    joined_schemas_ctx_.Build();
    return Status::OK();
}

void PhysicalWindowNode::Print(std::ostream& output,
                               const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);

    output << "(partition_" << window_op_.partition_.ToString() << ", "
           << window_op_.sort_.ToString();
    if (window_op_.range_.Valid()) {
        output << ", " << window_op_.range_.ToString();
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}
}  // namespace vm
}  // namespace fesql
