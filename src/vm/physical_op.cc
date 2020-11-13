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

namespace fesql {
namespace vm {

const char INDENT[] = "  ";
void PhysicalOpNode::Print(std::ostream& output, const std::string& tab) const {
    output << tab << PhysicalOpTypeName(type_);
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
bool PhysicalUnaryNode::InitSchema() {
    if (producers_.empty() || nullptr == producers_[0]) {
        LOG(WARNING) << "InitSchema fail: producers is empty or null";
        return false;
    }
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    output_name_schema_list_.AddSchemaSources(
        producers_[0]->GetOutputNameSchemaList());
    PrintSchema();
    return true;
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
bool PhysicalProjectNode::InitSchema() {
    output_schema_.CopyFrom(project_.fn_schema_);
    output_name_schema_list_.AddSchemaSource("", &project_.fn_schema_,
                                             &sources_);
    PrintSchema();
    return true;
}
bool PhysicalConstProjectNode::InitSchema() {
    output_schema_.CopyFrom(project_.fn_schema_);
    output_name_schema_list_.AddSchemaSource("", &project_.fn_schema_,
                                             &sources_);
    PrintSchema();
    return true;
}
bool PhysicalSimpleProjectNode::InitSchema() {
    output_name_schema_list_.AddSchemaSource(schema_name_, &output_schema_,
                                             &project_.column_sources());
    PrintSchema();
    return true;
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

PhysicalFliterNode* PhysicalFliterNode::CastFrom(PhysicalOpNode* node) {
    return dynamic_cast<PhysicalFliterNode*>(node);
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
    output << "(" << project_.ToString();
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
    if (need_append_input_) {
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
bool PhysicalJoinNode::InitSchema() {
    if (2 != producers_.size() || nullptr == producers_[0] ||
        nullptr == producers_[1]) {
        LOG(WARNING) << "InitSchema fail: producers size isn't 2 or left/right "
                        "producer is null";
        return false;
    }
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    output_schema_.MergeFrom(producers_[1]->output_schema_);
    output_name_schema_list_.AddSchemaSources(
        producers_[0]->GetOutputNameSchemaList());
    output_name_schema_list_.AddSchemaSources(
        producers_[1]->GetOutputNameSchemaList());
    PrintSchema();
    return true;
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
void PhysicalFliterNode::Print(std::ostream& output,
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

bool PhysicalDataProviderNode::InitSchema() {
    if (table_handler_) {
        auto schema = table_handler_->GetSchema();
        if (schema) {
            output_schema_.CopyFrom(*schema);
            output_name_schema_list_.AddSchemaSource(
                table_handler_->GetName(), table_handler_->GetSchema());
            PrintSchema();
            return true;
        } else {
            LOG(WARNING) << "InitSchema fail: table schema is null";
            return false;
        }
    } else {
        LOG(WARNING) << "InitSchema fail: table handler is null";
        return false;
    }
}

bool PhysicalRequestProviderNodeWithCommonColumn::
    ResetSchemaWithCommonColumnInfo() {
    if (table_handler_) {
        auto schema = table_handler_->GetSchema();
        if (!schema) {
            LOG(WARNING) << "InitSchema fail: table schema is null";
            return false;
        }
        size_t schema_size = static_cast<size_t>(schema->size());
        bool share_common = common_column_indices_.size() > 0 &&
                            common_column_indices_.size() < schema_size;
        if (share_common) {
            owned_common_schema_ = std::unique_ptr<Schema>(new Schema());
            owned_non_common_schema_ = std::unique_ptr<Schema>(new Schema());
            for (size_t i = 0; i < schema_size; ++i) {
                if (common_column_indices_.find(i) !=
                    common_column_indices_.end()) {
                    *owned_common_schema_->Add() = schema->Get(i);
                } else {
                    *owned_non_common_schema_->Add() = schema->Get(i);
                }
            }
            output_name_schema_list_.schema_source_list_.clear();
            output_name_schema_list_.AddSchemaSource(
                table_handler_->GetName(), owned_common_schema_.get());
            output_name_schema_list_.AddSchemaSource(
                table_handler_->GetName(), owned_non_common_schema_.get());

            output_schema_.Clear();
            for (auto i = 0; i < owned_common_schema_->size(); ++i) {
                *output_schema_.Add() = owned_common_schema_->Get(i);
            }
            for (auto i = 0; i < owned_non_common_schema_->size(); ++i) {
                *output_schema_.Add() = owned_non_common_schema_->Get(i);
            }
            return true;
        } else {
            output_name_schema_list_.schema_source_list_.clear();
            output_name_schema_list_.AddSchemaSource(table_handler_->GetName(),
                                                     schema);
            output_schema_ = *schema;
            return true;
        }
    } else {
        LOG(WARNING) << "InitSchema fail: table handler is null";
        return false;
    }
}

void PhysicalOpNode::PrintSchema() {
    std::stringstream ss;
    ss << PhysicalOpTypeName(type_) << " output name schema list: \n";
    for (auto pair : GetOutputNameSchemaList().schema_source_list_) {
        ss << "pair table: " << pair.table_name_ << "\n";
        for (int32_t i = 0; i < pair.schema_->size(); i++) {
            if (i > 0) {
                ss << "\n";
            }
            const type::ColumnDef& column = pair.schema_->Get(i);
            ss << column.name() << " " << type::Type_Name(column.type());
            if (nullptr != pair.sources_) {
                ss << " " << pair.sources_->at(i).ToString();
            }
        }
        ss << "\n";
    }
    ss << "output schema\n";
    for (int32_t i = 0; i < output_schema_.size(); i++) {
        if (i > 0) {
            ss << "\n";
        }
        const type::ColumnDef& column = output_schema_.Get(i);
        ss << column.name() << " " << type::Type_Name(column.type());
    }
    DLOG(INFO) << "\n" << ss.str();
}
bool PhysicalUnionNode::InitSchema() {
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    output_name_schema_list_.AddSchemaSources(
        producers_[0]->GetOutputNameSchemaList());
    PrintSchema();
    return true;
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
bool PhysicalRequestUnionNode::InitSchema() {
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    output_name_schema_list_.AddSchemaSources(
        producers_[0]->GetOutputNameSchemaList());
    PrintSchema();
    return true;
}
bool PhysicalRenameNode::InitSchema() {
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    for (auto source :
         producers_[0]->GetOutputNameSchemaList().schema_source_list_) {
        output_name_schema_list_.AddSchemaSource(name_, source.schema_,
                                                 source.sources_);
    }
    PrintSchema();
    return true;
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

bool PhysicalRequestJoinNode::InitSchema() {
    if (2 != producers_.size() || nullptr == producers_[0] ||
        nullptr == producers_[1]) {
        LOG(WARNING) << "InitSchema fail: producers size isn't 2 or left/right "
                        "producer is null";
        return false;
    }
    if (output_right_only_) {
        output_schema_.CopyFrom(producers_[1]->output_schema_);
        output_name_schema_list_.AddSchemaSources(
            producers_[1]->GetOutputNameSchemaList());
    } else {
        output_schema_.CopyFrom(producers_[0]->output_schema_);
        output_schema_.MergeFrom(producers_[1]->output_schema_);
        output_name_schema_list_.AddSchemaSources(
            producers_[0]->GetOutputNameSchemaList());
        output_name_schema_list_.AddSchemaSources(
            producers_[1]->GetOutputNameSchemaList());
    }
    PrintSchema();
    return true;
}
void PhysicalWindowNode::Print(std::ostream& output,
                               const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);

    output << "(partition_" << partition_.ToString() << ", "
           << sort_.ToString();
    if (range_.Valid()) {
        output << ", " << range_.ToString();
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}
}  // namespace vm
}  // namespace fesql
