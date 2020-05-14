/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * physical_op.cc
 *
 * Author: chenjing
 * Date: 2020/3/12
 *--------------------------------------------------------------------------
 **/
#include "vm/physical_op.h"
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
    output_name_schema_list_.AddSchemaSource("", &output_schema_, &sources_);
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

void PhysicalWindowAggrerationNode::Print(std::ostream& output,
                                          const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_);
    if (instance_not_in_window_) {
        output << ", INSTANCE_NOT_IN_WINDOW";
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
            window_union.first->Print(output, tab + INDENT + INDENT);
        }
    }
    output << "\n";
    PrintChildren(output, tab);
}
bool PhysicalWindowAggrerationNode::InitSchema() {
    // TODO(chenjing): Init Schema with window Join
    return false;
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
            window_union.first->Print(output, tab + INDENT + INDENT);
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

void PhysicalRequestJoinNode::Print(std::ostream& output,
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

bool PhysicalRequestJoinNode::InitSchema() {
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
