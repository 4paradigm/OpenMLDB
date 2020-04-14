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
    for(auto pair : producers_[0]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
    PrintSchema();
    return true;
}
bool PhysicalBinaryNode::InitSchema() {
    if (producers_.empty() || nullptr == producers_[0]) {
        LOG(WARNING) << "InitSchema fail: producers is empty or null";
        return false;
    }
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    for(auto pair : producers_[0]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
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

void PhysicalScanIndexNode::Print(std::ostream& output,
                                  const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << DataProviderTypeName(provider_type_)
           << ", table=" << table_handler_->GetName()
           << ", index=" << index_name_ << ")";
}

void PhysicalGroupNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(groups=" << node::ExprString(groups_) << ")";
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalGroupAndSortNode::Print(std::ostream& output,
                                     const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(groups=" << node::ExprString(groups_)
           << ", orders=" << node::ExprString(orders_) << ")";
    output << "\n";
    PrintChildren(output, tab);
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
    PrintSchema();
    return true;
}

void PhysicalGroupAggrerationNode::Print(std::ostream& output,
                                         const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ProjectTypeName(project_type_)
           << ", groups=" << node::ExprString(groups_);
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
    output << "(type=" << ProjectTypeName(project_type_)
           << ", groups=" << node::ExprString(groups_)
           << ", orders=" << node::ExprString(orders_)
           << ", start=" << std::to_string(start_offset_)
           << ", end=" << std::to_string(end_offset_);
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalLoopsNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalJoinNode::Print(std::ostream& output,
                             const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << node::JoinTypeName(join_type_)
           << ", condition=" << node::ExprString(condition_);
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
    for(auto pair : producers_[0]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
    for(auto right_pair: producers_[1]->output_name_schema_list_) {
        output_name_schema_list_.push_back(right_pair);
    }
    PrintSchema();
    return false;
}

void PhysicalSortNode::Print(std::ostream& output,
                             const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << node::ExprString(order_);
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
    output << "(condition=" << node::ExprString(condition_);
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}

bool PhysicalDataProviderNode::InitSchema() {
    if (table_handler_) {
        auto schema = table_handler_->GetSchema();
        if (schema) {
            output_schema_.CopyFrom(*schema);
            output_name_schema_list_.push_back(
                std::make_pair(table_handler_->GetName(), table_handler_->GetSchema()));
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
    for(auto pair : producers_[0]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
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
    output << "(groups=" << node::ExprString(groups_)
           << ", orders=" << node::ExprString(orders_)
           << ", keys=" << node::ExprString(keys_)
           << ", start=" << std::to_string(start_offset_)
           << ", end=" << std::to_string(end_offset_);
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}
bool PhysicalRequestUnionNode::InitSchema() {
    output_schema_.CopyFrom(producers_[0]->output_schema_);
    for(auto pair : producers_[0]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
    PrintSchema();
    return true;
}

void PhysicalRequestJoinNode::Print(std::ostream& output,
                                    const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(condition=" << node::ExprString(condition_);
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
    for(auto pair : producers_[0]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
    for(auto right_pair: producers_[1]->output_name_schema_list_) {
        output_name_schema_list_.push_back(right_pair);
    }
    PrintSchema();
    return true;
}
void PhysicalSeekIndexNode::Print(std::ostream& output,
                                  const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(keys=" << node::ExprString(keys_);
    if (limit_cnt_ > 0) {
        output << ", limit=" << limit_cnt_;
    }
    output << ")";
    output << "\n";
    PrintChildren(output, tab);
}
bool PhysicalSeekIndexNode::InitSchema() {
    output_schema_.CopyFrom(producers_[1]->output_schema_);
    for(auto pair : producers_[1]->output_name_schema_list_) {
        output_name_schema_list_.push_back(pair);
    }
    PrintSchema();
    return true;
}
}  // namespace vm
}  // namespace fesql
