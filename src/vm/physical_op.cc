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
    output << "\n";
    PrintChildren(output, tab);
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
void PhysicalScanTableNode::Print(std::ostream& output,
                                  const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(table=" << table_handler_->GetName() << ")";
}

void PhysicalScanIndexNode::Print(std::ostream& output,
                                  const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(type=" << ScanTypeName(scan_type_)
           << ", table=" << table_handler_->GetName()
           << ", index=" << index_name_ << ")";
}
void PhysicalGroupNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(group by =" << node::ExprString(groups_) << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalProjectNode::Print(std::ostream& output,
                                const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    std::string projects_str = "(";
    if (project_.empty()) {
        projects_str.append(")");
    } else {
        auto iter = project_.cbegin();
        auto node = dynamic_cast<node::ProjectNode*>(*iter);
        projects_str.append(node->GetName());
        iter++;
        for (;iter != project_.cend(); iter++) {
            auto node = dynamic_cast<node::ProjectNode*>(*iter);
            projects_str.append(",");

            if (node->GetPos() >= 10) {
                projects_str.append("...");
                break;
            }
            projects_str.append(node->GetName());
        }
        projects_str.append(")");
    }

    output << "(type=" << ProjectTypeName(project_type_)
           << ", projects=" << projects_str << ")";
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
           << ", condition=" << node::ExprString(condition_) << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalSortNode::Print(std::ostream& output,
                             const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << node::ExprString(order_) << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalLimitNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(limit=" << std::to_string(limit_cnt) << ")";
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
    output << "(condition=" << node::ExprString(condition_) << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalBufferNode::Print(std::ostream& output,
                               const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(start=" << std::to_string(start_offset_)
           << ", end=" << std::to_string(end_offset_) << ")";
    output << "\n";
    PrintChildren(output, tab);
}
}  // namespace vm
}  // namespace fesql
