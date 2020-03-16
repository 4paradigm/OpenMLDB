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

const char INDENT[] = "\t";
void PhysicalOpNode::Print(std::ostream& output, const std::string& tab) const {
    output << PhysicalOpTypeName(type_);
}
void PhysicalOpNode::PrintChildren(std::ostream& output,
                                   const std::string& tab) const {}
void PhysicalUnaryNode::PrintChildren(std::ostream& output,
                                      const std::string& tab) const {
    producer_->Print(output, tab);
}
void PhysicalBinaryNode::PrintChildren(std::ostream& output,
                                       const std::string& tab) const {
    left_producer_->Print(output, tab + INDENT);
    output << "\n";
    right_producer_->Print(output, tab + INDENT);
}
void PhysicalScanTableNode::Print(std::ostream& output,
                                  const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(table=" << table_handler_->GetName() << ")";
    output << "\n";
    PrintChildren(output, tab);
}

void PhysicalScanIndexNode::Print(std::ostream& output,
                                  const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(table=" << table_handler_->GetName() << "index=" << index_name_
           << ")";
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalGroupNode::Print(std::ostream& output,
                              const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalProjectNode::Print(std::ostream& output,
                                const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(projectType=" << ProjectTypeName(project_type_);
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
    output << "\n";
    PrintChildren(output, tab);
}
void PhysicalSortNode::Print(std::ostream& output,
                             const std::string& tab) const {
    PhysicalOpNode::Print(output, tab);
    output << "(" << order_->GetExprString() << ")";
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
}  // namespace vm
}  // namespace fesql
