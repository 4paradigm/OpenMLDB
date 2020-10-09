/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * catalog_wrapper.cc
 *
 * Author: chenjing
 * Date: 2020/5/19
 *--------------------------------------------------------------------------
 **/
#include "vm/catalog_wrapper.h"
namespace fesql {
namespace vm {

std::shared_ptr<TableHandler> PartitionWrapper::GetSegment(
    std::shared_ptr<PartitionHandler> partition_handler,
    const std::string& key) {
    return partition_handler_->GetSegment(partition_handler, key);
}
base::ConstIterator<uint64_t, Row>* PartitionWrapper::GetRawIterator() const {
    return new IteratorWrapper(partition_handler_->GetIterator(), fun_);
}
std::shared_ptr<PartitionHandler> TableWrapper::GetPartition(
    std::shared_ptr<TableHandler> table_hander,
    const std::string& index_name) const {
    return table_hander_->GetPartition(table_hander, index_name);
}
}  // namespace vm
}  // namespace fesql
