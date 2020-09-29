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

std::shared_ptr<TableHandler> PartitionProjectWrapper::GetSegment(
    std::shared_ptr<PartitionHandler> partition_handler,
    const std::string& key) {
    auto segment = partition_handler_->GetSegment(partition_handler_, key);
    if (!segment) {
        return std::shared_ptr<TableHandler>();
    } else {
        return std::shared_ptr<TableHandler>(
            new TableProjectWrapper(segment, fun_));
    }
}
base::ConstIterator<uint64_t, Row>* PartitionProjectWrapper::GetRawIterator()
    const {
    auto iter = partition_handler_->GetIterator();
    if (!iter) {
        return nullptr;
    } else {
        return new IteratorProjectWrapper(std::move(iter), fun_);
    }
}

std::shared_ptr<TableHandler> PartitionFilterWrapper::GetSegment(
    std::shared_ptr<PartitionHandler> partition_handler,
    const std::string& key) {
    auto segment = partition_handler_->GetSegment(partition_handler_, key);
    if (!segment) {
        return std::shared_ptr<TableHandler>();
    } else {
        return std::shared_ptr<TableHandler>(
            new TableFilterWrapper(segment, fun_));
    }
}
base::ConstIterator<uint64_t, Row>* PartitionFilterWrapper::GetRawIterator()
    const {
    auto iter = partition_handler_->GetIterator();
    if (!iter) {
        return nullptr;
    } else {
        return new IteratorFilterWrapper(std::move(iter), fun_);
    }
}
std::shared_ptr<PartitionHandler> TableProjectWrapper::GetPartition(
    std::shared_ptr<TableHandler> table_hander,
    const std::string& index_name) const {
    auto partition = table_hander_->GetPartition(table_hander_, index_name);
    if (!partition) {
        return std::shared_ptr<PartitionHandler>();
    } else {
        return std::shared_ptr<PartitionHandler>(
            new PartitionProjectWrapper(partition, fun_));
    }
}
std::shared_ptr<PartitionHandler> TableFilterWrapper::GetPartition(
    std::shared_ptr<TableHandler> table_hander,
    const std::string& index_name) const {
    auto partition = table_hander_->GetPartition(table_hander_, index_name);
    if (!partition) {
        return std::shared_ptr<PartitionHandler>();
    } else {
        return std::shared_ptr<PartitionHandler>(
            new PartitionFilterWrapper(partition, fun_));
    }
}
}  // namespace vm
}  // namespace fesql
