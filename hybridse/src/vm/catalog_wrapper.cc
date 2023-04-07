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

#include "vm/catalog_wrapper.h"
namespace hybridse {
namespace vm {

std::shared_ptr<TableHandler> PartitionProjectWrapper::GetSegment(
    const std::string& key) {
    auto segment = partition_handler_->GetSegment(key);
    if (!segment) {
        return std::shared_ptr<TableHandler>();
    } else {
        return std::shared_ptr<TableHandler>(
            new TableProjectWrapper(segment, parameter_, fun_));
    }
}
base::ConstIterator<uint64_t, Row>* PartitionProjectWrapper::GetRawIterator() {
    auto iter = partition_handler_->GetIterator();
    if (!iter) {
        return nullptr;
    } else {
        return new IteratorProjectWrapper(std::move(iter), parameter_, fun_);
    }
}

std::shared_ptr<TableHandler> PartitionFilterWrapper::GetSegment(
    const std::string& key) {
    auto segment = partition_handler_->GetSegment(key);
    if (!segment) {
        return std::shared_ptr<TableHandler>();
    } else {
        return std::shared_ptr<TableHandler>(
            new TableFilterWrapper(segment, parameter_, fun_));
    }
}
base::ConstIterator<uint64_t, Row>* PartitionFilterWrapper::GetRawIterator() {
    auto iter = partition_handler_->GetIterator();
    if (!iter) {
        return nullptr;
    } else {
        return new IteratorFilterWrapper(std::move(iter), parameter_, fun_);
    }
}
std::shared_ptr<PartitionHandler> TableProjectWrapper::GetPartition(
    const std::string& index_name) {
    auto partition = table_hander_->GetPartition(index_name);
    if (!partition) {
        return std::shared_ptr<PartitionHandler>();
    } else {
        return std::shared_ptr<PartitionHandler>(
            new PartitionProjectWrapper(partition, parameter_, fun_));
    }
}
std::shared_ptr<PartitionHandler> TableFilterWrapper::GetPartition(
    const std::string& index_name) {
    auto partition = table_hander_->GetPartition(index_name);
    if (!partition) {
        return std::shared_ptr<PartitionHandler>();
    } else {
        return std::shared_ptr<PartitionHandler>(
            new PartitionFilterWrapper(partition, parameter_, fun_));
    }
}

LazyLastJoinTableHandler::LazyLastJoinTableHandler(std::shared_ptr<TableHandler> left,
                                                   std::shared_ptr<PartitionHandler> right, const Row& param,
                                                   std::shared_ptr<JoinGenerator> join)
    : left_(left), right_(right), parameter_(param), join_(join) {}

LazyLastJoinPartitionHandler::LazyLastJoinPartitionHandler(std::shared_ptr<PartitionHandler> left,
                                                         std::shared_ptr<PartitionHandler> right, const Row& param,
                                                         std::shared_ptr<JoinGenerator> join)
    : left_(left), right_(right), parameter_(param), join_(join) {}

}  // namespace vm
}  // namespace hybridse
