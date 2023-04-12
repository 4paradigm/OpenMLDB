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

LazyLastJoinIterator::LazyLastJoinIterator(std::unique_ptr<RowIterator>&& left, std::shared_ptr<PartitionHandler> right,
                                           const Row& param, std::shared_ptr<JoinGenerator> join)
    : left_it_(std::move(left)), right_(right), parameter_(param), join_(join) {}

void LazyLastJoinIterator::Seek(const uint64_t& key) { left_it_->Seek(key); }

void LazyLastJoinIterator::SeekToFirst() { left_it_->SeekToFirst(); }

const uint64_t& LazyLastJoinIterator::GetKey() const { return left_it_->GetKey(); }

void LazyLastJoinIterator::Next() { left_it_->Next(); }

bool LazyLastJoinIterator::Valid() const { return left_it_ && left_it_->Valid(); }

LazyLastJoinTableHandler::LazyLastJoinTableHandler(std::shared_ptr<TableHandler> left,
                                                   std::shared_ptr<PartitionHandler> right, const Row& param,
                                                   std::shared_ptr<JoinGenerator> join)
    : left_(left), right_(right), parameter_(param), join_(join) {}

LazyLastJoinPartitionHandler::LazyLastJoinPartitionHandler(std::shared_ptr<PartitionHandler> left,
                                                         std::shared_ptr<PartitionHandler> right, const Row& param,
                                                         std::shared_ptr<JoinGenerator> join)
    : left_(left), right_(right), parameter_(param), join_(join) {}

std::shared_ptr<TableHandler> LazyLastJoinPartitionHandler::GetSegment(const std::string& key) {
    auto left_seg = left_->GetSegment(key);
    return std::shared_ptr<TableHandler>(new LazyLastJoinTableHandler(left_seg, right_, parameter_, join_));
}

std::shared_ptr<PartitionHandler> LazyLastJoinTableHandler::GetPartition(const std::string& index_name) {
    return std::shared_ptr<PartitionHandler>(
        new LazyLastJoinPartitionHandler(left_->GetPartition(index_name), right_, parameter_, join_));
}

std::unique_ptr<RowIterator> LazyLastJoinTableHandler::GetIterator() {
    auto iter = left_->GetIterator();
    if (!iter) {
        return std::unique_ptr<RowIterator>();
    }

    return std::unique_ptr<RowIterator>(new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_));
}
std::unique_ptr<RowIterator> LazyLastJoinPartitionHandler::GetIterator() {
    auto iter = left_->GetIterator();
    if (!iter) {
        return std::unique_ptr<RowIterator>();
    }
    return std::unique_ptr<RowIterator>(new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_));
}

std::unique_ptr<WindowIterator> LazyLastJoinPartitionHandler::GetWindowIterator() { return left_->GetWindowIterator(); }

const Row& LazyLastJoinIterator::GetValue() {
    value_ = join_->RowLastJoin(left_it_->GetValue(), right_, parameter_);
    return value_;
}

std::unique_ptr<WindowIterator> LazyLastJoinTableHandler::GetWindowIterator(const std::string& idx_name) {
    return nullptr;
}
}  // namespace vm
}  // namespace hybridse
