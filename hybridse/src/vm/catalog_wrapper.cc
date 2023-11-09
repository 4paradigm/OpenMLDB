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

std::unique_ptr<WindowIterator> LazyLastJoinPartitionHandler::GetWindowIterator() {
    auto wi = left_->GetWindowIterator();
    if (wi == nullptr) {
        return std::unique_ptr<WindowIterator>();
    }

    return std::unique_ptr<WindowIterator>(new LazyLastJoinWindowIterator(std::move(wi), right_, parameter_, join_));
}

const Row& LazyLastJoinIterator::GetValue() {
    value_ = join_->RowLastJoin(left_it_->GetValue(), right_, parameter_);
    return value_;
}

std::unique_ptr<WindowIterator> LazyLastJoinTableHandler::GetWindowIterator(const std::string& idx_name) {
    return nullptr;
}

LazyLastJoinWindowIterator::LazyLastJoinWindowIterator(std::unique_ptr<WindowIterator>&& iter,
                                                       std::shared_ptr<PartitionHandler> right, const Row& param,
                                                       std::shared_ptr<JoinGenerator> join)
    : left_(std::move(iter)), right_(right), parameter_(param), join_(join) {}
std::unique_ptr<RowIterator> LazyLastJoinWindowIterator::GetValue() {
    auto iter = left_->GetValue();
    if (!iter) {
        return std::unique_ptr<RowIterator>();
    }

    return std::unique_ptr<RowIterator>(new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_));
}
RowIterator* LazyLastJoinWindowIterator::GetRawValue() {
    auto iter = left_->GetValue();
    if (!iter) {
        return nullptr;
    }

    return new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_);
}

std::shared_ptr<TableHandler> ConcatPartitionHandler::GetSegment(const std::string& key) {
    auto left_seg = left_->GetSegment(key);
    auto right_seg = right_->GetSegment(key);
    return std::shared_ptr<TableHandler>(
        new SimpleConcatTableHandler(left_seg, left_slices_, right_seg, right_slices_));
}

RowIterator* ConcatPartitionHandler::GetRawIterator() {
    auto li = left_->GetIterator();
    if (!li) {
        return nullptr;
    }
    auto ri = right_->GetIterator();
    return new ConcatIterator(std::move(li), left_slices_, std::move(ri), right_slices_);
}

std::unique_ptr<RowIterator> ConcatPartitionHandler::GetIterator() {
    auto p = GetRawIterator();
    if (p == nullptr) {
        return {};
    }
    return std::unique_ptr<RowIterator>(p);
}

std::unique_ptr<WindowIterator> LazyRequestUnionPartitionHandler::GetWindowIterator() {
    auto w = left_->GetWindowIterator();
    if (!w) {
        return {};
    }

    return std::unique_ptr<WindowIterator>(new LazyRequestUnionWindowIterator(std::move(w), func_));
}

std::shared_ptr<TableHandler> LazyRequestUnionPartitionHandler::GetSegment(const std::string& key) {
    return nullptr;
}

std::unique_ptr<RowIterator> LazyRequestUnionPartitionHandler::GetIterator() {
    return std::unique_ptr<RowIterator>(GetRawIterator());
}
const IndexHint& LazyRequestUnionPartitionHandler::GetIndex() { return left_->GetIndex(); }

const Types& LazyRequestUnionPartitionHandler::GetTypes() { return left_->GetTypes(); }

base::ConstIterator<uint64_t, Row>* LazyRequestUnionPartitionHandler::GetRawIterator() { return nullptr; }
bool LazyAggIterator::Valid() const { return it_->Valid(); }
void LazyAggIterator::Next() { it_->Next(); }
const uint64_t& LazyAggIterator::GetKey() const { return it_->GetKey(); }
const Row& LazyAggIterator::GetValue() {
    if (Valid()) {
        auto request = it_->GetValue();
        auto window = func_(request);
        if (window) {
            buf_ = agg_gen_->Gen(parameter_, window);
            return buf_;
        }
    }

    buf_ = Row();
    return buf_;
}

void LazyAggIterator::Seek(const uint64_t& key) { it_->Seek(key); }
void LazyAggIterator::SeekToFirst() { it_->SeekToFirst(); }
std::unique_ptr<RowIterator> LazyAggTableHandler::GetIterator() {
    auto* it = GetRawIterator();
    if (it == nullptr) {
        return {};
    }
    return std::unique_ptr<RowIterator>(it);
}
std::unique_ptr<WindowIterator> LazyAggTableHandler::GetWindowIterator(const std::string& idx_name) { return nullptr; }
base::ConstIterator<uint64_t, Row>* LazyAggTableHandler::GetRawIterator() {
    auto it = left_->GetIterator();
    if (!it) {
        return nullptr;
    }
    return new LazyAggIterator(std::move(it), func_, agg_gen_, parameter_);
}
std::shared_ptr<PartitionHandler> LazyAggTableHandler::GetPartition(const std::string& index_name) { return nullptr; }
const Types& LazyAggTableHandler::GetTypes() { return left_->GetTypes(); }
const IndexHint& LazyAggTableHandler::GetIndex() { return left_->GetIndex(); }
const Schema* LazyAggTableHandler::GetSchema() { return nullptr; }
const std::string& LazyAggTableHandler::GetName() { return left_->GetName(); }
const std::string& LazyAggTableHandler::GetDatabase() { return left_->GetDatabase(); }
std::shared_ptr<TableHandler> LazyAggPartitionHandler::GetSegment(const std::string& key) {
    auto seg = input_->Left()->GetSegment(key);
    return std::shared_ptr<TableHandler>(new LazyAggTableHandler(seg, input_->Func(), agg_gen_, parameter_));
}
const std::string LazyAggPartitionHandler::GetHandlerTypeName() { return "LazyLastJoinPartitionHandler"; }
std::unique_ptr<RowIterator> LazyAggPartitionHandler::GetIterator() {
    auto it = input_->Left()->GetIterator();
    return std::unique_ptr<RowIterator>(new LazyAggIterator(std::move(it), input_->Func(), agg_gen_, parameter_));
}
base::ConstIterator<uint64_t, Row>* LazyAggPartitionHandler::GetRawIterator() { return nullptr; }
bool ConcatIterator::Valid() const { return left_ && left_->Valid(); }
void ConcatIterator::Next() {
    left_->Next();
    if (right_ && right_->Valid()) {
        right_->Next();
    }
}
const uint64_t& ConcatIterator::GetKey() const { return left_->GetKey(); }
const Row& ConcatIterator::GetValue() {
    if (!right_ || !right_->Valid()) {
        buf_ = Row(left_slices_, left_->GetValue(), right_slices_, Row());
    } else {
        buf_ = Row(left_slices_, left_->GetValue(), right_slices_, right_->GetValue());
    }
    return buf_;
}
void ConcatIterator::Seek(const uint64_t& key) {
    left_->Seek(key);
    if (right_ && right_->Valid()) {
        right_->Seek(key);
    }
}
void ConcatIterator::SeekToFirst() {
    left_->SeekToFirst();
    if (right_) {
        right_->SeekToFirst();
    }
}
std::unique_ptr<RowIterator> SimpleConcatTableHandler::GetIterator() {
    auto p = GetRawIterator();
    if (p == nullptr) {
        return {};
    }
    return std::unique_ptr<RowIterator>(p);
}
RowIterator* SimpleConcatTableHandler::GetRawIterator() {
    auto li = left_->GetIterator();
    if (!li) {
        return nullptr;
    }
    auto ri = right_->GetIterator();
    return new ConcatIterator(std::move(li), left_slices_, std::move(ri), right_slices_);
}
std::unique_ptr<WindowIterator> SimpleConcatTableHandler::GetWindowIterator(const std::string& idx_name) {
    return nullptr;
}
std::unique_ptr<WindowIterator> ConcatPartitionHandler::GetWindowIterator() { return nullptr; }
std::unique_ptr<WindowIterator> ConcatPartitionHandler::GetWindowIterator(const std::string& idx_name) {
    return nullptr;
}

std::unique_ptr<WindowIterator> LazyAggPartitionHandler::GetWindowIterator() {
    auto w = input_->Left()->GetWindowIterator();
    return std::unique_ptr<WindowIterator>(
        new LazyAggWindowIterator(std::move(w), input_->Func(), agg_gen_, parameter_));
}

RowIterator* LazyAggWindowIterator::GetRawValue() {
    auto w = left_->GetValue();
    if (!w) {
        return nullptr;
    }

    return new LazyAggIterator(std::move(w), func_, agg_gen_, parameter_);
}
void LazyRequestUnionIterator::Next() {
    if (Valid()) {
        cur_iter_->Next();
    }
    if (!Valid()) {
        left_->Next();
        OnNewRow();
    }
}
bool LazyRequestUnionIterator::Valid() const { return cur_iter_ && cur_iter_->Valid(); }
void LazyRequestUnionIterator::Seek(const uint64_t& key) {
    left_->Seek(key);
    OnNewRow(false);
}
void LazyRequestUnionIterator::SeekToFirst() {
    left_->SeekToFirst();
    OnNewRow();
}
void LazyRequestUnionIterator::OnNewRow(bool continue_on_empty) {
    while (left_->Valid()) {
        auto row = left_->GetValue();
        auto tb = func_(row);
        if (tb) {
            auto it = tb->GetIterator();
            if (it) {
                it->SeekToFirst();
                if (it->Valid()) {
                    cur_window_ = tb;
                    cur_iter_ = std::move(it);
                    break;
                }
            }
        }

        if (continue_on_empty) {
            left_->Next();
        } else {
            cur_window_ = {};
            cur_iter_ = {};
            break;
        }
    }
}
const uint64_t& LazyRequestUnionIterator::GetKey() const { return cur_iter_->GetKey(); }
const Row& LazyRequestUnionIterator::GetValue() { return cur_iter_->GetValue(); }
RowIterator* LazyRequestUnionWindowIterator::GetRawValue() {
    auto rows = left_->GetValue();
    if (!rows) {
        return {};
    }

    return new LazyRequestUnionIterator(std::move(rows), func_);
}
bool LazyRequestUnionWindowIterator::Valid() { return left_ && left_->Valid(); }
const Row LazyRequestUnionWindowIterator::GetKey() { return left_->GetKey(); }
void LazyRequestUnionWindowIterator::SeekToFirst() { left_->SeekToFirst(); }
void LazyRequestUnionWindowIterator::Seek(const std::string& key) { left_->Seek(key); }
void LazyRequestUnionWindowIterator::Next() { left_->Next(); }
}  // namespace vm
}  // namespace hybridse
