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
codec::RowIterator* PartitionProjectWrapper::GetRawIterator() {
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
codec::RowIterator* PartitionFilterWrapper::GetRawIterator() {
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

void LazyLastJoinIterator::Seek(const uint64_t& key) { left_it_->Seek(key); }

void LazyLastJoinIterator::SeekToFirst() { left_it_->SeekToFirst(); }

const uint64_t& LazyLastJoinIterator::GetKey() const { return left_it_->GetKey(); }

void LazyLastJoinIterator::Next() { left_it_->Next(); }

bool LazyLastJoinIterator::Valid() const { return left_it_ && left_it_->Valid(); }

LazyJoinPartitionHandler::LazyJoinPartitionHandler(std::shared_ptr<PartitionHandler> left,
                                                   std::shared_ptr<DataHandler> right, const Row& param,
                                                   std::shared_ptr<JoinGenerator> join)
    : left_(left), right_(right), parameter_(param), join_(join) {}

std::shared_ptr<TableHandler> LazyJoinPartitionHandler::GetSegment(const std::string& key) {
    auto left_seg = left_->GetSegment(key);
    return std::shared_ptr<TableHandler>(new LazyJoinTableHandler(left_seg, right_, parameter_, join_));
}

std::shared_ptr<PartitionHandler> LazyJoinTableHandler::GetPartition(const std::string& index_name) {
    return std::shared_ptr<PartitionHandler>(
        new LazyJoinPartitionHandler(left_->GetPartition(index_name), right_, parameter_, join_));
}

codec::RowIterator* LazyJoinPartitionHandler::GetRawIterator() {
    auto iter = left_->GetIterator();
    if (!iter) {
        return nullptr;
    }
    return new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_);
}

std::unique_ptr<WindowIterator> LazyJoinPartitionHandler::GetWindowIterator() {
    auto wi = left_->GetWindowIterator();
    if (wi == nullptr) {
        return std::unique_ptr<WindowIterator>();
    }

    return std::unique_ptr<WindowIterator>(new LazyJoinWindowIterator(std::move(wi), right_, parameter_, join_));
}

const Row& LazyLastJoinIterator::GetValue() {
    value_ = join_->RowLastJoin(left_it_->GetValue(), right_, parameter_);
    return value_;
}

codec::RowIterator* LazyJoinTableHandler::GetRawIterator() {
    auto iter = left_->GetIterator();
    if (!iter) {
        return {};
    }

    switch (join_->join_type_) {
        case node::kJoinTypeLast:
            return new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_);
        case node::kJoinTypeLeft:
            return new LazyLeftJoinIterator(std::move(iter), right_, parameter_, join_);
        default:
            return {};
    }
}

LazyJoinWindowIterator::LazyJoinWindowIterator(std::unique_ptr<WindowIterator>&& iter,
                                               std::shared_ptr<DataHandler> right, const Row& param,
                                               std::shared_ptr<JoinGenerator> join)
    : left_(std::move(iter)), right_(right), parameter_(param), join_(join) {}

codec::RowIterator* LazyJoinWindowIterator::GetRawValue() {
    auto iter = left_->GetValue();
    if (!iter) {
        return nullptr;
    }

    switch (join_->join_type_) {
        case node::kJoinTypeLast:
            return new LazyLastJoinIterator(std::move(iter), right_, parameter_, join_);
        case node::kJoinTypeLeft:
            return new LazyLeftJoinIterator(std::move(iter), right_, parameter_, join_);
        default:
            return {};
    }
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

const IndexHint& LazyRequestUnionPartitionHandler::GetIndex() { return left_->GetIndex(); }

const Types& LazyRequestUnionPartitionHandler::GetTypes() { return left_->GetTypes(); }

codec::RowIterator* LazyRequestUnionPartitionHandler::GetRawIterator() { return nullptr; }

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

codec::RowIterator* LazyAggTableHandler::GetRawIterator() {
    auto it = left_->GetIterator();
    if (!it) {
        return nullptr;
    }
    return new LazyAggIterator(std::move(it), func_, agg_gen_, parameter_);
}

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

codec::RowIterator* LazyAggPartitionHandler::GetRawIterator() {
    auto it = input_->Left()->GetIterator();
    return new LazyAggIterator(std::move(it), input_->Func(), agg_gen_, parameter_);
}

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
RowIterator* SimpleConcatTableHandler::GetRawIterator() {
    auto li = left_->GetIterator();
    if (!li) {
        return nullptr;
    }
    auto ri = right_->GetIterator();
    return new ConcatIterator(std::move(li), left_slices_, std::move(ri), right_slices_);
}
std::unique_ptr<WindowIterator> ConcatPartitionHandler::GetWindowIterator() { return nullptr; }

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
const std::string LazyJoinPartitionHandler::GetHandlerTypeName() {
    return "LazyJoinPartitionHandler(" + node::JoinTypeName(join_->join_type_) + ")";
}
const std::string LazyJoinTableHandler::GetHandlerTypeName() {
    return "LazyJoinTableHandler(" + node::JoinTypeName(join_->join_type_) + ")";
}
void LazyLeftJoinIterator::Next() {
    if (right_it_ && right_it_->Valid()) {
        right_it_->Next();
        auto res = join_->RowJoinIterator(left_value_, right_it_, parameter_);
        matches_right_ |= res.second;
        if (matches_right_ && !right_it_->Valid()) {
            // matched from right somewhere, skip the NULL match
            left_it_->Next();
            onNewLeftRow();
        } else {
            // RowJoinIterator returns NULL match by default
            value_ = res.first;
        }
    } else {
        left_it_->Next();
        onNewLeftRow();
    }
}
void LazyLeftJoinIterator::onNewLeftRow() {
    // reset
    right_it_ = nullptr;
    left_value_ = Row();
    value_ = Row();
    matches_right_ = false;

    if (!left_it_->Valid()) {
        // end of iterator
        return;
    }

    left_value_ = left_it_->GetValue();
    if (right_partition_) {
        right_it_ = join_->InitRight(left_value_, right_partition_, parameter_);
    } else {
        right_it_ = right_->GetIterator();
        right_it_->SeekToFirst();
    }

    auto res = join_->RowJoinIterator(left_value_, right_it_, parameter_);
    value_ = res.first;
    matches_right_ |= res.second;
}
void UnionIterator::Next() {
    auto top = keys_.top();
    keys_.pop();
    inputs_.at(top.second)->Next();

    if (inputs_.at(top.second)->Valid()) {
        keys_.emplace(inputs_.at(top.second)->GetKey(), top.second);
    }
}
const uint64_t& UnionIterator::GetKey() const {
    if (Valid()) {
        auto& top = keys_.top();
        return inputs_.at(top.second)->GetKey();
    }

    return INVALID_KEY;
}
const Row& UnionIterator::GetValue() {
    if (Valid()) {
        auto& top = keys_.top();
        return inputs_.at(top.second)->GetValue();
    }

    return INVALID_ROW;
}
void UnionIterator::Seek(const uint64_t& key) {
    for (auto& n : inputs_) {
        n->Seek(key);
    }
    rebuild_keys();
}
void UnionIterator::SeekToFirst() {
    for (auto& n : inputs_) {
        n->SeekToFirst();
    }
    rebuild_keys();
}
void UnionIterator::rebuild_keys() {
    keys_ = {};
    for (size_t i = 0; i < inputs_.size(); ++i) {
        if (inputs_[i]->Valid()) {
            keys_.emplace(inputs_[i]->GetKey(), i);
        }
    }
}
RowIterator* SetOperationHandler::GetRawIterator() {
    switch (op_type_) {
        case node::SetOperationType::UNION: {
            std::vector<std::unique_ptr<RowIterator> > iters;
            for (auto tb : inputs_) {
                iters.emplace_back(tb->GetIterator());
            }
            return new UnionIterator(absl::MakeSpan(iters), distinct_);
        }
        default:
            return nullptr;
    }
}
RowIterator* UnionWindowIterator::GetRawValue() {
    std::vector<std::unique_ptr<codec::RowIterator>> iters;
    if (Valid()) {
        auto& idxs = keys_.begin()->second;
        for (auto i : idxs) {
            iters.push_back(inputs_.at(i)->GetValue());
        }
    }

    return new UnionIterator(absl::MakeSpan(iters), distinct_);
}
void UnionWindowIterator::Seek(const std::string& key) {
    for (auto& i : inputs_) {
        i->Seek(key);
    }
    rebuild_keys();
}
void UnionWindowIterator::SeekToFirst() {
    for (auto& i : inputs_) {
        i->SeekToFirst();
    }
    rebuild_keys();
}
void UnionWindowIterator::Next() {
    auto idxs = keys_.begin()->second;
    keys_.erase(keys_.begin());
    for (auto i : idxs) {
        inputs_.at(i)->Next();
        if (inputs_.at(i)->Valid()) {
            keys_[inputs_.at(i)->GetKey()].push_back(i);
        }
    }
}
const codec::Row UnionWindowIterator::GetKey() {
    if (Valid()) {
        return keys_.begin()->first;
    }

    return INVALID_ROW;
}
void UnionWindowIterator::rebuild_keys() {
    keys_.clear();
    for (size_t i = 0; i < inputs_.size(); i++) {
        if (inputs_[i]->Valid()) {
            keys_[inputs_[i]->GetKey()].push_back(i);
        }
    }
}
RowIterator* SetOperationPartitionHandler::GetRawIterator() {
    switch (op_type_) {
        case node::SetOperationType::UNION: {
            std::vector<std::unique_ptr<RowIterator>> iters;
            for (auto tb : inputs_) {
                iters.emplace_back(tb->GetIterator());
            }
            return new UnionIterator(absl::MakeSpan(iters), distinct_);
        }
        default:
            return nullptr;
    }
}
std::shared_ptr<TableHandler> SetOperationPartitionHandler::GetSegment(const std::string& key) {
    std::vector<std::shared_ptr<TableHandler>> segs;
    for (auto n : inputs_) {
        segs.push_back(n->GetSegment(key));
    }

    return std::shared_ptr<TableHandler>(new SetOperationHandler(op_type_, segs, distinct_));
}
std::unique_ptr<WindowIterator> SetOperationPartitionHandler::GetWindowIterator() {
    // NOTE: window iterator may out-of-order, use 'GetSegment' if ordering is mandatory
    if (op_type_ != node::SetOperationType::UNION) {
        return {};
    }

    std::vector<std::unique_ptr<codec::WindowIterator>> iters;
    for (auto n : inputs_) {
        iters.push_back(n->GetWindowIterator());
    }

    return std::unique_ptr<WindowIterator>(new UnionWindowIterator(absl::MakeSpan(iters), distinct_));
}
}  // namespace vm
}  // namespace hybridse
