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

#ifndef HYBRIDSE_SRC_VM_CATALOG_WRAPPER_H_
#define HYBRIDSE_SRC_VM_CATALOG_WRAPPER_H_

#include <functional>
#include <memory>
#include <string>
#include <utility>

#include "absl/base/attributes.h"
#include "codec/row_iterator.h"
#include "vm/catalog.h"
#include "vm/generator.h"

namespace hybridse {
namespace vm {

class IteratorProjectWrapper : public RowIterator {
 public:
    IteratorProjectWrapper(std::unique_ptr<RowIterator>&& iter, const Row& parameter, const ProjectFun* fun)
        : RowIterator(), iter_(std::move(iter)), parameter_(parameter), fun_(fun), value_() {}
    virtual ~IteratorProjectWrapper() {}
    bool Valid() const override { return iter_->Valid(); }
    void Next() override { iter_->Next(); }
    const uint64_t& GetKey() const override { return iter_->GetKey(); }
    const Row& GetValue() override {
        value_ = fun_->operator()(iter_->GetValue(), parameter_);
        return value_;
    }
    void Seek(const uint64_t& k) override { iter_->Seek(k); }
    void SeekToFirst() override { iter_->SeekToFirst(); }
    bool IsSeekable() const override { return iter_->IsSeekable(); }
    std::unique_ptr<RowIterator> iter_;
    const Row& parameter_;
    const ProjectFun* fun_;
    Row value_;
};

class IteratorFilterWrapper : public RowIterator {
 public:
    IteratorFilterWrapper(std::unique_ptr<RowIterator>&& iter, const Row& parameter, const PredicateFun* fun)
        : RowIterator(), iter_(std::move(iter)), parameter_(parameter), predicate_(fun) {}

    virtual ~IteratorFilterWrapper() {}

    bool Valid() const override {
        return iter_->Valid() && predicate_->operator()(iter_->GetValue(), parameter_);
    }
    void Next() override {
        iter_->Next();
        while (iter_->Valid() && !predicate_->operator()(iter_->GetValue(), parameter_)) {
            iter_->Next();
        }
    }
    const uint64_t& GetKey() const override { return iter_->GetKey(); }
    const Row& GetValue() override {
        value_ = iter_->GetValue();
        return value_;
    }
    void Seek(const uint64_t& k) override {
        iter_->Seek(k);
        while (iter_->Valid() && !predicate_->operator()(iter_->GetValue(), parameter_)) {
            iter_->Next();
        }
    }
    void SeekToFirst() override {
        iter_->SeekToFirst();
        while (iter_->Valid() && !predicate_->operator()(iter_->GetValue(), parameter_)) {
            iter_->Next();
        }
    }
    bool IsSeekable() const override { return iter_->IsSeekable(); }
    std::unique_ptr<RowIterator> iter_;
    const Row& parameter_;
    const PredicateFun* predicate_;
    Row value_;
};

// iterator start from `iter` but limit rows count
// stop when `iter` is invalid or reaches limit count
class LimitIterator : public RowIterator {
 public:
    explicit LimitIterator(std::unique_ptr<RowIterator>&& iter, int32_t limit)
        : RowIterator(), iter_(std::move(iter)), limit_(limit) {
        SeekToFirst();
    }
    virtual ~LimitIterator() {}

    bool Valid() const override {
        return iter_->Valid() && cnt_ <= limit_;
    }
    void Next() override {
        iter_->Next();
        cnt_++;
    }
    const uint64_t& GetKey() const override {
        return iter_->GetKey();
    }
    const Row& GetValue() override {
        return iter_->GetValue();
    }

    // limit iterator is not seekable
    bool IsSeekable() const override {
        return false;
    };

    void Seek(const uint64_t& key) override {
        LOG(ERROR) << "LimitIterator is not seekable";
    }

    void SeekToFirst() override {
        // not lazy
        // seek to the first valid row
        // so it correctly handle limit(filter iterator)
        iter_->SeekToFirst();
    };

 private:
    std::unique_ptr<RowIterator> iter_;
    int32_t cnt_ = 1;
    // limit_ inherited from sql limit clause, 0 means no no rows will return
    const int32_t limit_ = 0;
};

class WindowIteratorProjectWrapper : public WindowIterator {
 public:
    WindowIteratorProjectWrapper(std::unique_ptr<WindowIterator> iter,
                                 const Row& parameter,
                                 const ProjectFun* fun)
        : WindowIterator(), iter_(std::move(iter)), parameter_(parameter), fun_(fun) {}
    virtual ~WindowIteratorProjectWrapper() {}
    RowIterator* GetRawValue() override {
        auto iter = iter_->GetValue();
        if (!iter) {
            return nullptr;
        } else {
            return new IteratorProjectWrapper(std::move(iter), parameter_, fun_);
        }
    }
    void Seek(const std::string& key) override { iter_->Seek(key); }
    void SeekToFirst() override { iter_->SeekToFirst(); }
    void Next() override { iter_->Next(); }
    bool Valid() override { return iter_->Valid(); }
    const Row GetKey() override { return iter_->GetKey(); }
    std::unique_ptr<WindowIterator> iter_;
    const Row& parameter_;
    const ProjectFun* fun_;
};

class WindowIteratorFilterWrapper : public WindowIterator {
 public:
    WindowIteratorFilterWrapper(std::unique_ptr<WindowIterator> iter,
                                const Row& parameter,
                                const PredicateFun* fun)
        : WindowIterator(), iter_(std::move(iter)), parameter_(parameter), fun_(fun) {}
    virtual ~WindowIteratorFilterWrapper() {}
    RowIterator* GetRawValue() override {
        auto iter = iter_->GetValue();
        if (!iter) {
            return nullptr;
        } else {
            return new IteratorFilterWrapper(std::move(iter), parameter_, fun_);
        }
    }
    void Seek(const std::string& key) override { iter_->Seek(key); }
    void SeekToFirst() override { iter_->SeekToFirst(); }
    void Next() override { iter_->Next(); }
    bool Valid() override { return iter_->Valid(); }
    const Row GetKey() override { return iter_->GetKey(); }
    std::unique_ptr<WindowIterator> iter_;
    const Row& parameter_;
    const PredicateFun* fun_;
};

class TableProjectWrapper;
class TableFilterWrapper;

class PartitionProjectWrapper : public PartitionHandler {
 public:
    PartitionProjectWrapper(std::shared_ptr<PartitionHandler> partition_handler,
                            const Row& parameter,
                            const ProjectFun* fun)
        : PartitionHandler(),
          partition_handler_(partition_handler),
          parameter_(parameter),
          value_(),
          fun_(fun) {}
    virtual ~PartitionProjectWrapper() {}
    std::unique_ptr<WindowIterator> GetWindowIterator() override {
        auto iter = partition_handler_->GetWindowIterator();
        if (!iter) {
            return std::unique_ptr<WindowIterator>();
        } else {
            return std::unique_ptr<WindowIterator>(
                new WindowIteratorProjectWrapper(std::move(iter), parameter_, fun_));
        }
    }
    const Types& GetTypes() override { return partition_handler_->GetTypes(); }
    const IndexHint& GetIndex() override {
        return partition_handler_->GetIndex();
    }

    const Schema* GetSchema() override {
        return partition_handler_->GetSchema();
    }
    const std::string& GetName() override {
        return partition_handler_->GetName();
    }
    const std::string& GetDatabase() override {
        return partition_handler_->GetDatabase();
    }
    codec::RowIterator* GetRawIterator() override;
    Row At(uint64_t pos) override {
        value_ = fun_->operator()(partition_handler_->At(pos), parameter_);
        return value_;
    }
    const uint64_t GetCount() override {
        return partition_handler_->GetCount();
    }

    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const OrderType GetOrderType() const override { return partition_handler_->GetOrderType(); }

    const std::string GetHandlerTypeName() override {
        return "PartitionHandler";
    }
    std::shared_ptr<PartitionHandler> partition_handler_;
    const Row& parameter_;
    Row value_;
    const ProjectFun* fun_;
};
class PartitionFilterWrapper : public PartitionHandler {
 public:
    PartitionFilterWrapper(std::shared_ptr<PartitionHandler> partition_handler,
                           const Row& parameter,
                           const PredicateFun* fun)
        : PartitionHandler(),
          partition_handler_(partition_handler),
          parameter_(parameter),
          fun_(fun) {}
    virtual ~PartitionFilterWrapper() {}
    std::unique_ptr<WindowIterator> GetWindowIterator() override {
        auto iter = partition_handler_->GetWindowIterator();
        if (!iter) {
            return std::unique_ptr<WindowIterator>();
        } else {
            return std::unique_ptr<WindowIterator>(
                new WindowIteratorFilterWrapper(std::move(iter), parameter_, fun_));
        }
    }
    const Types& GetTypes() override { return partition_handler_->GetTypes(); }
    const IndexHint& GetIndex() override {
        return partition_handler_->GetIndex();
    }

    const Schema* GetSchema() override {
        return partition_handler_->GetSchema();
    }
    const std::string& GetName() override {
        return partition_handler_->GetName();
    }
    const std::string& GetDatabase() override {
        return partition_handler_->GetDatabase();
    }

    codec::RowIterator* GetRawIterator() override;

    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const OrderType GetOrderType() const override { return partition_handler_->GetOrderType(); }

    const std::string GetHandlerTypeName() override {
        return "PartitionHandler";
    }
    std::shared_ptr<PartitionHandler> partition_handler_;
    const Row& parameter_;
    const PredicateFun* fun_;
};

class TableProjectWrapper : public TableHandler {
 public:
    TableProjectWrapper(std::shared_ptr<TableHandler> table_handler,
                        const Row& parameter,
                        const ProjectFun* fun)
        : TableHandler(), table_hander_(table_handler), parameter_(parameter), value_(), fun_(fun) {}
    virtual ~TableProjectWrapper() {}

    const Types& GetTypes() override { return table_hander_->GetTypes(); }
    const IndexHint& GetIndex() override { return table_hander_->GetIndex(); }
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) override {
        auto iter = table_hander_->GetWindowIterator(idx_name);
        if (!iter) {
            return std::unique_ptr<WindowIterator>();
        } else {
            return std::unique_ptr<WindowIterator>(
                new WindowIteratorProjectWrapper(std::move(iter), parameter_, fun_));
        }
    }
    const Schema* GetSchema() override { return table_hander_->GetSchema(); }
    const std::string& GetName() override { return table_hander_->GetName(); }
    const std::string& GetDatabase() override {
        return table_hander_->GetDatabase();
    }
    codec::RowIterator* GetRawIterator() override {
        auto iter = table_hander_->GetIterator();
        if (!iter) {
            return nullptr;
        } else {
            return new IteratorProjectWrapper(std::move(iter), parameter_, fun_);
        }
    }
    Row At(uint64_t pos) override {
        value_ = fun_->operator()(table_hander_->At(pos), parameter_);
        return value_;
    }

    const uint64_t GetCount() override { return table_hander_->GetCount(); }
    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override;
    const OrderType GetOrderType() const override { return table_hander_->GetOrderType(); }

    std::shared_ptr<TableHandler> table_hander_;
    const Row& parameter_;
    Row value_;
    const ProjectFun* fun_;
};

class TableFilterWrapper : public TableHandler {
 public:
    TableFilterWrapper(std::shared_ptr<TableHandler> table_handler, const Row& parameter, const PredicateFun* fun)
        : TableHandler(), table_hander_(table_handler), parameter_(parameter), fun_(fun) {}
    virtual ~TableFilterWrapper() {}

    const Types& GetTypes() override { return table_hander_->GetTypes(); }
    const IndexHint& GetIndex() override { return table_hander_->GetIndex(); }

    std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) override {
        auto iter = table_hander_->GetWindowIterator(idx_name);
        if (!iter) {
            return std::unique_ptr<WindowIterator>();
        } else {
            return std::make_unique<WindowIteratorFilterWrapper>(std::move(iter), parameter_, fun_);
        }
    }

    const Schema* GetSchema() override { return table_hander_->GetSchema(); }
    const std::string& GetName() override { return table_hander_->GetName(); }
    const std::string& GetDatabase() override { return table_hander_->GetDatabase(); }
    codec::RowIterator* GetRawIterator() override {
        auto iter = table_hander_->GetIterator();
        if (!iter) {
            return nullptr;
        } else {
            return new IteratorFilterWrapper(std::move(iter), parameter_, fun_);
        }
    }
    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override;
    const OrderType GetOrderType() const override { return table_hander_->GetOrderType(); }

 private:
    std::shared_ptr<TableHandler> table_hander_;
    const Row& parameter_;
    Row value_;
    const PredicateFun* fun_;
};

class LimitTableHandler final : public TableHandler {
 public:
    explicit LimitTableHandler(std::shared_ptr<TableHandler> table, int32_t limit)
        : TableHandler(), table_hander_(table), limit_(limit) {}
    virtual ~LimitTableHandler() {}

    // FIXME(ace): do not use this, not implemented
    std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) override {
        LOG(ERROR) << "window iterator for LimitTableHandler is not implemented, don't use";
        return table_hander_->GetWindowIterator(idx_name);
    }

    codec::RowIterator* GetRawIterator() override {
        auto iter = table_hander_->GetIterator();
        if (!iter) {
            return nullptr;
        } else {
            return new LimitIterator(std::move(iter), limit_);
        }
    }

    const Types& GetTypes() override { return table_hander_->GetTypes(); }
    const IndexHint& GetIndex() override { return table_hander_->GetIndex(); }
    const Schema* GetSchema() override { return table_hander_->GetSchema(); }
    const std::string& GetName() override { return table_hander_->GetName(); }
    const std::string& GetDatabase() override { return table_hander_->GetDatabase(); }

    // FIXME(ace): do not use this, not implemented
    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override {
        LOG(ERROR) << "Get partition for LimitTableHandler is not implemented, don't use";
        return table_hander_->GetPartition(index_name);
    }

    const OrderType GetOrderType() const override { return table_hander_->GetOrderType(); }

 private:
    std::shared_ptr<TableHandler> table_hander_;
    int32_t limit_;
};

class RowProjectWrapper : public RowHandler {
 public:
    RowProjectWrapper(std::shared_ptr<RowHandler> row_handler,
                      const Row& parameter,
                      const ProjectFun* fun)
        : RowHandler(), row_handler_(row_handler), parameter_(parameter), value_(), fun_(fun) {}
    virtual ~RowProjectWrapper() {}
    const Row& GetValue() override {
        auto& row = row_handler_->GetValue();
        if (row.empty()) {
            value_ = row;
            return value_;
        }
        value_ = fun_->operator()(row, parameter_);
        return value_;
    }
    const Schema* GetSchema() override { return row_handler_->GetSchema(); }
    const std::string& GetName() override { return row_handler_->GetName(); }
    const std::string& GetDatabase() override {
        return row_handler_->GetDatabase();
    }
    std::shared_ptr<RowHandler> row_handler_;
    const Row& parameter_;
    Row value_;
    const ProjectFun* fun_;
};
class RowCombineWrapper : public RowHandler {
 public:
    RowCombineWrapper(std::shared_ptr<RowHandler> left, size_t left_slices,
                      std::shared_ptr<RowHandler> right, size_t right_slices)
        : RowHandler(),
          status_(base::Status::Running()),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          left_(left),
          left_slices_(left_slices),
          right_(right),
          right_slices_(right_slices),
          value_() {}
    virtual ~RowCombineWrapper() {}
    const Row& GetValue() override {
        if (!status_.isRunning()) {
            return value_;
        }
        if (!left_) {
            status_ = base::Status::OK();
            value_ = Row();
            return value_;
        }
        auto left_row =
            std::dynamic_pointer_cast<RowHandler>(left_)->GetValue();
        if (!right_) {
            value_ = Row(left_slices_, left_row, right_slices_, Row());
            status_ = base::Status::OK();
            return value_;
        }
        if (kRowHandler == right_->GetHandlerType()) {
            auto right_row =
                std::dynamic_pointer_cast<RowHandler>(right_)->GetValue();
            value_ = Row(left_slices_, left_row, right_slices_, right_row);
        } else if (kTableHandler == right_->GetHandlerType()) {
            auto right_table = std::dynamic_pointer_cast<TableHandler>(right_);
            auto right_iter = right_table->GetIterator();
            if (!right_iter) {
                value_ = Row(left_slices_, left_row, right_slices_, Row());
            } else {
                right_iter->SeekToFirst();
                value_ = Row(left_slices_, left_row, right_slices_,
                             right_iter->GetValue());
            }
        } else {
            value_ = Row(left_slices_, left_row, right_slices_, Row());
        }
        status_ = base::Status::OK();
        return value_;
    }
    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }
    base::Status status_;
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    std::shared_ptr<RowHandler> left_;
    size_t left_slices_;
    std::shared_ptr<RowHandler> right_;
    size_t right_slices_;
    Row value_;
    const ProjectFun* fun_;
};

// Last Join iterator on demand
// for request mode, right source must be a PartitionHandler
class LazyLastJoinIterator : public RowIterator {
 public:
    LazyLastJoinIterator(std::unique_ptr<RowIterator>&& left, std::shared_ptr<DataHandler> right, const Row& param,
                         std::shared_ptr<JoinGenerator> join) ABSL_ATTRIBUTE_NONNULL()
        : left_it_(std::move(left)), right_(right), parameter_(param), join_(join) {
        SeekToFirst();
    }

    ~LazyLastJoinIterator() override {}

    bool Valid() const override;
    void Next() override;
    const uint64_t& GetKey() const override;
    const Row& GetValue() override;

    bool IsSeekable() const override { return true; };

    void Seek(const uint64_t& key) override;

    void SeekToFirst() override;

 private:
    std::unique_ptr<RowIterator> left_it_;
    std::shared_ptr<DataHandler>  right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;

    Row value_;
};
class LazyLeftJoinIterator : public RowIterator {
 public:
    LazyLeftJoinIterator(std::unique_ptr<RowIterator>&& left, std::shared_ptr<DataHandler> right, const Row& param,
                         std::shared_ptr<JoinGenerator> join)
    : left_it_(std::move(left)), right_(right), parameter_(param), join_(join) {
        if (right_->GetHandlerType() == kPartitionHandler) {
            right_partition_ = std::dynamic_pointer_cast<PartitionHandler>(right_);
        }
        SeekToFirst();
    }

    ~LazyLeftJoinIterator() override {}

    bool Valid() const override { return left_it_->Valid(); }

    // actual compute performed here, left_it_ and right_it_ is updated to the next position of join
    void Next() override;

    const uint64_t& GetKey() const override {
        return left_it_->GetKey();
    }

    const Row& GetValue() override {
        return value_;
    }

    bool IsSeekable() const override { return true; };

    void Seek(const uint64_t& key) override {
        left_it_->Seek(key);
        onNewLeftRow();
    }

    void SeekToFirst() override {
        left_it_->SeekToFirst();
        onNewLeftRow();
    }

 private:
    // left_value_ changed, update right_it_ based on join condition
    void onNewLeftRow();

    std::unique_ptr<RowIterator> left_it_;
    std::shared_ptr<DataHandler> right_;
    std::shared_ptr<PartitionHandler> right_partition_;
    const Row parameter_;
    std::shared_ptr<JoinGenerator> join_;

    // whether current left row has any rows from right joined, left join fallback to NULL if non matches
    bool matches_right_ = false;
    std::unique_ptr<RowIterator> right_it_;
    Row left_value_;
    Row value_;
};

class LazyJoinPartitionHandler final : public PartitionHandler {
 public:
    LazyJoinPartitionHandler(std::shared_ptr<PartitionHandler> left, std::shared_ptr<DataHandler> right,
                             const Row& param, std::shared_ptr<JoinGenerator> join);
    ~LazyJoinPartitionHandler() override {}

    // NOTE: only support get segement by key from left source
    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const std::string GetHandlerTypeName() override;

    std::unique_ptr<WindowIterator> GetWindowIterator() override;

    codec::RowIterator* GetRawIterator() override;

    const IndexHint& GetIndex() override { return left_->GetIndex(); }

    // unimplemented
    const Types& GetTypes() override { return left_->GetTypes(); }

    // unimplemented
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return left_->GetName(); }
    const std::string& GetDatabase() override { return left_->GetDatabase(); }

 private:
    std::shared_ptr<PartitionHandler> left_;
    std::shared_ptr<DataHandler> right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;
};

class LazyJoinTableHandler final : public TableHandler {
 public:
    LazyJoinTableHandler(std::shared_ptr<TableHandler> left, std::shared_ptr<DataHandler> right, const Row& param,
                         std::shared_ptr<JoinGenerator> join)
        : left_(left), right_(right), parameter_(param), join_(join) {
    }

    ~LazyJoinTableHandler() override {}

    // unimplemented
    const Types& GetTypes() override { return left_->GetTypes(); }

    const IndexHint& GetIndex() override { return left_->GetIndex(); }

    // unimplemented
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return left_->GetName(); }
    const std::string& GetDatabase() override { return left_->GetDatabase(); }

    codec::RowIterator* GetRawIterator() override;

    const uint64_t GetCount() override { return left_->GetCount(); }

    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override;

    const OrderType GetOrderType() const override { return left_->GetOrderType(); }

    const std::string GetHandlerTypeName() override;

 private:
    std::shared_ptr<TableHandler> left_;
    std::shared_ptr<DataHandler> right_;
    const Row parameter_;
    std::shared_ptr<JoinGenerator> join_;
};

class LazyJoinWindowIterator final : public codec::WindowIterator {
 public:
    LazyJoinWindowIterator(std::unique_ptr<WindowIterator>&& iter, std::shared_ptr<DataHandler> right, const Row& param,
                           std::shared_ptr<JoinGenerator> join);

    ~LazyJoinWindowIterator() override {}

    codec::RowIterator* GetRawValue() override;

    void Seek(const std::string& key) override { left_->Seek(key); }
    void SeekToFirst() override { left_->SeekToFirst(); }
    void Next() override { left_->Next(); }
    bool Valid() override { return left_ && left_->Valid(); }
    const Row GetKey() override { return left_->GetKey(); }

    std::shared_ptr<WindowIterator> left_;
    std::shared_ptr<DataHandler> right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;
};

class LazyRequestUnionIterator final : public RowIterator {
 public:
    LazyRequestUnionIterator(std::unique_ptr<RowIterator>&& left,
                             std::function<std::shared_ptr<TableHandler>(const Row&)> func)
        : left_(std::move(left)), func_(func) {
        SeekToFirst();
    }
    ~LazyRequestUnionIterator() override {}

    bool Valid() const override;
    void Next() override;
    const uint64_t& GetKey() const override;
    const Row& GetValue() override;
    bool IsSeekable() const override { return true; }

    void Seek(const uint64_t& key) override;
    void SeekToFirst() override;

 private:
    void OnNewRow(bool continue_on_empty = true);

 private:
    // all same keys from left form a window, although it is better that every row be a partition
    std::unique_ptr<RowIterator> left_;
    std::function<std::shared_ptr<TableHandler>(const Row&)> func_;

    std::shared_ptr<TableHandler> cur_window_;
    std::unique_ptr<RowIterator> cur_iter_;
};

class LazyRequestUnionWindowIterator final : public codec::WindowIterator {
 public:
    LazyRequestUnionWindowIterator(std::unique_ptr<WindowIterator>&& left,
                                     std::function<std::shared_ptr<TableHandler>(const Row&)> func)
        : left_(std::move(left)), func_(func) {
        SeekToFirst();
    }
    ~LazyRequestUnionWindowIterator() override {}

    RowIterator* GetRawValue() override;

    void Seek(const std::string& key) override;
    void SeekToFirst() override;
    void Next() override;
    bool Valid() override;
    const Row GetKey() override;

 private:
    std::unique_ptr<WindowIterator> left_;
    std::function<std::shared_ptr<TableHandler>(const Row&)> func_;
};

class LazyRequestUnionPartitionHandler final : public PartitionHandler {
 public:
    LazyRequestUnionPartitionHandler(std::shared_ptr<PartitionHandler> left,
                                     std::function<std::shared_ptr<TableHandler>(const Row&)> func)
        : left_(left), func_(func) {}
    ~LazyRequestUnionPartitionHandler() override {}

    std::unique_ptr<WindowIterator> GetWindowIterator() override;

    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const std::string GetHandlerTypeName() override { return "LazyRequestUnionPartitiontHandler"; }

    codec::RowIterator* GetRawIterator() override;

    const IndexHint& GetIndex() override;

    // unimplemented
    const Types& GetTypes() override;

    // unimplemented
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return left_->GetName(); }
    const std::string& GetDatabase() override { return left_->GetDatabase(); }

    auto Left() const { return left_; }
    auto Func() const { return func_; }

 private:
    std::shared_ptr<PartitionHandler> left_;
    std::function<std::shared_ptr<TableHandler>(const Row&)> func_;
};

class LazyAggIterator final : public RowIterator {
 public:
    LazyAggIterator(std::unique_ptr<RowIterator>&& it, std::function<std::shared_ptr<TableHandler>(const Row&)> func,
                    std::shared_ptr<AggGenerator> agg_gen, const Row& param)
        : it_(std::move(it)), func_(func), agg_gen_(agg_gen), parameter_(param) {
        SeekToFirst();
    }

    ~LazyAggIterator() override {}

    bool Valid() const override;
    void Next() override;
    const uint64_t& GetKey() const override;
    const Row& GetValue() override;
    bool IsSeekable() const override { return true; }

    void Seek(const uint64_t& key) override;
    void SeekToFirst() override;

 private:
    std::unique_ptr<RowIterator> it_;
    std::function<std::shared_ptr<TableHandler>(const Row&)> func_;
    std::shared_ptr<AggGenerator> agg_gen_;
    const Row& parameter_;

    Row buf_;
};

class LazyAggTableHandler final : public TableHandler {
 public:
    LazyAggTableHandler(std::shared_ptr<TableHandler> left,
                        std::function<std::shared_ptr<TableHandler>(const Row&)> func,
                        std::shared_ptr<AggGenerator> agg_gen, const Row& param)
        : left_(left), func_(func), agg_gen_(agg_gen), parameter_(param) {
        DLOG(INFO) << "iterator count = " << left_->GetCount();
    }
    ~LazyAggTableHandler() override {}

    RowIterator* GetRawIterator() override;

    // unimplemented
    const Types& GetTypes() override;
    const IndexHint& GetIndex() override;
    const Schema* GetSchema() override;
    const std::string& GetName() override;
    const std::string& GetDatabase() override;

 private:
    std::shared_ptr<TableHandler> left_;
    std::function<std::shared_ptr<TableHandler>(const Row&)> func_;
    std::shared_ptr<AggGenerator> agg_gen_;
    const Row& parameter_;
};

class LazyAggWindowIterator final : public codec::WindowIterator {
 public:
    LazyAggWindowIterator(std::unique_ptr<codec::WindowIterator> left,
                          std::function<std::shared_ptr<TableHandler>(const Row&)> func,
                          std::shared_ptr<AggGenerator> gen, const Row& p)
        : left_(std::move(left)), func_(func), agg_gen_(gen), parameter_(p) {}
    ~LazyAggWindowIterator() override {}

    RowIterator* GetRawValue() override;

    void Seek(const std::string& key) override { left_->Seek(key); }
    void SeekToFirst() override { left_->SeekToFirst(); }
    void Next() override { left_->Next(); }
    bool Valid() override { return left_ && left_->Valid(); }
    const Row GetKey() override { return left_->GetKey(); }

 private:
    std::unique_ptr<codec::WindowIterator> left_;
    std::function<std::shared_ptr<TableHandler>(const Row&)> func_;
    std::shared_ptr<AggGenerator> agg_gen_;
    const Row& parameter_;
};

class LazyAggPartitionHandler final : public PartitionHandler {
 public:
    LazyAggPartitionHandler(std::shared_ptr<LazyRequestUnionPartitionHandler> input,
                            std::shared_ptr<AggGenerator> agg_gen, const Row& param)
        : input_(input), agg_gen_(agg_gen), parameter_(param) {}
    ~LazyAggPartitionHandler() override {}

    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const std::string GetHandlerTypeName() override;

    codec::RowIterator* GetRawIterator() override;

    std::unique_ptr<WindowIterator> GetWindowIterator() override;

    const IndexHint& GetIndex() override { return input_->GetIndex(); }

    // unimplemented
    const Types& GetTypes() override { return input_->GetTypes(); }
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return input_->GetName(); }
    const std::string& GetDatabase() override { return input_->GetDatabase(); }

 private:
    std::shared_ptr<LazyRequestUnionPartitionHandler> input_;
    std::shared_ptr<AggGenerator> agg_gen_;
    const Row& parameter_;
};

class ConcatIterator final : public RowIterator {
 public:
    ConcatIterator(std::unique_ptr<RowIterator>&& left, size_t left_slices, std::unique_ptr<RowIterator>&& right,
                   size_t right_slices)
        : left_(std::move(left)), left_slices_(left_slices), right_(std::move(right)), right_slices_(right_slices) {
        SeekToFirst();
    }
    ~ConcatIterator() override {}

    bool Valid() const override;
    void Next() override;
    const uint64_t& GetKey() const override;
    const Row& GetValue() override;

    bool IsSeekable() const override { return true; };

    void Seek(const uint64_t& key) override;

    void SeekToFirst() override;

 private:
    std::unique_ptr<RowIterator> left_;
    size_t left_slices_;
    std::unique_ptr<RowIterator> right_;
    size_t right_slices_;

    Row buf_;
};

class SimpleConcatTableHandler final : public TableHandler {
 public:
    SimpleConcatTableHandler(std::shared_ptr<TableHandler> left, size_t left_slices,
                           std::shared_ptr<TableHandler> right, size_t right_slices)
        : left_(left), left_slices_(left_slices), right_(right), right_slices_(right_slices) {}
    ~SimpleConcatTableHandler() override {}

    RowIterator* GetRawIterator() override;

    const Types& GetTypes() override { return left_->GetTypes(); }

    const IndexHint& GetIndex() override { return left_->GetIndex(); }

    // unimplemented
    const Schema* GetSchema() override { return left_->GetSchema(); }
    const std::string& GetName() override { return left_->GetName(); }
    const std::string& GetDatabase() override { return left_->GetDatabase(); }

 private:
    std::shared_ptr<TableHandler> left_;
    size_t left_slices_;
    std::shared_ptr<TableHandler> right_;
    size_t right_slices_;
};

class ConcatPartitionHandler final : public PartitionHandler {
 public:
    ConcatPartitionHandler(std::shared_ptr<PartitionHandler> left, size_t left_slices,
                           std::shared_ptr<PartitionHandler> right, size_t right_slices)
        : left_(left), left_slices_(left_slices), right_(right), right_slices_(right_slices) {}
    ~ConcatPartitionHandler() override {}

    RowIterator* GetRawIterator() override;

    std::unique_ptr<WindowIterator> GetWindowIterator() override;

    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const Types& GetTypes() override { return left_->GetTypes(); }

    const IndexHint& GetIndex() override { return left_->GetIndex(); }

    // unimplemented
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return left_->GetName(); }
    const std::string& GetDatabase() override { return left_->GetDatabase(); }

 private:
    std::shared_ptr<PartitionHandler> left_;
    size_t left_slices_;
    std::shared_ptr<PartitionHandler> right_;
    size_t right_slices_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_CATALOG_WRAPPER_H_
