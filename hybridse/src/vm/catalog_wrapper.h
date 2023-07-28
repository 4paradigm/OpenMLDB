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

#include <memory>
#include <string>
#include <utility>

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
    std::unique_ptr<RowIterator> GetValue() override {
        auto iter = iter_->GetValue();
        if (!iter) {
            return std::unique_ptr<RowIterator>();
        } else {
            return std::unique_ptr<RowIterator>(
                new IteratorProjectWrapper(std::move(iter), parameter_, fun_));
        }
    }
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
    std::unique_ptr<RowIterator> GetValue() override {
        auto iter = iter_->GetValue();
        if (!iter) {
            return std::unique_ptr<RowIterator>();
        } else {
            return std::unique_ptr<RowIterator>(
                new IteratorFilterWrapper(std::move(iter), parameter_, fun_));
        }
    }
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
    std::unique_ptr<base::ConstIterator<uint64_t, Row>> GetIterator() override {
        auto iter = partition_handler_->GetIterator();
        if (!iter) {
            return std::unique_ptr<RowIterator>();
        } else {
            return std::unique_ptr<RowIterator>(
                new IteratorProjectWrapper(std::move(iter), parameter_, fun_));
        }
    }
    base::ConstIterator<uint64_t, Row>* GetRawIterator() override;
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
    std::unique_ptr<base::ConstIterator<uint64_t, Row>> GetIterator() override {
        auto iter = partition_handler_->GetIterator();
        if (!iter) {
            return std::unique_ptr<base::ConstIterator<uint64_t, Row>>();
        } else {
            return std::unique_ptr<RowIterator>(
                new IteratorFilterWrapper(std::move(iter), parameter_, fun_));
        }
    }
    base::ConstIterator<uint64_t, Row>* GetRawIterator() override;

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

    std::unique_ptr<RowIterator> GetIterator() override {
        auto iter = table_hander_->GetIterator();
        if (!iter) {
            return std::unique_ptr<RowIterator>();
        } else {
            return std::unique_ptr<RowIterator>(
                new IteratorProjectWrapper(std::move(iter), parameter_, fun_));
        }
    }
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
    base::ConstIterator<uint64_t, Row>* GetRawIterator() override {
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

    std::unique_ptr<RowIterator> GetIterator() override {
        auto iter = table_hander_->GetIterator();
        if (!iter) {
            return std::unique_ptr<RowIterator>();
        } else {
            return std::make_unique<IteratorFilterWrapper>(std::move(iter), parameter_, fun_);
        }
    }
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
    base::ConstIterator<uint64_t, Row>* GetRawIterator() override {
        return new IteratorFilterWrapper(static_cast<std::unique_ptr<RowIterator>>(table_hander_->GetRawIterator()),
                                         parameter_, fun_);
    }
    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override;
    const OrderType GetOrderType() const override { return table_hander_->GetOrderType(); }

 private:
    std::shared_ptr<TableHandler> table_hander_;
    const Row& parameter_;
    Row value_;
    const PredicateFun* fun_;
};

class LimitTableHandler : public TableHandler {
 public:
    explicit LimitTableHandler(std::shared_ptr<TableHandler> table, int32_t limit)
        : TableHandler(), table_hander_(table), limit_(limit) {}
    virtual ~LimitTableHandler() {}

    std::unique_ptr<RowIterator> GetIterator() override {
        auto iter = table_hander_->GetIterator();
        if (!iter) {
            return std::unique_ptr<RowIterator>();
        } else {
            return std::make_unique<LimitIterator>(std::move(iter), limit_);
        }
    }

    // FIXME(ace): do not use this, not implemented
    std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) override {
        LOG(ERROR) << "window iterator for LimitTableHandler is not implemented, don't use";
        return table_hander_->GetWindowIterator(idx_name);
    }

    base::ConstIterator<uint64_t, Row>* GetRawIterator() override {
        return new LimitIterator(static_cast<std::unique_ptr<RowIterator>>(table_hander_->GetRawIterator()), limit_);
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

class LazyLastJoinIterator : public RowIterator {
 public:
    LazyLastJoinIterator(std::unique_ptr<RowIterator>&& left, std::shared_ptr<PartitionHandler> right, const Row& param,
                         std::shared_ptr<JoinGenerator> join);

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
    std::shared_ptr<PartitionHandler> right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;

    Row value_;
};

class LazyLastJoinPartitionHandler final : public PartitionHandler {
 public:
    LazyLastJoinPartitionHandler(std::shared_ptr<PartitionHandler> left, std::shared_ptr<PartitionHandler> right,
                                const Row& param, std::shared_ptr<JoinGenerator> join);
    ~LazyLastJoinPartitionHandler() override {}

    // NOTE: only support get segement by key from left source
    std::shared_ptr<TableHandler> GetSegment(const std::string& key) override;

    const std::string GetHandlerTypeName() override {
        return "LazyLastJoinPartitionHandler";
    }

    std::unique_ptr<RowIterator> GetIterator() override;

    std::unique_ptr<WindowIterator> GetWindowIterator() override;

    const IndexHint& GetIndex() override { return left_->GetIndex(); }

    // unimplemented
    const Types& GetTypes() override { return left_->GetTypes(); }

    // unimplemented
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return name_; }
    const std::string& GetDatabase() override { return db_; }

    // unimplemented
    base::ConstIterator<uint64_t, Row>* GetRawIterator() override {
        return nullptr;
    }

 private:
    std::shared_ptr<PartitionHandler> left_;
    std::shared_ptr<PartitionHandler> right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;

    std::string name_ = "";
    std::string db_ = "";
};

class LazyLastJoinTableHandler final : public TableHandler {
 public:
    LazyLastJoinTableHandler(std::shared_ptr<TableHandler> left, std::shared_ptr<PartitionHandler> right,
                                const Row& param, std::shared_ptr<JoinGenerator> join);
    ~LazyLastJoinTableHandler() override {}

    std::unique_ptr<RowIterator> GetIterator() override;

    // unimplemented
    const Types& GetTypes() override { return left_->GetTypes(); }

    const IndexHint& GetIndex() override { return left_->GetIndex(); }

    // unimplemented
    std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) override;

    // unimplemented
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return name_; }
    const std::string& GetDatabase() override { return db_; }

    base::ConstIterator<uint64_t, Row>* GetRawIterator() override {
        // unimplemented
        return nullptr;
    }

    Row At(uint64_t pos) override {
        // unimplemented
        return value_;
    }

    const uint64_t GetCount() override { return left_->GetCount(); }

    std::shared_ptr<PartitionHandler> GetPartition(const std::string& index_name) override;

    const OrderType GetOrderType() const override { return left_->GetOrderType(); }

    const std::string GetHandlerTypeName() override {
        return "LazyLastJoinTableHandler";
    }

 private:
    std::shared_ptr<TableHandler> left_;
    std::shared_ptr<PartitionHandler> right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;

    Row value_;
    std::string name_ = "";
    std::string db_ = "";
};

class LazyLastJoinWindowIterator final : public codec::WindowIterator {
 public:
    LazyLastJoinWindowIterator(std::unique_ptr<WindowIterator>&& iter, std::shared_ptr<PartitionHandler> right,
                               const Row& param, std::shared_ptr<JoinGenerator> join);

    ~LazyLastJoinWindowIterator() override {}

    std::unique_ptr<RowIterator> GetValue() override;
    RowIterator* GetRawValue() override;

    void Seek(const std::string& key) override { left_->Seek(key); }
    void SeekToFirst() override { left_->SeekToFirst(); }
    void Next() override { left_->Next(); }
    bool Valid() override { return left_ && left_->Valid(); }
    const Row GetKey() override { return left_->GetKey(); }

    std::shared_ptr<WindowIterator> left_;
    std::shared_ptr<PartitionHandler> right_;
    const Row& parameter_;
    std::shared_ptr<JoinGenerator> join_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_SRC_VM_CATALOG_WRAPPER_H_
