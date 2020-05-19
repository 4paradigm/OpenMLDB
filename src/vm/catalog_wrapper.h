/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * catalog_wrapper.h
 *
 * Author: chenjing
 * Date: 2020/5/18
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_CATALOG_WRAPPER_H_
#define SRC_VM_CATALOG_WRAPPER_H_
#include <memory>
#include <string>
#include <utility>
#include "vm/catalog.h"
namespace fesql {
namespace vm {

class WrapperFun {
 public:
    virtual Row operator()(const Row& row) const = 0;
};

class IteratorWrapper : public RowIterator {
 public:
    IteratorWrapper(std::unique_ptr<RowIterator> iter, const WrapperFun* fun)
        : RowIterator(), iter_(std::move(iter)), fun_(fun) {}
    virtual ~IteratorWrapper() {}
    bool Valid() const override { return iter_->Valid(); }
    void Next() override { iter_->Next(); }
    const uint64_t& GetKey() const override { return iter_->GetKey(); }
    const Row& GetValue() override {
        value_ = fun_->operator()(iter_->GetValue());
        return value_;
    }
    void Seek(const uint64_t& k) override { iter_->Seek(k); }
    void SeekToFirst() override { iter_->SeekToFirst(); }
    bool IsSeekable() const override { return iter_->IsSeekable(); }
    std::unique_ptr<RowIterator> iter_;
    const WrapperFun* fun_;
    Row value_;
};

class WindowIteratorWrapper : public WindowIterator {
 public:
    WindowIteratorWrapper(std::unique_ptr<WindowIterator> iter,
                          const WrapperFun* fun)
        : WindowIterator(), iter_(std::move(iter)), fun_(fun) {}
    virtual ~WindowIteratorWrapper() {}
    std::unique_ptr<RowIterator> GetValue() override {
        return std::unique_ptr<RowIterator>(
            new IteratorWrapper(iter_->GetValue(), fun_));
    }
    void Seek(const std::string& key) override { iter_->Seek(key); }
    void SeekToFirst() override { iter_->SeekToFirst(); }
    void Next() override { iter_->Next(); }
    bool Valid() override { return iter_->Valid(); }
    const Row GetKey() override { return iter_->GetKey(); }
    std::unique_ptr<WindowIterator> iter_;
    const WrapperFun* fun_;
};

class TableWrapper : public TableHandler {
 public:
    TableWrapper(std::shared_ptr<TableHandler> table_handler,
                 const WrapperFun* fun)
        : TableHandler(), table_hander_(table_handler), value_(), fun_(fun) {}
    virtual ~TableWrapper() {}

    std::unique_ptr<RowIterator> GetIterator() const {
        return std::unique_ptr<RowIterator>(
            new IteratorWrapper(table_hander_->GetIterator(), fun_));
    }
    const Types& GetTypes() override { return table_hander_->GetTypes(); }
    const IndexHint& GetIndex() override { return table_hander_->GetIndex(); }
    std::unique_ptr<WindowIterator> GetWindowIterator(
        const std::string& idx_name) override {
        return std::unique_ptr<WindowIterator>(new WindowIteratorWrapper(
            table_hander_->GetWindowIterator(idx_name), fun_));
    }
    const Schema* GetSchema() override { return table_hander_->GetSchema(); }
    const std::string& GetName() override { return table_hander_->GetName(); }
    const std::string& GetDatabase() override {
        return table_hander_->GetDatabase();
    }
    base::ConstIterator<uint64_t, Row>* GetIterator(
        int8_t* addr) const override {
        return new IteratorWrapper(static_cast<std::unique_ptr<RowIterator>>(
                                       table_hander_->GetIterator(addr)),
                                   fun_);
    }
    Row At(uint64_t pos) override {
        value_ = fun_->operator()(table_hander_->At(pos));
        return value_;
    }
    const uint64_t GetCount() override { return table_hander_->GetCount(); }
    std::shared_ptr<TableHandler> table_hander_;
    Row value_;
    const WrapperFun* fun_;
};

class PartitionWrapper : public PartitionHandler {
 public:
    PartitionWrapper(std::shared_ptr<PartitionHandler> partition_handler,
                     const WrapperFun* fun)
        : PartitionHandler(),
          partition_handler_(partition_handler),
          value_(),
          fun_(fun) {}
    virtual ~PartitionWrapper() {}
    std::unique_ptr<WindowIterator> GetWindowIterator() override {
        return std::unique_ptr<WindowIterator>(new WindowIteratorWrapper(
            partition_handler_->GetWindowIterator(), fun_));
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
    std::unique_ptr<base::ConstIterator<uint64_t, Row>> GetIterator()
        const override {
        return std::unique_ptr<RowIterator>(
            new IteratorWrapper(partition_handler_->GetIterator(), fun_));
    }
    base::ConstIterator<uint64_t, Row>* GetIterator(
        int8_t* addr) const override {
        return new IteratorWrapper(static_cast<std::unique_ptr<RowIterator>>(
                                       partition_handler_->GetIterator(addr)),
                                   fun_);
    }
    Row At(uint64_t pos) override {
        value_ = fun_->operator()(partition_handler_->At(pos));
        return value_;
    }
    const uint64_t GetCount() override {
        return partition_handler_->GetCount();
    }

    std::shared_ptr<PartitionHandler> partition_handler_;
    Row value_;
    const WrapperFun* fun_;
};

class RowWrapper : public RowHandler {
 public:
    RowWrapper(std::shared_ptr<RowHandler> row_handler, const WrapperFun* fun)
        : RowHandler(), row_handler_(row_handler), value_(), fun_(fun) {}
    virtual ~RowWrapper() {}
    const Row& GetValue() override {
        value_ = fun_->operator()(row_handler_->GetValue());
        return value_;
    }
    const Schema* GetSchema() override { return row_handler_->GetSchema(); }
    const std::string& GetName() override { return row_handler_->GetName(); }
    const std::string& GetDatabase() override {
        return row_handler_->GetDatabase();
    }
    std::shared_ptr<RowHandler> row_handler_;
    Row value_;
    const WrapperFun* fun_;
};
}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_CATALOG_WRAPPER_H_
