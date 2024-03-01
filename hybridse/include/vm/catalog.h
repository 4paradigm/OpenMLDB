/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef HYBRIDSE_INCLUDE_VM_CATALOG_H_
#define HYBRIDSE_INCLUDE_VM_CATALOG_H_
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_slice.h"
#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "codec/list_iterator_codec.h"
#include "codec/row.h"
#include "proto/fe_type.pb.h"
#include "sdk/base.h"

namespace hybridse {
namespace vm {

using hybridse::codec::ColInfo;
using hybridse::codec::ListV;
using hybridse::codec::Row;
using hybridse::codec::RowIterator;
using hybridse::codec::Schema;
using hybridse::codec::WindowIterator;

constexpr uint32_t INVALID_POS =
    UINT32_MAX;    ///< Invalid position. A column position is invalid if equals to INVALID_POS

/// Represents index information, e.g, name, first keys, second key
/// information
struct IndexSt {
    std::string name;           ///< index name
    uint32_t index;             ///< position of index
    uint32_t ts_pos;            ///< second key column position
    std::vector<ColInfo> keys;  ///< first keys set
};

/// \typedef IndexList repeated fields of IndexDef
typedef ::google::protobuf::RepeatedPtrField<::hybridse::type::IndexDef>
    IndexList;
/// \typedef Types a map with string type key and ColInfo value
typedef std::map<std::string, ColInfo> Types;
/// \typedef IndexHint a map with string type key and IndexSt value
typedef std::map<std::string, IndexSt> IndexHint;

class PartitionHandler;
class TableHandler;
class RowHandler;
class Tablet;

enum HandlerType { kRowHandler, kTableHandler, kPartitionHandler };
enum OrderType { kDescOrder, kAscOrder, kNoneOrder };

/// \brief The basic dataset operation abstraction.
///
/// It contains the basic operations available on all row-based dataset
/// handlers, such as TableHandler, Partitionhandler.
class DataHandler : public ListV<Row> {
 public:
    DataHandler() {}
    virtual ~DataHandler() {}
    /// Return table schema.
    virtual const Schema* GetSchema() = 0;

    /// Return table name.
    virtual const std::string& GetName() = 0;

    /// Return the name of database.
    virtual const std::string& GetDatabase() = 0;

    /// Return the type of DataHandler.
    virtual const HandlerType GetHandlerType() = 0;
    /// Return the name of handler type
    virtual const std::string GetHandlerTypeName() = 0;
    /// Return dataset status. Default is hybridse::common::kOk
    virtual base::Status GetStatus() { return base::Status::OK(); }
};

/// \brief A sequence of DataHandler
class DataHandlerList {
 public:
    DataHandlerList() {}
    virtual ~DataHandlerList() {}
    /// Return the number of elements
    virtual size_t GetSize() = 0;
    /// Return the idx-th element
    virtual std::shared_ptr<DataHandler> Get(size_t idx) = 0;
};

/// \brief A implementation of DataHandlerList.
class DataHandlerVector : public DataHandlerList {
 public:
    DataHandlerVector() : data_handlers_() {}
    ~DataHandlerVector() {}
    void Add(std::shared_ptr<DataHandler> data_handler) {
        data_handlers_.push_back(data_handler);
    }
    /// Return the number of elements
    size_t GetSize() { return data_handlers_.size(); }
    /// Return the idx-th element. Return `null` when position is out of range
    std::shared_ptr<DataHandler> Get(size_t idx) {
        return idx < data_handlers_.size() ? data_handlers_[idx]
                                           : std::shared_ptr<DataHandler>();
    }

 private:
    std::vector<std::shared_ptr<DataHandler>> data_handlers_;
};
/// \brief A implementation of DataHandlerList.
///
/// Actually, we just keep one data_handler_ in container where elements are
/// repeated logically.
class DataHandlerRepeater : public DataHandlerList {
 public:
    /// Create DataHandlerRepeater with a DataHandler and elements number
    DataHandlerRepeater(std::shared_ptr<DataHandler> data_handler, size_t size)
        : size_(size), data_handler_(data_handler) {}
    ~DataHandlerRepeater() {}

    /// Return the number of elements
    size_t GetSize() { return size_; }
    /// Return the idx-th element. Return `null` when position is out of range
    std::shared_ptr<DataHandler> Get(size_t idx) {
        return idx < size_ ? data_handler_ : std::shared_ptr<DataHandler>();
    }

 private:
    size_t size_;
    std::shared_ptr<DataHandler> data_handler_;
};

/// \brief A row operation abstraction.
class RowHandler : public DataHandler {
 public:
    RowHandler() {}

    virtual ~RowHandler() {}
    /// Return `null` since GetIterator isn't supported for a row
    std::unique_ptr<RowIterator> GetIterator() override {
        return std::unique_ptr<RowIterator>();
    }
    /// Return `null` since GetRawIterator isn't supported for a row,
    RowIterator* GetRawIterator() override { return nullptr; }

    /// Return 0 since Getcount isn't supported for a row
    const uint64_t GetCount() override { return 0; }

    /// Return 0 since Getcount isn't supported for a row
    Row At(uint64_t pos) override { return Row(); }

    /// Return the HandlerType of the row handler.
    /// Return HandlerType::kRowHandler by default
    const HandlerType GetHandlerType() override { return kRowHandler; }

    /// Return value of row
    virtual const Row& GetValue() = 0;

    /// Get the name of handler type.
    /// \return name of handler type, default is `"RowHandler"`
    const std::string GetHandlerTypeName() override { return "RowHandler"; }
};

/// \brief A row's error handler, representing a error row
class ErrorRowHandler : public RowHandler {
 public:
    /// Creating ErrorRowHandler with status code and error msg
    ErrorRowHandler(common::StatusCode status_code, const std::string& msg_str)
        : status_(status_code, msg_str),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          row_() {}
    ~ErrorRowHandler() {}

    /// Return empty Row as value
    const Row& GetValue() final { return row_; }

    /// Return handler type name, and return "ErrorRowHandler" by default.
    const std::string GetHandlerTypeName() override {
        return "ErrorRowHandler";
    }
    const Schema* GetSchema() override { return nullptr; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }
    virtual base::Status GetStatus() { return status_; }

 private:
    base::Status status_;
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    Row row_;
};

/// \brief A table dataset operation abstraction.
class TableHandler : public DataHandler {
 public:
    TableHandler() : DataHandler() {}

    virtual ~TableHandler() {}

    /// Return table column Types information.
    /// TODO: rm it, never used
    virtual const Types& GetTypes() = 0;

    /// Return the index information
    virtual const IndexHint& GetIndex() = 0;

    /// Return WindowIterator
    /// so that user can use it to iterate datasets segment by segment.
    virtual std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) { return nullptr; }

    /// Return the HandlerType of the dataset.
    /// Return HandlerType::kTableHandler by default
    const HandlerType GetHandlerType() override { return kTableHandler; }

    /// Return partition handler of specify partition binding to given index.
    /// Return `null` by default.
    virtual std::shared_ptr<PartitionHandler> GetPartition(
        const std::string& index_name) {
        return std::shared_ptr<PartitionHandler>();
    }

    /// Return the name of handler and return "TableHandler" by default.
    const std::string GetHandlerTypeName() override { return "TableHandler"; }

    /// Return the order type of the dataset,
    /// and return OrderType::kNoneOrder by default.
    virtual const OrderType GetOrderType() const { return kNoneOrder; }

    /// Return Tablet binding to specify index and key.
    /// Return `null` by default.
    virtual std::shared_ptr<Tablet> GetTablet(const std::string& index_name,
                                              const std::string& pk) {
        return std::shared_ptr<Tablet>();
    }

    /// Return Tablet binding to specify index and keys.
    /// Return `null` by default.
    virtual std::shared_ptr<Tablet> GetTablet(const std::string& index_name, const std::vector<std::string>& pks) {
        return std::shared_ptr<Tablet>();
    }

    static std::shared_ptr<TableHandler> Cast(std::shared_ptr<DataHandler> in);
};

/// \brief A table dataset's error handler, representing a error table
class ErrorTableHandler : public TableHandler {
 public:
    /// Create ErrorTableTable with initializing status_ with
    /// common::kCallMethodError
    ErrorTableHandler()
        : status_(common::kCallRpcMethodError, "error"),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          types_(),
          index_hint_() {}
    /// Create ErrorTableHandler with specific status code and message
    ErrorTableHandler(common::StatusCode status_code,
                      const std::string& msg_str)
        : status_(status_code, msg_str),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          types_(),
          index_hint_() {}
    ~ErrorTableHandler() {}

    /// Return empty column Types.
    const Types& GetTypes() override { return types_; }
    /// Return empty table Schema.
    const Schema* GetSchema() override { return schema_; }
    /// Return empty table name
    const std::string& GetName() override { return table_name_; }
    /// Return empty indexn information
    const IndexHint& GetIndex() override { return index_hint_; }
    /// Return name of database
    const std::string& GetDatabase() override { return db_; }

    /// Return null iterator
    RowIterator* GetRawIterator() override { return nullptr; }

    /// Return empty row
    Row At(uint64_t pos) override { return Row(); }

    /// Return 0
    const uint64_t GetCount() override { return 0; }

    /// Return handler type name, and return "ErrorTableHandler" by default.
    const std::string GetHandlerTypeName() override {
        return "ErrorTableHandler";
    }

    /// Return status
    base::Status GetStatus() override { return status_; }

 protected:
    base::Status status_;
    const std::string table_name_;
    const std::string db_;
    const Schema* schema_;
    Types types_;
    IndexHint index_hint_;
    OrderType order_type_;
};

/// \brief The abstraction of partition dataset operation.
///
/// A partition dataset is always organized by segments
///              +-- key1 --> segment1
///  partition --+-- key2 --> segment2
///              +-- key3 --> segment3
class PartitionHandler : public TableHandler {
 public:
    PartitionHandler() : TableHandler() {}
    ~PartitionHandler() {}

    // Return the iterator of row iterator
    // Return null by default
    RowIterator* GetRawIterator() override { return nullptr; }

    using TableHandler::GetWindowIterator;

    /// Return WindowIterator to iterate datasets
    /// segment-by-segment.
    virtual std::unique_ptr<WindowIterator> GetWindowIterator() = 0;

    /// Return HandlerType::kPartitionHandler by default
    const HandlerType GetHandlerType() override { return kPartitionHandler; }

    /// Return empty row, cause partition dataset does not support At operation.
    // virtual Row At(uint64_t pos) { return Row(); }

    /// Return Return table handler of specific segment binding to given key.
    /// Return `null` by default.
    virtual std::shared_ptr<TableHandler> GetSegment(const std::string& key) = 0;

    /// Return a sequence of table handles of specify segments binding to given
    /// keys set.
    virtual std::vector<std::shared_ptr<TableHandler>> GetSegments(const std::vector<std::string>& keys) {
        std::vector<std::shared_ptr<TableHandler>> segments;
        for (auto key : keys) {
            segments.push_back(GetSegment(key));
        }
        return segments;
    }
    /// Return the name of handler, and return `"PartitionHandler"` by default.
    const std::string GetHandlerTypeName() override {
        return "PartitionHandler";
    }

    static std::shared_ptr<PartitionHandler> Cast(std::shared_ptr<DataHandler> in);
};

/// \brief A wrapper of table handler which is used as a asynchronous row
/// handler
///
/// AysncRowHandler is statefull. It is running when created.
/// GetValue is invoked, status will be changed if it is running at that moment.
class AysncRowHandler : public RowHandler {
 public:
    /// Create with given table_handler and row position index.
    /// status_ is set with common::kRunning
    AysncRowHandler(size_t idx,
                    std::shared_ptr<TableHandler> aysnc_table_handler)
        : RowHandler(),
          status_(base::Status::Running()),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          idx_(idx),
          aysnc_table_handler_(aysnc_table_handler),
          value_() {
        if (!aysnc_table_handler_) {
            status_ = base::Status(hybridse::common::kNullPointer,
                                   "async table handler is null");
        }
    }
    virtual ~AysncRowHandler() {}

    /// Return the row value.
    /// Sync row value by invoking aysnc_table_handler_->At(idx_)
    /// if status isn't common::kRunning
    const Row& GetValue() override {
        if (!status_.isRunning()) {
            return value_;
        }
        value_ = aysnc_table_handler_->At(idx_);
        status_ = aysnc_table_handler_->GetStatus();
        return value_;
    }
    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }

 private:
    base::Status status_;
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    size_t idx_;
    std::shared_ptr<TableHandler> aysnc_table_handler_;
    Row value_;
};

/// \brief A component responsible to Query subtask
class Tablet {
 public:
    Tablet() {}
    virtual ~Tablet() {}
    /// Return the name of tablet.
    virtual const std::string& GetName() const = 0;
    /// Return RowHandler by calling request-mode
    /// query on subtask which is specified by task_id and sql string
    virtual std::shared_ptr<RowHandler> SubQuery(
        uint32_t task_id, const std::string& db, const std::string& sql,
        const hybridse::codec::Row& row, const bool is_procedure,
        const bool is_debug) = 0;
    /// Return TableHandler by calling
    /// batch-request-mode query on subtask which is specified by task_id and
    /// sql
    virtual std::shared_ptr<TableHandler> SubQuery(
        uint32_t task_id, const std::string& db, const std::string& sql,
        const std::set<size_t>& common_column_indices,
        const std::vector<Row>& in_rows, const bool request_is_common,
        const bool is_procedure, const bool is_debug) = 0;
};
struct AggrTableInfo {
    std::string aggr_table;
    std::string aggr_db;
    std::string base_db;
    std::string base_table;
    std::string aggr_func;
    std::string aggr_col;
    std::string partition_cols;
    std::string order_by_col;
    std::string bucket_size;
    std::string filter_col;

    bool operator==(const AggrTableInfo& rhs) const {
        return aggr_table == rhs.aggr_table &&
            aggr_db == rhs.aggr_db &&
            base_db == rhs.base_db &&
            base_table == rhs.base_table &&
            aggr_func == rhs.aggr_func &&
            aggr_col == rhs.aggr_col &&
            partition_cols == rhs.partition_cols &&
            order_by_col == rhs.order_by_col &&
            bucket_size == rhs.bucket_size &&
            filter_col == rhs.filter_col;
    }
};

/// \brief A Catalog handler which defines a set of operation for, e.g,
/// database, table and index management.
///
/// Users should implement the subclasses for their own purpose
class Catalog {
 public:
    Catalog() {}

    virtual ~Catalog() {}

    /// Return whether index is supported or not.
    virtual bool IndexSupport() = 0;

    /// Return database information.
    virtual std::shared_ptr<type::Database> GetDatabase(
        const std::string& db) = 0;

    /// Return a table handler with given table
    /// name
    virtual std::shared_ptr<TableHandler> GetTable(
        const std::string& db, const std::string& table_name) = 0;

    /// Return ProcedureInfo instance with given database name `db` and
    /// procedure name `sp_name`
    virtual std::shared_ptr<hybridse::sdk::ProcedureInfo> GetProcedureInfo(
        const std::string& db, const std::string& sp_name) {
        return nullptr;
    }

    virtual std::vector<AggrTableInfo> GetAggrTables(const std::string& base_db, const std::string& base_table,
                                                     const std::string& aggr_func, const std::string& aggr_col,
                                                     const std::string& partition_cols, const std::string& order_col,
                                                     const std::string& filter_col) {
        return std::vector<AggrTableInfo>();
    }
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_VM_CATALOG_H_
