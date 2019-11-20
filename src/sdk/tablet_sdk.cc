/*
 * tablet_sdk.cc
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#include "sdk/tablet_sdk.h"
#include <analyser/analyser.h>
#include <base/strings.h>
#include <node/node_enum.h>
#include <parser/parser.h>
#include <plan/planner.h>

#include "brpc/channel.h"
#include "glog/logging.h"
#include "proto/tablet.pb.h"
#include "storage/codec.h"

namespace fesql {
namespace sdk {

class TabletSdkImpl;
class ResultSetImpl;
class ResultSetIteratorImpl;

inline const std::string DataTypeName(const DataType& type) {
    switch (type) {
        case kTypeBool:
            return "bool";
        case kTypeInt16:
            return "int16";
        case kTypeInt32:
            return "int32";
        case kTypeInt64:
            return "int64";
        case kTypeFloat:
            return "float";
        case kTypeDouble:
            return "double";
        case kTypeString:
            return "string";
        case kTypeTimestamp:
            return "timestamp";
        default:
            return "unknownType";
    }
}

class ResultSetIteratorImpl : public ResultSetIterator {
 public:
    ResultSetIteratorImpl(tablet::QueryResponse* response);

    ~ResultSetIteratorImpl();

    bool HasNext();

    void Next();

    bool GetInt16(uint32_t idx, int16_t* val);
    bool GetInt32(uint32_t idx, int32_t* val);
    bool GetInt64(uint32_t idx, int64_t* val);
    bool GetFloat(uint32_t idx, float* val);
    bool GetDouble(uint32_t idx, double* val);

 private:
    std::vector<uint32_t> offsets_;
    uint32_t idx_;
    tablet::QueryResponse* response_;
    std::unique_ptr<storage::RowView> row_view_;
};

ResultSetIteratorImpl::ResultSetIteratorImpl(tablet::QueryResponse* response)
    : offsets_(), idx_(0), response_(response), row_view_() {
    uint32_t offset = 2;
    for (int32_t i = 0; i < response_->schema_size(); i++) {
        offsets_.push_back(offset);
        const ::fesql::type::ColumnDef& column = response_->schema(i);
        switch (column.type()) {
            case ::fesql::type::kInt16: {
                offset += 2;
                break;
            }
            case ::fesql::type::kInt32:
            case ::fesql::type::kFloat: {
                offset += 4;
                break;
            }
            case ::fesql::type::kInt64:
            case ::fesql::type::kDouble: {
                offset += 8;
                break;
            }
            default: {
                LOG(WARNING) << ::fesql::type::Type_Name(column.type())
                             << " is not supported";
                break;
            }
        }
    }
}

ResultSetIteratorImpl::~ResultSetIteratorImpl() {}

bool ResultSetIteratorImpl::HasNext() {
    if ((int32_t)idx_ < response_->result_set_size()) return true;
    return false;
}

void ResultSetIteratorImpl::Next() {
    const int8_t* row =
        reinterpret_cast<const int8_t*>(response_->result_set(idx_).c_str());
    uint32_t size = response_->result_set(idx_).size();
    row_view_ = std::move(std::unique_ptr<storage::RowView>(
        new storage::RowView(&(response_->schema()), row, &offsets_, size)));
    idx_ += 1;
}

bool ResultSetIteratorImpl::GetInt16(uint32_t idx, int16_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt16(idx, val);
}

bool ResultSetIteratorImpl::GetInt32(uint32_t idx, int32_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt32(idx, val);
}

bool ResultSetIteratorImpl::GetInt64(uint32_t idx, int64_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt64(idx, val);
}

bool ResultSetIteratorImpl::GetFloat(uint32_t idx, float* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetFloat(idx, val);
}

bool ResultSetIteratorImpl::GetDouble(uint32_t idx, double* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetDouble(idx, val);
}

class ResultSetImpl : public ResultSet {
 public:
    ResultSetImpl() : response_() {}
    ~ResultSetImpl() {}

    const uint32_t GetColumnCnt() const { return response_.schema_size(); }
    const std::string& GetColumnName(uint32_t i) const {
        // TODO check i out of index
        return response_.schema(i).name();
    }

    const DataType GetColumnType(uint32_t i) const {
        switch (response_.schema(i).type()) {
            case fesql::type::kBool:
                return kTypeBool;
            case fesql::type::kInt16:
                return kTypeInt16;
            case fesql::type::kInt32:
                return kTypeInt32;
            case fesql::type::kInt64:
                return kTypeInt64;
            case fesql::type::kFloat:
                return kTypeFloat;
            case fesql::type::kDouble:
                return kTypeDouble;
            case fesql::type::kString:
                return kTypeString;
            case fesql::type::kDate:
                return kTypeDate;
            case fesql::type::kTimestamp:
                return kTypeTimestamp;
        }
    }
    const uint32_t GetRowCnt() const { return response_.result_set_size(); }

    std::unique_ptr<ResultSetIterator> Iterator() {
        return std::move(std::unique_ptr<ResultSetIteratorImpl>(
            new ResultSetIteratorImpl(&response_)));
    }

 private:
    friend TabletSdkImpl;
    tablet::QueryResponse response_;
};

class TabletSdkImpl : public TabletSdk {
 public:
    TabletSdkImpl() {}
    TabletSdkImpl(const std::string& endpoint)
        : endpoint_(endpoint), channel_(NULL) {}
    ~TabletSdkImpl() { delete channel_; }

    bool Init();

    std::unique_ptr<ResultSet> SyncQuery(const Query& query);

    void SyncInsert(const Insert& insert, base::Status& status);

    void SyncInsert(const std::string& db, const std::string& sql,
                    base::Status& status);
    bool GetSchema(const std::string& db, const std::string& table,
                   type::TableDef& schema, base::Status& status);

    void GetSqlPlan(const std::string& db, const std::string& sql,
                    node::NodeManager& node_manager,
                    node::PlanNodeList& plan_trees, base::Status& status);

 private:
    std::string endpoint_;
    brpc::Channel* channel_;
};

bool TabletSdkImpl::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        return false;
    }
    return true;
}

void TabletSdkImpl::SyncInsert(const Insert& insert, base::Status& status) {
    size_t row_size = 0;
    for (auto item : insert.values) {
        row_size += item.GetSize();
    }

    type::TableDef schema;

    if (false == GetSchema(insert.db, insert.table, schema, status)) {
        if (0 == status.code) {
            status.code = -1;
            status.msg = "Table Not Exist";
        }
        return;
    }
    std::string row;
    DLOG(INFO) << "row size: " << row_size;
    row.resize(row_size+2);
    char* str_buf = reinterpret_cast<char*>(&(row[0]));
    storage::RowBuilder rbuilder(&(schema.columns()), (int8_t*)str_buf,
                                 row_size+2);

    // TODO(chenjing): handle insert into table(col1, col2, col3) values(1, 2.1,
    // 3);
    for (unsigned i = 0; i < schema.columns_size(); i++) {
        const Value& value = insert.values[i];
        switch (schema.columns(i).type()) {
            case type::kInt32:
                rbuilder.AppendInt32(value.GetInt32());
                break;
            case type::kInt16:
                rbuilder.AppendInt16(value.GetInt16());
                break;
            case type::kInt64:
                rbuilder.AppendInt64(value.GetInt64());
                break;
            case type::kFloat:
                rbuilder.AppendFloat(value.GetFloat());
                break;
            case type::kDouble:
                rbuilder.AppendDouble(value.GetDouble());
                break;
            default: {
                status.msg =
                    "UnSupport data type " + DataTypeName(value.GetDataType());
                status.code = error::kExecuteErrorUnSupport;
                return;
            }
        }
    }
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::InsertRequest req;
    req.set_db(insert.db);
    req.set_table(insert.table);
    req.set_row(row);
    req.set_ts(insert.ts);
    req.set_key(insert.key);
    ::fesql::tablet::InsertResponse response;
    brpc::Controller cntl;
    stub.Insert(&cntl, &req, &response, NULL);
    if (cntl.Failed()) {
        status.code = -1;
        status.msg = "Rpc Control error";
    }
}

std::unique_ptr<ResultSet> TabletSdkImpl::SyncQuery(const Query& query) {
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::QueryRequest request;
    request.set_sql(query.sql);
    request.set_db(query.db);
    brpc::Controller cntl;
    ResultSetImpl* rs = new ResultSetImpl();
    stub.Query(&cntl, &request, &(rs->response_), NULL);
    if (cntl.Failed() || rs->response_.status().code() != common::kOk) {
        delete rs;
        return std::unique_ptr<ResultSet>();
    }
    return std::move(std::unique_ptr<ResultSet>(rs));
}

bool TabletSdkImpl::GetSchema(const std::string& db, const std::string& table,
                              type::TableDef& schema, base::Status& status) {
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::GetTablesSchemaRequest request;
    ::fesql::tablet::GetTableSchemaReponse response;
    request.set_db(db);
    request.set_name(table);
    brpc::Controller cntl;
    stub.GetTableSchema(&cntl, &request, &response, NULL);
    if (response.status().code() != common::kOk) {
        status.code = response.status().code();
        status.msg = response.status().msg();
        return false;
    }
    if (cntl.Failed()) {
        status.code = (common::kConnError);
        status.msg = ("rpc controll error");
        return false;
    }
    schema = response.schema();
    return true;
}

void TabletSdkImpl::GetSqlPlan(const std::string& db, const std::string& sql,
                               node::NodeManager& node_manager,
                               node::PlanNodeList& plan_trees,
                               base::Status& status) {
    parser::FeSQLParser parser;
    analyser::FeSQLAnalyser analyser(&node_manager);
    plan::SimplePlanner planner(&node_manager);

    // TODO(chenjing): init with db
    node::NodePointVector parser_trees;
    parser.parse(sql, parser_trees, &node_manager, status);
    if (0 != status.code) {
        LOG(WARNING) << status.msg;
        return;
    }
    node::NodePointVector query_trees;
    analyser.Analyse(parser_trees, query_trees, status);
    if (0 != status.code) {
        LOG(WARNING) << status.msg;
        return;
    }
    planner.CreatePlanTree(query_trees, plan_trees, status);

    if (0 != status.code) {
        LOG(WARNING) << status.msg;
        return;
    }
}
void TabletSdkImpl::SyncInsert(const std::string& db, const std::string& sql,
                               base::Status& status) {
    node::PlanNodeList plan_trees;
    node::NodeManager node_manager;
    GetSqlPlan(db, sql, node_manager, plan_trees, status);
    if (0 != status.code) {
        return;
    }

    if (plan_trees.empty() || nullptr == plan_trees[0]) {
        status.msg = "fail to execute plan : plan tree is empty or null";
        status.code = error::kExecuteErrorNullNode;
        LOG(WARNING) << status.msg;
        return;
    }
    node::PlanNode* plan = plan_trees[0];
    switch (plan->GetType()) {
        case node::kPlanTypeInsert: {
            node::InsertPlanNode* insert_plan =
                dynamic_cast<node::InsertPlanNode*>(plan);
            const node::InsertStmt* insert_stmt = insert_plan->GetInsertNode();
            if (nullptr == insert_stmt) {
                status.code = error::kExecuteErrorNullNode;
                status.msg = "fail to execute insert statement with null node";
                return;
            }

            Insert insert;
            insert.db = db;
            insert.table = insert_stmt->table_name_;
            insert.columns = insert_stmt->columns_;

            size_t row_size = 0;
            for (auto item : insert.values) {
                row_size += item.GetSize();
            }

            type::TableDef schema;

            if (false == GetSchema(insert.db, insert.table, schema, status)) {
                if (0 == status.code) {
                    status.code = -1;
                    status.msg = "Table Not Exist";
                }
                return;
            }

            for (auto value : insert_stmt->values_) {
                switch (value->GetExprType()) {
                    case node::kExprPrimary: {
                        node::ConstNode* primary =
                            dynamic_cast<node::ConstNode*>(value);
                        switch (primary->GetDataType()) {
                            case node::kTypeInt16: {
                                insert.values.push_back(
                                    sdk::Value(primary->GetSmallInt()));
                                break;
                            }
                            case node::kTypeInt32: {
                                insert.values.push_back(
                                    sdk::Value(primary->GetInt()));
                                break;
                            }
                            case node::kTypeInt64: {
                                insert.values.push_back(
                                    sdk::Value(primary->GetLong()));
                                break;
                            }
                            case node::kTypeFloat: {
                                insert.values.push_back(
                                    sdk::Value(primary->GetFloat()));
                                break;
                            }
                            case node::kTypeDouble: {
                                insert.values.push_back(
                                    sdk::Value(primary->GetDouble()));
                                break;
                            }
                            default: {
                                status.code = error::kExecuteErrorUnSupport;
                                status.msg =
                                    "can not handle data type " +
                                    node::DataTypeName(primary->GetDataType());
                                return;
                            }
                        }
                    }
                }
            }
            SyncInsert(insert, status);
            return;
        }
        default: {
            status.code = error::kExecuteErrorUnSupport;
            status.msg = "can not execute plan type " +
                         node::NameOfPlanNodeType(plan->GetType());
            return;
        }
    }
}

std::unique_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint) {
    TabletSdkImpl* sdk = new TabletSdkImpl(endpoint);
    bool ok = sdk->Init();
    if (!ok) {
        delete sdk;
        return std::unique_ptr<TabletSdk>();
    }
    return std::move(std::unique_ptr<TabletSdk>(sdk));
}

}  // namespace sdk
}  // namespace fesql
