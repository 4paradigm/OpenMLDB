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
#include <map>
#include <memory>
#include <string>
#include <utility>
#include "analyser/analyser.h"
#include "base/strings.h"
#include "node/node_enum.h"
#include "parser/parser.h"
#include "plan/planner.h"

#include "brpc/channel.h"
#include "glog/logging.h"
#include "proto/tablet.pb.h"
#include "storage/codec.h"

namespace fesql {
namespace sdk {

class TabletSdkImpl;
class ResultSetImpl;
class ResultSetIteratorImpl;

class ResultSetIteratorImpl : public ResultSetIterator {
 public:
    explicit ResultSetIteratorImpl(tablet::QueryResponse* response);

    ~ResultSetIteratorImpl();

    bool HasNext();

    void Next();

    bool GetInt16(uint32_t idx, int16_t* val);
    bool GetInt32(uint32_t idx, int32_t* val);
    bool GetInt64(uint32_t idx, int64_t* val);
    bool GetFloat(uint32_t idx, float* val);
    bool GetDouble(uint32_t idx, double* val);
    bool GetString(uint32_t idx, char** val, uint32_t* size);

 private:
    uint32_t idx_;
    tablet::QueryResponse* response_;
    std::unique_ptr<storage::RowView> row_view_;
};

ResultSetIteratorImpl::ResultSetIteratorImpl(tablet::QueryResponse* response)
    : idx_(0), response_(response), row_view_() {}

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
        new storage::RowView(response_->schema(), row, size)));
    idx_ += 1;
}

bool ResultSetIteratorImpl::GetInt16(uint32_t idx, int16_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt16(idx, val) == 0;
}

bool ResultSetIteratorImpl::GetInt32(uint32_t idx, int32_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt32(idx, val) == 0;
}

bool ResultSetIteratorImpl::GetInt64(uint32_t idx, int64_t* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetInt64(idx, val) == 0;
}

bool ResultSetIteratorImpl::GetFloat(uint32_t idx, float* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetFloat(idx, val) == 0;
}

bool ResultSetIteratorImpl::GetDouble(uint32_t idx, double* val) {
    if (!row_view_) {
        return false;
    }
    return row_view_->GetDouble(idx, val) == 0;
}

bool ResultSetIteratorImpl::GetString(uint32_t idx, char** val,
                                      uint32_t* size) {
    if (!row_view_ || val == NULL || size == NULL) {
        return false;
    }
    return row_view_->GetString(idx, val, size) == 0;
}

class ResultSetImpl : public ResultSet {
 public:
    ResultSetImpl() : response_() {}
    ~ResultSetImpl() {}

    const uint32_t GetColumnCnt() const { return response_.schema_size(); }
    const std::string& GetColumnName(uint32_t i) const {
        // TODO(wangtaize) check i out of index
        return response_.schema(i).name();
    }

    const DataType GetColumnType(uint32_t i) const {
        return DataTypeFromProtoType(response_.schema(i).type());
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
    explicit TabletSdkImpl(const std::string& endpoint)
        : endpoint_(endpoint), channel_(NULL) {}
    ~TabletSdkImpl() { delete channel_; }

    bool Init();

    std::unique_ptr<ResultSet> SyncQuery(
        const Query& query,
        sdk::Status& status);  // NOLINT (runtime/references)

    void SyncInsert(const Insert& insert,
                    sdk::Status& status);  // NOLINT (runtime/references)

    void SyncInsert(const std::string& db, const std::string& sql,
                    sdk::Status& status);  // NOLINT (runtime/references)

    bool GetSchema(const std::string& db, const std::string& table,
                   type::TableDef& schema,  // NOLINT (runtime/references)
                   sdk::Status& status);    // NOLINT (runtime/references)

    void GetSqlPlan(
        const std::string& db, const std::string& sql,
        node::NodeManager& node_manager,  // NOLINT (runtime/references)
        node::PlanNodeList& plan_trees,   // NOLINT (runtime/references)
        sdk::Status& status);             // NOLINT (runtime/references)

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

void TabletSdkImpl::SyncInsert(const Insert& insert, sdk::Status& status) {
    type::TableDef schema;

    // TODO(wangtaize) reduce visit tablet count
    if (false == GetSchema(insert.db, insert.table, schema, status)) {
        if (0 == status.code) {
            status.code = -1;
            status.msg = "Table Not Exist";
        }
        return;
    }

    uint32_t string_length = 0;
    for (int i = 0; i < schema.columns_size(); i++) {
        const Value& value = insert.values[i];
        if (schema.columns(i).type() == ::fesql::type::kVarchar) {
            string_length += value.GetSize();
        }
    }

    storage::RowBuilder rbuilder(schema.columns());
    uint32_t row_size = rbuilder.CalTotalLength(string_length);
    std::string row;
    DLOG(INFO) << "row size: " << row_size << " str total leng"
               << string_length;
    row.resize(row_size);
    char* str_buf = reinterpret_cast<char*>(&(row[0]));
    rbuilder.SetBuffer(reinterpret_cast<int8_t*>(str_buf), row_size);

    // TODO(chenjing): handle insert into table(col1, col2, col3) values(1, 2.1,
    // 3);
    for (int i = 0; i < schema.columns_size(); i++) {
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
            case type::kVarchar:
                rbuilder.AppendString(value.GetStr(), value.GetSize());
                break;
            default: {
                status.msg =
                    "UnSupport data type " + DataTypeName(value.GetDataType());
                status.code = common::kUnSupport;
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

std::unique_ptr<ResultSet> TabletSdkImpl::SyncQuery(
    const Query& query, sdk::Status& status) {  // NOLINT (runtime/references)
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::QueryRequest request;
    request.set_sql(query.sql);
    request.set_db(query.db);
    brpc::Controller cntl;
    ResultSetImpl* rs = new ResultSetImpl();
    stub.Query(&cntl, &request, &(rs->response_), NULL);
    if (cntl.Failed()) {
        status.code = common::kConnError;
        status.msg = "Rpc control error";
        delete rs;
        return std::unique_ptr<ResultSet>();
    }
    if (rs->response_.status().code() != common::kOk) {
        status.code = rs->response_.status().code();
        status.msg = rs->response_.status().msg();
        delete rs;
        return std::unique_ptr<ResultSet>();
    }
    return std::move(std::unique_ptr<ResultSet>(rs));
}

bool TabletSdkImpl::GetSchema(const std::string& db, const std::string& table,
                              type::TableDef& schema, sdk::Status& status) {
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
                               sdk::Status& status) {
    parser::FeSQLParser parser;
    analyser::FeSQLAnalyser analyser(&node_manager);
    plan::SimplePlanner planner(&node_manager);
    base::Status sql_status;

    // TODO(chenjing): init with db
    node::NodePointVector parser_trees;
    parser.parse(sql, parser_trees, &node_manager, sql_status);
    if (0 != sql_status.code) {
        status.code = sql_status.code;
        status.msg = sql_status.msg;
        LOG(WARNING) << status.msg;
        return;
    }
    node::NodePointVector query_trees;
    analyser.Analyse(parser_trees, query_trees, sql_status);
    if (0 != sql_status.code) {
        status.code = sql_status.code;
        status.msg = sql_status.msg;
        LOG(WARNING) << status.msg;
        return;
    }
    planner.CreatePlanTree(query_trees, plan_trees, sql_status);

    if (0 != sql_status.code) {
        status.code = sql_status.code;
        status.msg = sql_status.msg;
        LOG(WARNING) << status.msg;
        return;
    }
}
void TabletSdkImpl::SyncInsert(const std::string& db, const std::string& sql,
                               sdk::Status& status) {
    node::PlanNodeList plan_trees;
    node::NodeManager node_manager;
    GetSqlPlan(db, sql, node_manager, plan_trees, status);
    if (0 != status.code) {
        return;
    }

    if (plan_trees.empty() || nullptr == plan_trees[0]) {
        status.msg = "fail to execute plan : plan tree is empty or null";
        status.code = common::kPlanError;
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
                status.code = common::kNullPointer;
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
                            case node::kTypeString: {
                                // TODO(wangtaize) use slice
                                insert.values.push_back(
                                    sdk::Value(primary->GetStr()));
                                break;
                            }
                            default: {
                                status.code = common::kTypeError;
                                status.msg =
                                    "can not handle data type " +
                                    node::DataTypeName(primary->GetDataType());
                                return;
                            }
                        }
                        break;
                    }
                    default: {
                        status.code = common::kTypeError;
                        status.msg = "can not insert value with type " +
                                     node::ExprTypeName(value->GetExprType());
                        return;
                    }
                }
            }
            SyncInsert(insert, status);
            return;
        }
        default: {
            status.code = common::kUnSupport;
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
