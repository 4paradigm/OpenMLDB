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
#include "codec/row_codec.h"
#include "sdk/result_set_impl.h"
#include "glog/logging.h"

namespace fesql {
namespace sdk {

class TabletSdkImpl : public TabletSdk {
 public:
    TabletSdkImpl() {}

    explicit TabletSdkImpl(const std::string& endpoint)
        : endpoint_(endpoint), channel_(NULL) {}

    ~TabletSdkImpl() { delete channel_; }

    bool Init();

    std::unique_ptr<ResultSet> Query(const std::string& db,
                                     const std::string& sql,
                                     sdk::Status* status);

    void Insert(const std::string& db, const std::string& sql,
                sdk::Status* status);

 private:

    void BuildInsertRequest(const node::InsertPlanNode* iplan, const std::string& db,
            tablet::InsertRequest* request, Status* status);

    void Insert(const tablet::InsertRequest& request,
                sdk::Status* status);

    bool GetSchema(const std::string& db, 
                   const std::string& table,
                   type::TableDef* schema,
                   sdk::Status* status);

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

void TabletSdkImpl::Insert(const tablet::InsertRequest& request, sdk::Status* status) {
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::InsertResponse response;
    brpc::Controller cntl;
    stub.Insert(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status->code = -1;
        status->msg = "Rpc Control error";
    }
    status->code = 0;
}

std::unique_ptr<ResultSet> TabletSdkImpl::Query(const std::string& db, 
        const std::string& sql, sdk::Status* status) { 
    if (status == NULL) {
        return std::unique_ptr<ResultSet>();
    }
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::QueryRequest request;
    std::unique_ptr<tablet::QueryResponse> response(new tablet::QueryResponse());
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(true);
    brpc::Controller cntl;
    stub.Query(&cntl, &request, response.get(), NULL);
    if (cntl.Failed()) {
        status->code = common::kConnError;
        status->msg = "Rpc control error";
        return std::unique_ptr<ResultSet>();
    }
    if (response->status().code() != common::kOk) {
        status->code = response->status().code();
        status->msg = response->status().msg();
        return std::unique_ptr<ResultSet>();
    }
    status->code = 0;
    std::unique_ptr<ResultSetImpl> impl(new ResultSetImpl(std::move(response)));
    impl->Init();
    return std::move(impl);
}

bool TabletSdkImpl::GetSchema(const std::string& db, const std::string& table,
                              type::TableDef* schema, sdk::Status* status) {
    if (schema == NULL || status == NULL) return false;
    ::fesql::tablet::TabletServer_Stub stub(channel_);
    ::fesql::tablet::GetTablesSchemaRequest request;
    ::fesql::tablet::GetTableSchemaReponse response;
    request.set_db(db);
    request.set_name(table);
    brpc::Controller cntl;
    stub.GetTableSchema(&cntl, &request, &response, NULL);
    if (response.status().code() != common::kOk) {
        status->code = response.status().code();
        status->msg = response.status().msg();
        return false;
    }
    if (cntl.Failed()) {
        status->code = (common::kConnError);
        status->msg = ("rpc controll error");
        return false;
    }
    schema->CopyFrom(response.schema());
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

void TabletSdkImpl::BuildInsertRequest(const node::InsertPlanNode* iplan, const std::string& db,
        tablet::InsertRequest* request, sdk::Status *status) {
    const node::InsertStmt* insert_stmt = iplan->GetInsertNode();
    if (nullptr == insert_stmt) {
        status->code = common::kNullPointer;
        status->msg = "fail to execute insert statement with null node";
        return;
    }
    type::TableDef schema;
    if (!GetSchema(db, insert_stmt->table_name_, &schema, status)) {
        if (0 == status->code) {
            status->code = -1;
            status->msg = "Table Not Exist";
        }
        return;
    }
    request->set_table(insert_stmt->table_name_);
    request->set_db(db);
    uint32_t str_size =  0;
    for (auto value : insert_stmt->values_) {
        switch (value->GetExprType()) {
            case node::kExprPrimary: {
                node::ConstNode* primary =
                    dynamic_cast<node::ConstNode*>(value);
                switch (primary->GetDataType()) {
                    case node::kVarchar: {
                        str_size += strlen(primary->GetStr());
                        break;
                    }
                    default: {
                        continue;
                    }
                }
                break;
            }
            default: {
                status->code = common::kTypeError;
                status->msg = "can not insert value with type " +
                             node::ExprTypeName(value->GetExprType());
                return;
            }
        }
    }
    codec::RowBuilder rb(schema.columns());
    uint32_t row_size = rb.CalTotalLength(str_size);
    std::string* row = request->mutable_row();
    row->resize(row_size);
    char* buf = reinterpret_cast<char*>(&(row->at(0)));
    rb.SetBuffer(reinterpret_cast<int8_t*>(buf), row_size);
    auto it = schema.columns().begin();
    uint32_t index = 0;
    for(; it != schema.columns().end(); ++it) {
        if (index >= insert_stmt->values_.size()) {
            break;
        }
        const node::ConstNode* primary = 
            dynamic_cast<const node::ConstNode*>(insert_stmt->values_.at(index));
        index++;
        bool ok = false;
        switch(it->type()) {
            case type::kInt16: {
                ok = rb.AppendInt16(primary->GetSmallInt());
                break;
            }
            case type::kInt32: {
                ok = rb.AppendInt32(primary->GetInt());
                break;
            }
            case type::kInt64: {
                ok = rb.AppendInt64(primary->GetInt());
                break;
            }
            case type::kFloat: {
                ok = rb.AppendFloat(static_cast<float>(primary->GetDouble()));
                break;
            }
            case type::kDouble: {
                ok = rb.AppendDouble(primary->GetDouble());
                break;
            }
            case type::kVarchar: {
                ok = rb.AppendString(primary->GetStr(), strlen(primary->GetStr()));
                break;
            }
            default : {
                status->code = common::kTypeError;
                status->msg = "can not handle data type " +
                               node::DataTypeName(primary->GetDataType());
                LOG(WARNING) << status->msg;
                return;
            }
        }
        if (!ok) {
            status->code = common::kTypeError;
            status->msg = "can not handle data type " +
                               node::DataTypeName(primary->GetDataType());

            LOG(WARNING) << status->msg;
            return;
        }
    }
    status->code = 0;
}

void TabletSdkImpl::Insert(const std::string& db, const std::string& sql,
                               sdk::Status *status) {
    if (status == NULL) {
        LOG(WARNING) << "status is null";
        return;
    }
    node::PlanNodeList plan_trees;
    node::NodeManager node_manager;
    GetSqlPlan(db, sql, node_manager, plan_trees, *status);
    if (0 != status->code) {
        return;
    }
    if (plan_trees.empty() || nullptr == plan_trees[0]) {
        status->msg = "fail to execute plan : plan tree is empty or null";
        status->code = common::kPlanError;
        LOG(WARNING) << status->msg;
        return;
    }
    node::PlanNode* plan = plan_trees[0];
    switch (plan->GetType()) {
        case node::kPlanTypeInsert: {
            node::InsertPlanNode* insert_plan =
                dynamic_cast<node::InsertPlanNode*>(plan);
            tablet::InsertRequest request;
            BuildInsertRequest(insert_plan, db, &request, status);
            if (status->code != 0) {
                return;
            }
            Insert(request, status);
            return;
        }
        default: {
            status->code = common::kUnSupport;
            status->msg = "can not execute plan type " +
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
