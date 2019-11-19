/*
 * dbms_sdk.cc
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

#include "sdk/dbms_sdk.h"
#include <plan/planner.h>
#include <iostream>
#include "analyser/analyser.h"
#include "brpc/channel.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "proto/dbms.pb.h"
namespace fesql {
namespace sdk {

class DBMSSdkImpl : public DBMSSdk {
 public:
    explicit DBMSSdkImpl(const std::string &endpoint);
    ~DBMSSdkImpl();
    bool Init();
    void CreateGroup(const GroupDef &group,
                     base::Status &status)  // NOLINT (runtime/references)
        override;
    void CreateDatabase(const DatabaseDef &database,
                        base::Status &status);  // NOLINT (runtime/references)
    bool IsExistDatabase(const DatabaseDef &database,
                         base::Status &status);  // NOLINT (runtime/references)

    void GetSchema(const DatabaseDef &database, const std::string &name,
                   type::TableDef &table,
                   base::Status &status)  // NOLINT (runtime/references)
        override;
    void GetTables(
        const DatabaseDef &database,
        std::vector<std::string> &names,  // NOLINT (runtime/references)
        base::Status &status);            // NOLINT (runtime/references)
    void GetDatabases(
        std::vector<std::string> &names,  // NOLINT (runtime/references)
        base::Status &status);            // NOLINT (runtime/references)
    void ExecuteScript(
        const ExecuteRequst &request, ExecuteResult &result,
        base::Status &status) override;  // NOLINT (runtime/references)

 private:
    ::brpc::Channel *channel_;
    std::string endpoint_;
};

DBMSSdkImpl::DBMSSdkImpl(const std::string &endpoint)
    : channel_(NULL), endpoint_(endpoint) {}

DBMSSdkImpl::~DBMSSdkImpl() { delete channel_; }

bool DBMSSdkImpl::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        return false;
    }
    return true;
}

void DBMSSdkImpl::CreateGroup(
    const GroupDef &group,
    base::Status &status) {  // NOLINT (runtime/references)
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::AddGroupRequest request;
    request.set_name(group.name);
    ::fesql::dbms::AddGroupResponse response;
    brpc::Controller cntl;
    stub.AddGroup(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = -1;
        status.msg = "fail to call remote";
    } else {
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}
void DBMSSdkImpl::GetTables(
    const DatabaseDef &db, std::vector<std::string> &names,
    base::Status &status) {  // NOLINT (runtime/references)
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::GetItemsRequest request;
    ::fesql::dbms::GetItemsResponse response;
    brpc::Controller cntl;

    request.set_db_name(db.name);
    stub.GetTables(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = error::kRpcErrorUnknow;
        status.msg = "fail to call remote";
    } else {
        for (auto item : response.items()) {
            names.push_back(item);
        }
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}

void DBMSSdkImpl::GetDatabases(
    std::vector<std::string> &names,
    base::Status &status) {  // NOLINT (runtime/references)
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::GetItemsRequest request;
    ::fesql::dbms::GetItemsResponse response;
    brpc::Controller cntl;
    stub.GetDatabases(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = error::kRpcErrorUnknow;
        status.msg = "fail to call remote";
    } else {
        for (auto item : response.items()) {
            names.push_back(item);
        }
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}

void DBMSSdkImpl::GetSchema(
    const DatabaseDef &database, const std::string &name, type::TableDef &table,
    base::Status &status) {  // NOLINT (runtime/references)
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::GetSchemaRequest request;
    request.set_db_name(database.name);
    request.set_name(name);
    ::fesql::dbms::GetSchemaResponse response;
    brpc::Controller cntl;
    stub.GetSchema(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = error::kRpcErrorUnknow;
        status.msg = "fail to call remote";
    } else {
        table = response.table();
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}

void DBMSSdkImpl::ExecuteScript(
    const ExecuteRequst &request, ExecuteResult &result,
    base::Status &status) {  // NOLINT (runtime/references)
    node::NodeManager node_manager;
    parser::FeSQLParser parser;
    analyser::FeSQLAnalyser analyser(&node_manager);
    plan::SimplePlanner planner(&node_manager);

    // TODO(chenjing): init with db
    node::NodePointVector parser_trees;
    parser.parse(request.sql, parser_trees, &node_manager, status);
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
    node::PlanNodeList plan_trees;
    planner.CreatePlanTree(query_trees, plan_trees, status);

    if (0 != status.code) {
        LOG(WARNING) << status.msg;
        return;
    }

    node::PlanNode *plan = plan_trees[0];

    if (nullptr == plan) {
        status.msg = "fail to execute plan : plan null";
        status.code = error::kExecuteErrorNullNode;
        LOG(WARNING) << status.msg;
        return;
    }
    switch (plan->GetType()) {
        case node::kPlanTypeCreate: {
            node::CreatePlanNode *create =
                dynamic_cast<node::CreatePlanNode *>(plan);

            ::fesql::dbms::DBMSServer_Stub stub(channel_);
            ::fesql::dbms::AddTableRequest add_table_request;
            ::fesql::dbms::AddTableResponse response;

            add_table_request.set_db_name(request.database.name);

            ::fesql::type::TableDef *table = add_table_request.mutable_table();
            plan::TransformTableDef(create->GetTableName(),
                                    create->GetColumnDescList(), table, status);
            if (0 != status.code) {
                LOG(WARNING) << status.msg;
                return;
            }
            brpc::Controller cntl;
            stub.AddTable(&cntl, &add_table_request, &response, NULL);
            if (cntl.Failed()) {
                status.code = -1;
                status.msg = "fail to call remote";
            } else {
                status.code = response.status().code();
                status.msg = response.status().msg();
            }
            return;
        }

        default: {
            status.msg = "fail to execute script with unSuppurt type" +
                         node::NameOfPlanNodeType(plan->GetType());
            status.code = fesql::error::kExecuteErrorUnSupport;
            return;
        }
    }
}
void DBMSSdkImpl::CreateDatabase(
    const DatabaseDef &database,
    base::Status &status) {  // NOLINT (runtime/references)
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::AddDatabaseRequest request;
    request.set_name(database.name);
    ::fesql::dbms::AddDatabaseResponse response;
    brpc::Controller cntl;
    stub.AddDatabase(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = -1;
        status.msg = "fail to call remote";
    } else {
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}
bool DBMSSdkImpl::IsExistDatabase(
    const DatabaseDef &database,
    base::Status &status) {  // NOLINT (runtime/references)
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::IsExistRequest request;
    request.set_name(database.name);
    ::fesql::dbms::IsExistResponse response;
    brpc::Controller cntl;
    stub.IsExistDatabase(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = -1;
        status.msg = "fail to call remote";
        return false;
    } else {
        status.code = response.status().code();
        status.msg = response.status().msg();
        return response.exist();
    }
}

DBMSSdk *CreateDBMSSdk(const std::string &endpoint) {
    DBMSSdkImpl *sdk_impl = new DBMSSdkImpl(endpoint);
    if (sdk_impl->Init()) {
        return sdk_impl;
    }
    return nullptr;
}

}  // namespace sdk
}  // namespace fesql
