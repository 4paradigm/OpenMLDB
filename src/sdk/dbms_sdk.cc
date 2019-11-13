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
#include <fstream>
#include <iostream>
#include <sstream>
#include "analyser/analyser.h"
#include "brpc/channel.h"
#include "node/node_manager.h"
#include "parser/parser.h"
#include "proto/dbms.pb.h"
#include "base/texttable.h"
namespace fesql {
namespace sdk {

class DBMSSdkImpl : public DBMSSdk {
 public:
    explicit DBMSSdkImpl(const std::string &endpoint);
    ~DBMSSdkImpl();
    bool Init();
    void CreateGroup(const GroupDef &group, base::Status &status) override;
    void CreateDatabase(const DatabaseDef &database, base::Status &status);
    void EnterDatabase(const DatabaseDef &database, base::Status &status);
    void CreateTable(const std::string &sql, base::Status &status) override;
    void GetSchema(const std::string &name, type::TableDef &table,
                    base::Status &status) override;
    void GetTables(std::vector<std::string> &names, base::Status &status);
    void GetDatabases(std::vector<std::string> &names, base::Status &status);
    void ExecuteScript(const std::string &sql, base::Status &status) override;
    void PrintTableSchema(fesql::type::TableDef def);
    void PrintItems(std::vector<std::string> items);
 private:
    ::brpc::Channel *channel_;
    std::string endpoint_;
    void handleCmd(node::CmdPlanNode *cmd_node, base::Status &status);
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

void DBMSSdkImpl::CreateGroup(const GroupDef &group, base::Status &status) {
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
void DBMSSdkImpl::GetTables(std::vector<std::string> &names,
                             base::Status &status) {
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::GetItemsRequest request;
    ::fesql::dbms::GetItemsResponse response;
    brpc::Controller cntl;
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

void DBMSSdkImpl::GetDatabases(std::vector<std::string> &names,
                                base::Status &status) {
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

void DBMSSdkImpl::GetSchema(const std::string &name, type::TableDef &table,
                             base::Status &status) {
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::GetSchemaRequest request;
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
/**
 * create table with sql,
 * sql sample:
 * 'CREATE TABLE IF NOT EXISTS table_name (
 *      column1 int NOT NULL,
 *      column2 string NOT NULL,
 *      column3 double NOT NULL,
 *      column4 timestamp NOT NULL,
 *      index(key=(column1,column2), ts=timestamp, ttl=60d)
 * );'
 * @param sql
 * @param status
 */
void DBMSSdkImpl::CreateTable(const std::string &sql, base::Status &status) {
    LOG(INFO) << "create command: " << sql;
    ExecuteScript(sql, status);
}

void DBMSSdkImpl::ExecuteScript(const std::string &sql, base::Status &status) {
    node::NodeManager node_manager;
    parser::FeSQLParser parser;
    analyser::FeSQLAnalyser analyser(&node_manager);
    plan::SimplePlanner planner(&node_manager);

    node::NodePointVector parser_trees;
    parser.parse(sql, parser_trees, &node_manager, status);
    if (0 != status.code) {
        return;
    }
    node::NodePointVector query_trees;
    analyser.Analyse(parser_trees, query_trees, status);
    if (0 != status.code) {
        return;
    }
    node::PlanNodeList plan_trees;
    planner.CreatePlanTree(query_trees, plan_trees, status);

    if (0 != status.code) {
        return;
    }

    node::PlanNode *plan = plan_trees[0];

    if (nullptr == plan) {
        status.msg = "fail to execute plan : plan null";
        status.code = error::kExecuteErrorNullNode;
        return;
    }
    switch (plan->GetType()) {
        case node::kPlanTypeCmd: {
            node::CmdPlanNode *cmd = dynamic_cast<node::CmdPlanNode *>(plan);
            handleCmd(cmd, status);
            return;
        }
        case node::kPlanTypeCreate: {
            node::CreatePlanNode *create =
                dynamic_cast<node::CreatePlanNode *>(plan);

            ::fesql::dbms::DBMSServer_Stub stub(channel_);
            ::fesql::dbms::AddTableRequest request;
            ::fesql::type::TableDef *table = request.mutable_table();
            plan::TransformTableDef(create->GetTableName(),
                                    create->GetColumnDescList(), table, status);

            ::fesql::dbms::AddTableResponse response;
            brpc::Controller cntl;
            stub.AddTable(&cntl, &request, &response, NULL);
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
void DBMSSdkImpl::CreateDatabase(const DatabaseDef &database,
                                 base::Status &status) {
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
void DBMSSdkImpl::EnterDatabase(const DatabaseDef &database,
                                base::Status &status) {
    ::fesql::dbms::DBMSServer_Stub stub(channel_);
    ::fesql::dbms::EnterDatabaseRequest request;
    request.set_name(database.name);
    ::fesql::dbms::EnterDatabaseResponse response;
    brpc::Controller cntl;
    stub.EnterDatabase(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status.code = -1;
        status.msg = "fail to call remote";
    } else {
        status.code = response.status().code();
        status.msg = response.status().msg();
    }
}

void DBMSSdkImpl::PrintTableSchema(fesql::type::TableDef table) {
    ::fesql::base::TextTable t( '-', '|', '+' );

    t.add("Field");
    t.add( "Type" );
    t.add( "Null" );
    t.endOfRow();

    for(auto column : table.columns()) {
        t.add(column.name());
        t.add(fesql::type::Type_Name(column.type()));
        t.add(column.is_null() ? "YES" : "NO");
        t.endOfRow();
    }
    std::cout << t;
}

void DBMSSdkImpl::PrintItems(std::vector<std::string> items) {
    ::fesql::base::TextTable t( '-', '|', '+' );
    for(auto item : items) {
        t.add(item);
        t.endOfRow();
    }
    std::cout << t;
}

void DBMSSdkImpl::handleCmd(node::CmdPlanNode *cmd_node, base::Status &status) {
    switch (cmd_node->GetCmdType()) {
        case node::kCmdShowDatabases: {
            std::vector<std::string> names;
            GetDatabases(names, status);
            if (status.code == 0) {
                PrintItems(names);
            }
            return;
        }
        case node::kCmdShowTables: {
            std::vector<std::string> names;
            GetTables(names, status);
            if (status.code == 0) {
                PrintItems(names);
            }
            return;
        }
        case node::kCmdDescTable: {
            type::TableDef table;
            GetSchema(cmd_node->GetArgs()[0], table, status);
            if (status.code == 0) {
                PrintTableSchema(table);
            }
            break;
        }
        case node::kCmdCreateGroup: {
            GroupDef group;
            group.name = cmd_node->GetArgs()[0];
            CreateGroup(group, status);
            break;
        }
        case node::kCmdCreateDatabase: {
            DatabaseDef db;
            db.name = cmd_node->GetArgs()[0];
            CreateDatabase(db, status);
            break;
        }
        case node::kCmdCreateTable: {
            std::ifstream in;
            in.open(
                cmd_node->GetArgs()[0]);  // open the input file
            if (!in.is_open()) {
                status.code = error::kCmdErrorPathError;
                status.msg = "Incorrect file path";
                return;
            }
            std::stringstream str_stream;
            str_stream << in.rdbuf();  // read the file
            std::string str =
                str_stream.str();  // str holds the content of the file
            ::fesql::base::Status status;
            CreateTable(str, status);
            break;
        }
        case node::kCmdUseDatabase: {
            DatabaseDef db;
            db.name = cmd_node->GetArgs()[0];
            EnterDatabase(db, status);
            break;
        }
        default: {
            status.code = error::kCmdErrorUnSupport;
            status.msg = "UnSupport Cmd " + node::CmdTypeName(cmd_node->GetCmdType());
        }

    }
}
DBMSSdk *CreateDBMSSdk(const std::string &endpoint) {
    DBMSSdkImpl *sdk_impl = new DBMSSdkImpl(endpoint);
    if (sdk_impl->Init()) {
        return sdk_impl;
    }

    return NULL;
}

}  // namespace sdk
}  // namespace fesql
