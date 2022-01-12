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

#include "sdk/dbms_sdk.h"

#include <iostream>
#include <map>
#include <memory>
#include <string>
#include <utility>
#include "base/spin_lock.h"
#include "brpc/channel.h"
#include "node/node_manager.h"
#include "proto/dbms.pb.h"
#include "sdk/result_set_impl.h"
#include "sdk/tablet_sdk.h"
#include "plan/plan_api.h"
#include "plan/planner.h"

namespace hybridse {
namespace sdk {

class DBMSSdkImpl : public DBMSSdk {
 public:
    explicit DBMSSdkImpl(const std::string &endpoint);
    ~DBMSSdkImpl();
    bool Init();

    void CreateDatabase(const std::string &catalog, sdk::Status *status);

    std::shared_ptr<TableSet> GetTables(const std::string &catalog,
                                        sdk::Status *status);

    std::vector<std::string> GetDatabases(sdk::Status *status);

    std::shared_ptr<ResultSet> ExecuteQuery(const std::string &catalog,
                                            const std::string &sql,
                                            sdk::Status *status);

    // support select only
    std::shared_ptr<ResultSet> ExecuteQuery(
        const std::string &catalog, const std::string &sql,
        const std::shared_ptr<RequestRow> &row, sdk::Status *status) {
        return tablet_sdk_->Query(catalog, sql, row->GetRow(), status);
    }

    const Schema &GetInputSchema(const std::string &catalog,
                                 const std::string &sql, sdk::Status *status);

    std::shared_ptr<ExplainInfo> Explain(const std::string &catalog,
                                         const std::string &sql,
                                         sdk::Status *status);

    std::shared_ptr<RequestRow> GetRequestRow(const std::string &catalog,
                                              const std::string &sql,
                                              sdk::Status *status);

 private:
    bool InitTabletSdk();

 private:
    ::brpc::Channel *channel_;
    std::string endpoint_;
    std::shared_ptr<TabletSdk> tablet_sdk_;
    base::SpinMutex spin_mutex_;
    // TODO(wangtaize) remove shared_ptr
    std::map<std::string, std::map<std::string, std::shared_ptr<ExplainInfo>>>
        input_schema_map_;
};

DBMSSdkImpl::DBMSSdkImpl(const std::string &endpoint)
    : channel_(NULL), endpoint_(endpoint), tablet_sdk_() {}

DBMSSdkImpl::~DBMSSdkImpl() { delete channel_; }

bool DBMSSdkImpl::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        return false;
    }
    return InitTabletSdk();
}

std::shared_ptr<ExplainInfo> DBMSSdkImpl::Explain(const std::string &catalog,
                                                  const std::string &sql,
                                                  sdk::Status *status) {
    return tablet_sdk_->Explain(catalog, sql, status);
}

std::shared_ptr<RequestRow> DBMSSdkImpl::GetRequestRow(
    const std::string &catalog, const std::string &sql, sdk::Status *status) {
    if (status == NULL) return std::shared_ptr<RequestRow>();
    const Schema &schema = GetInputSchema(catalog, sql, status);
    if (status->code != common::kOk) {
        return std::shared_ptr<RequestRow>();
    }
    return std::shared_ptr<RequestRow>(new RequestRow(&schema));
}

const Schema &DBMSSdkImpl::GetInputSchema(const std::string &catalog,
                                          const std::string &sql,
                                          sdk::Status *status) {
    if (status == NULL) return EMPTY;
    {
        std::lock_guard<base::SpinMutex> lock(spin_mutex_);
        auto it = input_schema_map_.find(catalog);
        if (it != input_schema_map_.end()) {
            auto iit = it->second.find(sql);
            if (iit != it->second.end()) {
                if (status != NULL) status->code = 0;
                return iit->second->GetInputSchema();
            }
        }
    }

    std::shared_ptr<ExplainInfo> info =
        tablet_sdk_->Explain(catalog, sql, status);
    if (status->code != 0) {
        return EMPTY;
    }
    {
        std::lock_guard<base::SpinMutex> lock(spin_mutex_);
        auto it = input_schema_map_.find(catalog);
        if (it == input_schema_map_.end()) {
            std::map<std::string, std::shared_ptr<ExplainInfo>> schemas;
            schemas.insert(std::make_pair(sql, info));
            input_schema_map_.insert(std::make_pair(catalog, schemas));
        } else {
            it->second.insert(std::make_pair(sql, info));
        }
        return input_schema_map_[catalog][sql]->GetInputSchema();
    }
}

bool DBMSSdkImpl::InitTabletSdk() {
    ::hybridse::dbms::DBMSServer_Stub stub(channel_);
    ::hybridse::dbms::GetTabletRequest request;
    ::hybridse::dbms::GetTabletResponse response;
    brpc::Controller cntl;
    stub.GetTablet(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        return false;
    }
    if (response.endpoints_size() <= 0) {
        return false;
    }
    tablet_sdk_ = CreateTabletSdk(response.endpoints().Get(0));
    if (tablet_sdk_) return true;
    return false;
}

std::shared_ptr<TableSet> DBMSSdkImpl::GetTables(const std::string &catalog,
                                                 sdk::Status *status) {
    if (status == NULL) {
        return std::shared_ptr<TableSetImpl>();
    }
    ::hybridse::dbms::DBMSServer_Stub stub(channel_);
    ::hybridse::dbms::GetTablesRequest request;
    ::hybridse::dbms::GetTablesResponse response;
    brpc::Controller cntl;
    request.set_db_name(catalog);
    stub.GetTables(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status->code = common::kRpcError;
        status->msg = "fail to call remote";
        return std::shared_ptr<TableSetImpl>();
    } else {
        std::shared_ptr<TableSetImpl> table_set(
            new TableSetImpl(response.tables()));
        status->code = response.status().code();
        status->msg = response.status().msg();
        return table_set;
    }
}

std::vector<std::string> DBMSSdkImpl::GetDatabases(sdk::Status *status) {
    if (status == NULL) {
        return std::vector<std::string>();
    }
    ::hybridse::dbms::DBMSServer_Stub stub(channel_);
    ::hybridse::dbms::GetDatabasesRequest request;
    ::hybridse::dbms::GetDatabasesResponse response;
    std::vector<std::string> names;
    brpc::Controller cntl;
    stub.GetDatabases(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status->code = common::kRpcError;
        status->msg = "fail to call remote";
    } else {
        for (auto name : response.names()) {
            names.push_back(name);
        }
        status->code = response.status().code();
        status->msg = response.status().msg();
    }
    return names;
}

std::shared_ptr<ResultSet> DBMSSdkImpl::ExecuteQuery(const std::string &catalog,
                                                     const std::string &sql,
                                                     sdk::Status *status) {
    std::shared_ptr<ResultSetImpl> empty;
    node::NodeManager node_manager;
    DLOG(INFO) << "start to execute script from dbms:\n" << sql;

    base::Status sql_status;
    node::PlanNodeList plan_trees;
    plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);
    if (0 != sql_status.code) {
        status->code = sql_status.code;
        status->msg = sql_status.str();
        LOG(WARNING) << status->msg;
        return empty;
    }

    node::PlanNode *plan = plan_trees[0];

    if (nullptr == plan) {
        status->msg = "fail to execute plan : plan null";
        status->code = common::kPlanError;
        LOG(WARNING) << status->msg;
        return empty;
    }

    switch (plan->GetType()) {
        case node::kPlanTypeQuery: {
            return tablet_sdk_->Query(catalog, sql, status);
        }
        case node::kPlanTypeInsert: {
            tablet_sdk_->Insert(catalog, sql, status);
            return empty;
        }
        case node::kPlanTypeCreate: {
            node::CreatePlanNode *create =
                dynamic_cast<node::CreatePlanNode *>(plan);
            ::hybridse::dbms::DBMSServer_Stub stub(channel_);
            ::hybridse::dbms::AddTableRequest add_table_request;
            ::hybridse::dbms::AddTableResponse response;
            ::hybridse::type::TableDef *table =
                add_table_request.mutable_table();
            std::string db_name = create->GetDatabase().empty()? catalog : create->GetDatabase();

            add_table_request.set_db_name(db_name);
            table->set_catalog(db_name);
            sql_status = hybridse::plan::Planner::TransformTableDef(
                create->GetTableName(), create->GetColumnDescList(), table);
            if (!sql_status.isOK()) {
                status->code = sql_status.code;
                status->msg = sql_status.str();
                LOG(WARNING) << status->msg;
                return empty;
            }
            brpc::Controller cntl;
            stub.AddTable(&cntl, &add_table_request, &response, NULL);
            if (cntl.Failed()) {
                status->code = -1;
                status->msg = "fail to call remote";
            } else {
                status->code = response.status().code();
                status->msg = response.status().msg();
            }
            return empty;
        }

        default: {
            status->msg = "fail to execute script with unSuppurt type" +
                          node::NameOfPlanNodeType(plan->GetType());
            status->code = hybridse::common::kUnSupport;
            LOG(WARNING) << status->msg;
            return empty;
        }
    }
}
void DBMSSdkImpl::CreateDatabase(const std::string &catalog,
                                 sdk::Status *status) {
    if (status == NULL) return;
    ::hybridse::dbms::DBMSServer_Stub stub(channel_);
    ::hybridse::dbms::AddDatabaseRequest request;
    request.set_name(catalog);
    ::hybridse::dbms::AddDatabaseResponse response;
    brpc::Controller cntl;
    stub.AddDatabase(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status->code = -1;
        status->msg = "fail to call remote";
    } else {
        status->code = response.status().code();
        status->msg = response.status().msg();
    }
}

std::shared_ptr<DBMSSdk> CreateDBMSSdk(const std::string &endpoint) {
    DBMSSdkImpl *sdk_impl = new DBMSSdkImpl(endpoint);
    if (sdk_impl->Init()) {
        return std::shared_ptr<DBMSSdkImpl>(sdk_impl);
    }
    LOG(WARNING) << "fail to create dbms client with endpoint " << endpoint;
    return std::shared_ptr<DBMSSdk>();
}

}  // namespace sdk
}  // namespace hybridse
