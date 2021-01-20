//
// ns_client.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//

#include "client/ns_client.h"

#include <utility>

#include "base/strings.h"
#include "glog/logging.h"

DECLARE_int32(request_timeout_ms);
namespace rtidb {
namespace client {

NsClient::NsClient(const std::string& endpoint,
    const std::string& real_endpoint)
    : endpoint_(endpoint), client_(endpoint), db_("") {
        if (!real_endpoint.empty()) {
            client_ = ::rtidb::RpcClient<
                ::rtidb::nameserver::NameServer_Stub>(real_endpoint);
        }
    }

int NsClient::Init() { return client_.Init(); }

std::string NsClient::GetEndpoint() { return endpoint_; }

const std::string& NsClient::GetDb() { return db_; }

void NsClient::ClearDb() { db_.clear(); }

bool NsClient::Use(std::string db, std::string& msg) {
    ::rtidb::nameserver::UseDatabaseRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_db(db);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::UseDatabase,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        db_ = db;
        return true;
    }
    return false;
}

bool NsClient::CreateDatabase(const std::string& db, std::string& msg) {
    if (db.empty()) {
        return false;
    }
    ::rtidb::nameserver::CreateDatabaseRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_db(db);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::CreateDatabase, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    return ok && response.code() == 0;
}

bool NsClient::ShowDatabase(std::vector<std::string>* dbs, std::string& msg) {
    ::rtidb::nameserver::GeneralRequest request;
    ::rtidb::nameserver::ShowDatabaseResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowDatabase,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    for (auto db : response.db()) {
        dbs->push_back(db);
    }
    msg = response.msg();
    return ok && response.code() == 0;
}

bool NsClient::DropDatabase(const std::string& db, std::string& msg) {
    ::rtidb::nameserver::DropDatabaseRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_db(db);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DropDatabase,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    return ok && response.code() == 0;
}

bool NsClient::ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg) {
    ::rtidb::nameserver::ShowTabletRequest request;
    ::rtidb::nameserver::ShowTabletResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTablet,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::rtidb::nameserver::TabletStatus& status =
                response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.state = status.state();
            info.age = status.age();
            info.real_endpoint = status.real_endpoint();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowBlobServer(std::vector<TabletInfo>& tablets,
        std::string& msg) {
    ::rtidb::nameserver::ShowBlobServerRequest request;
    ::rtidb::nameserver::ShowBlobServerResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::
            ShowBlobServer, &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::rtidb::nameserver::TabletStatus& status =
                response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.state = status.state();
            info.age = status.age();
            info.real_endpoint = status.real_endpoint();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowSdkEndpoint(std::vector<TabletInfo>& tablets,
        std::string& msg) {
    ::rtidb::nameserver::ShowSdkEndpointRequest request;
    ::rtidb::nameserver::ShowSdkEndpointResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::
            ShowSdkEndpoint, &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.tablets_size(); i++) {
            const ::rtidb::nameserver::TabletStatus& status =
                response.tablets(i);
            TabletInfo info;
            info.endpoint = status.endpoint();
            info.real_endpoint = status.real_endpoint();
            tablets.push_back(info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowTable(const std::string& name, const std::string& db,
                         bool show_all,
                         std::vector<::rtidb::nameserver::TableInfo>& tables,
                         std::string& msg) {
    ::rtidb::nameserver::ShowTableRequest request;
    if (!name.empty()) {
        request.set_name(name);
    }
    request.set_db(db);
    request.set_show_all(show_all);
    ::rtidb::nameserver::ShowTableResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int32_t i = 0; i < response.table_info_size(); i++) {
            ::rtidb::nameserver::TableInfo table_info;
            table_info.CopyFrom(response.table_info(i));
            tables.push_back(table_info);
        }
        return true;
    }
    return false;
}

bool NsClient::ShowTable(const std::string& name,
                         std::vector<::rtidb::nameserver::TableInfo>& tables,
                         std::string& msg) {
    return ShowTable(name, GetDb(), false, tables, msg);
}

bool NsClient::ShowAllTable(std::vector<::rtidb::nameserver::TableInfo>& tables,
                            std::string& msg) {
    return ShowTable("", "", true, tables, msg);
}

bool NsClient::MakeSnapshot(const std::string& name, uint32_t pid,
                            uint64_t end_offset, std::string& msg) {
    return MakeSnapshot(name, GetDb(), pid, end_offset, msg);
}

bool NsClient::MakeSnapshot(const std::string& name, const std::string& db,
                            uint32_t pid, uint64_t end_offset,
                            std::string& msg) {
    ::rtidb::nameserver::MakeSnapshotNSRequest request;
    request.set_name(name);
    request.set_pid(pid);
    request.set_offset(end_offset);
    request.set_db(db);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::MakeSnapshotNS, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response,
                            const std::string& name, uint32_t pid,
                            std::string& msg) {
    ::rtidb::nameserver::ShowOPStatusRequest request;
    if (!name.empty()) {
        request.set_name(name);
        request.set_db(GetDb());
    }
    if (pid != INVALID_PID) {
        request.set_pid(pid);
    }
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowOPStatus,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CancelOP(uint64_t op_id, std::string& msg) {
    ::rtidb::nameserver::CancelOPRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_op_id(op_id);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CancelOP,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddTableField(const std::string& table_name,
                             const ::rtidb::common::ColumnDesc& column_desc,
                             std::string& msg) {
    ::rtidb::nameserver::AddTableFieldRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(table_name);
    request.set_db(GetDb());
    ::rtidb::common::ColumnDesc* column_desc_ptr =
        request.mutable_column_desc();
    column_desc_ptr->CopyFrom(column_desc);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::AddTableField, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ExecuteSQL(const std::string& script, std::string& msg) {
    return ExecuteSQL(GetDb(), script, msg);
}

bool NsClient::ExecuteSQL(const std::string& db, const std::string& script,
                          std::string& msg) {
    fesql::node::NodeManager node_manager;
    fesql::parser::FeSQLParser parser;
    DLOG(INFO) << "start to execute script from dbms:\n" << script;
    fesql::base::Status sql_status;
    fesql::node::NodePointVector parser_trees;
    parser.parse(script, parser_trees, &node_manager, sql_status);
    if (parser_trees.empty() || 0 != sql_status.code) {
        msg = sql_status.msg;
        return false;
    }
    fesql::node::SQLNode* node = parser_trees[0];
    switch (node->GetType()) {
        case fesql::node::kCmdStmt: {
            fesql::node::CmdNode* cmd =
                dynamic_cast<fesql::node::CmdNode*>(node);
            bool ok = HandleSQLCmd(cmd, db, &sql_status);
            if (!ok) {
                msg = sql_status.msg;
            }
            return ok;
        }
        case fesql::node::kCreateStmt: {
            bool ok = HandleSQLCreateTable(parser_trees, db, &node_manager,
                                           &sql_status);
            if (!ok) {
                msg = sql_status.msg;
            }
            return ok;
        }
        default: {
            msg = "fail to execute script with unSuppurt type";
            return false;
        }
    }
}

bool NsClient::HandleSQLCmd(const fesql::node::CmdNode* cmd_node,
                            const std::string& db,
                            fesql::base::Status* sql_status) {
    switch (cmd_node->GetCmdType()) {
        case fesql::node::kCmdDropTable: {
            std::string name = cmd_node->GetArgs()[0];
            std::string error;
            bool ok = DropTable(db, name, error);
            if (ok) {
                return true;
            } else {
                sql_status->msg = error;
                return false;
            }
        }
        case fesql::node::kCmdDropIndex: {
            std::string index_name = cmd_node->GetArgs()[0];
            std::string table_name = cmd_node->GetArgs()[1];
            std::string error;

            bool ok = DeleteIndex(db, table_name, index_name, error);
            if (ok) {
                return true;
            } else {
                sql_status->msg = error;
                return false;
            }
        }
        case fesql::node::kCmdDropSp: {
            std::string sp_name = cmd_node->GetArgs()[0];
            std::string error;
            bool ok = DropProcedure(db, sp_name, error);
            if (ok) {
                return true;
            } else {
                sql_status->msg = error;
                return false;
            }
        }
        default: {
            sql_status->msg = "fail to execute script with unSuppurt type";
            return false;
        }
    }
}

bool NsClient::HandleSQLCreateTable(
    const fesql::node::NodePointVector& parser_trees, const std::string& db,
    fesql::node::NodeManager* node_manager, fesql::base::Status* sql_status) {
    fesql::plan::SimplePlanner planner(node_manager);
    fesql::node::PlanNodeList plan_trees;
    planner.CreatePlanTree(parser_trees, plan_trees, *sql_status);
    if (plan_trees.empty() || 0 != sql_status->code) {
        return false;
    }

    fesql::node::PlanNode* plan = plan_trees[0];
    if (nullptr == plan) {
        sql_status->msg = "fail to execute plan : plan null";
        return false;
    }

    switch (plan->GetType()) {
        case fesql::node::kPlanTypeCreate: {
            fesql::node::CreatePlanNode* create =
                dynamic_cast<fesql::node::CreatePlanNode*>(plan);
            ::rtidb::nameserver::CreateTableRequest request;
            ::rtidb::nameserver::GeneralResponse response;
            ::rtidb::nameserver::TableInfo* table_info =
                request.mutable_table_info();
            table_info->set_db(db);
            TransformToTableDef(create, table_info, sql_status);
            if (0 != sql_status->code) {
                return false;
            }
            client_.SendRequest(
                &::rtidb::nameserver::NameServer_Stub::CreateTable, &request,
                &response, FLAGS_request_timeout_ms, 1);
            sql_status->msg = response.msg();
            if (0 != response.code()) {
                return false;
            }
            break;
        }
        default: {
            sql_status->msg = "fail to execute script with unSuppurt type" +
                              fesql::node::NameOfPlanNodeType(plan->GetType());
            return false;
        }
    }
    return true;
}

bool NsClient::CreateProcedure(const ::rtidb::api::ProcedureInfo& sp_info,
        uint64_t request_timeout, std::string* msg) {
    if (msg == nullptr) return false;
    ::rtidb::api::CreateProcedureRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::api::ProcedureInfo* sp_info_ptr = request.mutable_sp_info();
    sp_info_ptr->CopyFrom(sp_info);
    request.set_timeout_ms(request_timeout);
    bool ok = client_.SendRequest(
            &::rtidb::nameserver::NameServer_Stub::CreateProcedure, &request,
            &response, request_timeout, 1);
    *msg = response.msg();
    if (!ok || response.code() != 0) {
        return false;
    }
    return true;
}

bool NsClient::CreateTable(const ::rtidb::nameserver::TableInfo& table_info,
                           std::string& msg) {
    ::rtidb::nameserver::CreateTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::TableInfo* table_info_r = request.mutable_table_info();
    table_info_r->CopyFrom(table_info);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CreateTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DropTable(const std::string& name, std::string& msg) {
    return DropTable(GetDb(), name, msg);
}

bool NsClient::DropTable(const std::string& db, const std::string& name,
                         std::string& msg) {
    ::rtidb::nameserver::DropTableRequest request;
    request.set_name(name);
    request.set_db(db);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DropTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SyncTable(const std::string& name,
                         const std::string& cluster_alias, uint32_t pid,
                         std::string& msg) {
    ::rtidb::nameserver::SyncTableRequest request;
    request.set_name(name);
    request.set_cluster_alias(cluster_alias);
    if (pid != INVALID_PID) {
        request.set_pid(pid);
    }
    request.set_db(GetDb());
    ::rtidb::nameserver::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::SyncTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SetSdkEndpoint(const std::string& server_name,
        const std::string& sdk_endpoint, std::string* msg) {
    ::rtidb::nameserver::SetSdkEndpointRequest request;
    request.set_server_name(server_name);
    request.set_sdk_endpoint(sdk_endpoint);
    ::rtidb::nameserver::GeneralResponse response;
    bool ok = client_.SendRequest(
            &::rtidb::nameserver::NameServer_Stub::SetSdkEndpoint,
            &request, &response, FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplica(const std::string& name,
                          const std::set<uint32_t>& pid_set,
                          const std::string& endpoint, std::string& msg) {
    if (pid_set.empty()) {
        return false;
    }
    ::rtidb::nameserver::AddReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(*(pid_set.begin()));
    request.set_endpoint(endpoint);
    request.set_db(GetDb());
    if (pid_set.size() > 1) {
        for (auto pid : pid_set) {
            request.add_pid_group(pid);
        }
    }
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddReplicaNS,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaNS(const std::string& name,
                            const std::vector<std::string>& endpoint_vec,
                            uint32_t pid,
                            const ::rtidb::nameserver::ZoneInfo& zone_info,
                            const ::rtidb::api::TaskInfo& task_info) {
    if (endpoint_vec.empty()) {
        return false;
    }
    ::rtidb::nameserver::AddReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    for (auto& endpoint : endpoint_vec) {
        request.add_endpoint_group(endpoint);
    }
    request.set_pid(pid);
    request.set_endpoint(endpoint_vec.front());
    ::rtidb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::rtidb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::AddReplicaNSFromRemote, &request,
        &response, FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DelReplica(const std::string& name,
                          const std::set<uint32_t>& pid_set,
                          const std::string& endpoint, std::string& msg) {
    if (pid_set.empty()) {
        return false;
    }
    ::rtidb::nameserver::DelReplicaNSRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(*(pid_set.begin()));
    request.set_endpoint(endpoint);
    request.set_db(GetDb());
    if (pid_set.size() > 1) {
        for (auto pid : pid_set) {
            request.add_pid_group(pid);
        }
    }
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DelReplicaNS,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfSet(const std::string& key, const std::string& value,
                       std::string& msg) {
    ::rtidb::nameserver::ConfSetRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::nameserver::Pair* conf = request.mutable_conf();
    conf->set_key(key);
    conf->set_value(value);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConfSet,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConfGet(const std::string& key,
                       std::map<std::string, std::string>& conf_map,
                       std::string& msg) {
    conf_map.clear();
    ::rtidb::nameserver::ConfGetRequest request;
    ::rtidb::nameserver::ConfGetResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConfGet,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        for (int idx = 0; idx < response.conf_size(); idx++) {
            if (key.empty()) {
                conf_map.insert(std::make_pair(response.conf(idx).key(),
                                               response.conf(idx).value()));
            } else if (key == response.conf(idx).key()) {
                conf_map.insert(
                    std::make_pair(key, response.conf(idx).value()));
                break;
            }
        }
        if (!key.empty() && conf_map.empty()) {
            msg = "cannot found key " + key;
            return false;
        }
        return true;
    }
    return false;
}

bool NsClient::ChangeLeader(const std::string& name, uint32_t pid,
                            std::string& candidate_leader, std::string& msg) {
    ::rtidb::nameserver::ChangeLeaderRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    if (!candidate_leader.empty()) {
        request.set_candidate_leader(candidate_leader);
    }
    request.set_db(GetDb());
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ChangeLeader,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::OfflineEndpoint(const std::string& endpoint,
                               uint32_t concurrency, std::string& msg) {
    ::rtidb::nameserver::OfflineEndpointRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    if (concurrency > 0) {
        request.set_concurrency(concurrency);
    }
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::OfflineEndpoint, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::Migrate(const std::string& src_endpoint, const std::string& name,
                       const std::set<uint32_t>& pid_set,
                       const std::string& des_endpoint, std::string& msg) {
    ::rtidb::nameserver::MigrateRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_src_endpoint(src_endpoint);
    request.set_name(name);
    request.set_des_endpoint(des_endpoint);
    request.set_db(GetDb());
    for (auto pid : pid_set) {
        request.add_pid(pid);
    }
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::Migrate,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::RecoverEndpoint(const std::string& endpoint, bool need_restore,
                               uint32_t concurrency, std::string& msg) {
    ::rtidb::nameserver::RecoverEndpointRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    if (concurrency > 0) {
        request.set_concurrency(concurrency);
    }
    request.set_need_restore(need_restore);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::RecoverEndpoint, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::RecoverTable(const std::string& name, uint32_t pid,
                            const std::string& endpoint, std::string& msg) {
    ::rtidb::nameserver::RecoverTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_endpoint(endpoint);
    request.set_db(GetDb());
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::RecoverTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::ConnectZK(std::string& msg) {
    ::rtidb::nameserver::ConnectZKRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ConnectZK,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DisConnectZK(std::string& msg) {
    ::rtidb::nameserver::DisConnectZKRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DisConnectZK,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SetTablePartition(
    const std::string& name,
    const ::rtidb::nameserver::TablePartition& table_partition,
    std::string& msg) {
    ::rtidb::nameserver::SetTablePartitionRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_db(GetDb());
    ::rtidb::nameserver::TablePartition* cur_table_partition =
        request.mutable_table_partition();
    cur_table_partition->CopyFrom(table_partition);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::SetTablePartition, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::GetTablePartition(
    const std::string& name, uint32_t pid,
    ::rtidb::nameserver::TablePartition& table_partition, std::string& msg) {
    ::rtidb::nameserver::GetTablePartitionRequest request;
    ::rtidb::nameserver::GetTablePartitionResponse response;
    request.set_name(name);
    request.set_pid(pid);
    request.set_db(GetDb());
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::GetTablePartition, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        table_partition.CopyFrom(response.table_partition());
        return true;
    }
    return false;
}

bool NsClient::UpdateTableAliveStatus(const std::string& endpoint,
                                      std::string& name, uint32_t pid,
                                      bool is_alive, std::string& msg) {
    ::rtidb::nameserver::UpdateTableAliveRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_endpoint(endpoint);
    request.set_name(name);
    request.set_is_alive(is_alive);
    if (pid < UINT32_MAX) {
        request.set_pid(pid);
    }
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::UpdateTableAliveStatus, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::UpdateTTL(const std::string& name,
                         const ::rtidb::api::TTLType& type, uint64_t abs_ttl,
                         uint64_t lat_ttl, const std::string& ts_name,
                         std::string& msg) {
    ::rtidb::nameserver::UpdateTTLRequest request;
    ::rtidb::nameserver::UpdateTTLResponse response;
    request.set_name(name);
    ::rtidb::api::TTLDesc* ttl_desc = request.mutable_ttl_desc();
    ttl_desc->set_ttl_type(type);
    ttl_desc->set_abs_ttl(abs_ttl);
    ttl_desc->set_lat_ttl(lat_ttl);
    if (type == ::rtidb::api::TTLType::kAbsoluteTime) {
        request.set_ttl_type("kAbsoluteTime");
        request.set_value(abs_ttl);
    } else {
        request.set_ttl_type("kLatestTime");
        request.set_value(lat_ttl);
    }
    if (!ts_name.empty()) {
        request.set_ts_name(ts_name);
    }
    request.set_db(GetDb());
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::UpdateTTL,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DeleteOPTask(const std::vector<uint64_t>& op_id_vec) {
    ::rtidb::api::DeleteTaskRequest request;
    ::rtidb::api::GeneralResponse response;
    for (auto op_id : op_id_vec) {
        request.add_op_id(op_id);
    }
    bool ret =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DeleteOPTask,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool NsClient::GetTaskStatus(::rtidb::api::TaskStatusResponse& response) {
    ::rtidb::api::TaskStatusRequest request;
    bool ret = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::GetTaskStatus, &request,
        &response, FLAGS_request_timeout_ms, 1);
    if (!ret || response.code() != 0) {
        return false;
    }
    return true;
}

bool NsClient::LoadTable(const std::string& name, const std::string& endpoint,
                         uint32_t pid,
                         const ::rtidb::nameserver::ZoneInfo& zone_info,
                         const ::rtidb::api::TaskInfo& task_info) {
    return LoadTable(name, GetDb(), endpoint, pid, zone_info, task_info);
}

bool NsClient::LoadTable(const std::string& name, const std::string& db,
                         const std::string& endpoint, uint32_t pid,
                         const ::rtidb::nameserver::ZoneInfo& zone_info,
                         const ::rtidb::api::TaskInfo& task_info) {
    ::rtidb::nameserver::LoadTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_name(name);
    request.set_endpoint(endpoint);
    request.set_pid(pid);
    request.set_db(db);
    ::rtidb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::rtidb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::LoadTable,
                            &request, &response, FLAGS_request_timeout_ms, 3);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateRemoteTableInfo(
    const ::rtidb::nameserver::ZoneInfo& zone_info,
    ::rtidb::nameserver::TableInfo& table_info, std::string& msg) {
    ::rtidb::nameserver::CreateTableInfoRequest request;
    ::rtidb::nameserver::CreateTableInfoResponse response;
    ::rtidb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    ::rtidb::nameserver::TableInfo* table_info_p = request.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::CreateTableInfo, &request,
        &response, FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    table_info = response.table_info();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateRemoteTableInfoSimply(
    const ::rtidb::nameserver::ZoneInfo& zone_info,
    ::rtidb::nameserver::TableInfo& table_info, std::string& msg) {
    ::rtidb::nameserver::CreateTableInfoRequest request;
    ::rtidb::nameserver::CreateTableInfoResponse response;
    ::rtidb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    ::rtidb::nameserver::TableInfo* table_info_p = request.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::CreateTableInfoSimply, &request,
        &response, FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    table_info = response.table_info();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DropTableRemote(const ::rtidb::api::TaskInfo& task_info,
                               const std::string& name, const std::string& db,
                               const ::rtidb::nameserver::ZoneInfo& zone_info,
                               std::string& msg) {
    ::rtidb::nameserver::DropTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::rtidb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    request.set_name(name);
    request.set_db(db);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DropTable,
                            &request, &response, FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::CreateTableRemote(
    const ::rtidb::api::TaskInfo& task_info,
    const ::rtidb::nameserver::TableInfo& table_info,
    const ::rtidb::nameserver::ZoneInfo& zone_info, std::string& msg) {
    ::rtidb::nameserver::CreateTableRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::api::TaskInfo* task_info_p = request.mutable_task_info();
    task_info_p->CopyFrom(task_info);
    ::rtidb::nameserver::ZoneInfo* zone_info_p = request.mutable_zone_info();
    zone_info_p->CopyFrom(zone_info);
    ::rtidb::nameserver::TableInfo* table_info_p;
    table_info_p = request.mutable_table_info();
    table_info_p->CopyFrom(table_info);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::CreateTable,
                            &request, &response, FLAGS_request_timeout_ms, 3);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaClusterByNs(const std::string& alias,
                                     const std::string& name,
                                     const uint64_t term, std::string& msg) {
    ::rtidb::nameserver::ReplicaClusterByNsRequest request;
    ::rtidb::nameserver::ZoneInfo* zone_info = request.mutable_zone_info();
    ::rtidb::nameserver::AddReplicaClusterByNsResponse response;
    zone_info->set_replica_alias(alias);
    zone_info->set_zone_name(name);
    zone_info->set_zone_term(term);
    zone_info->set_mode(::rtidb::nameserver::kFOLLOWER);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::AddReplicaClusterByNs, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && ((response.code() == 0) || (response.code() == 408))) {
        return true;
    }
    return false;
}

bool NsClient::AddReplicaCluster(const std::string& zk_ep,
                                 const std::string& zk_path,
                                 const std::string& alias, std::string& msg) {
    ::rtidb::nameserver::ClusterAddress request;
    ::rtidb::nameserver::GeneralResponse response;
    if (zk_ep.size() < 1 || zk_path.size() < 1 || alias.size() < 1) {
        msg = "zookeeper endpoints or zk_path or alias is null";
        return false;
    }
    request.set_alias(alias);
    request.set_zk_path(zk_path);
    request.set_zk_endpoints(zk_ep);

    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::AddReplicaCluster, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();

    if (ok && (response.code() == 0)) {
        return true;
    }
    return false;
}

bool NsClient::ShowReplicaCluster(
    std::vector<::rtidb::nameserver::ClusterAddAge>& clusterinfo,
    std::string& msg) {
    clusterinfo.clear();
    ::rtidb::nameserver::GeneralRequest request;
    ::rtidb::nameserver::ShowReplicaClusterResponse response;
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::ShowReplicaCluster, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && (response.code() == 0)) {
        for (int32_t i = 0; i < response.replicas_size(); i++) {
            auto status = response.replicas(i);
            clusterinfo.push_back(status);
        }
        return true;
    }

    return false;
}

bool NsClient::RemoveReplicaCluster(const std::string& alias,
                                    std::string& msg) {
    ::rtidb::nameserver::RemoveReplicaOfRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_alias(alias);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::RemoveReplicaCluster, &request,
        &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::RemoveReplicaClusterByNs(const std::string& alias,
                                        const std::string& zone_name,
                                        const uint64_t term, int& code,
                                        std::string& msg) {
    ::rtidb::nameserver::ReplicaClusterByNsRequest request;
    ::rtidb::nameserver::ZoneInfo* zone_info = request.mutable_zone_info();
    ::rtidb::nameserver::GeneralResponse response;
    zone_info->set_replica_alias(alias);
    zone_info->set_zone_term(term);
    zone_info->set_zone_name(zone_name);
    zone_info->set_mode(::rtidb::nameserver::kNORMAL);
    bool ok = client_.SendRequest(
        &::rtidb::nameserver::NameServer_Stub::RemoveReplicaClusterByNs,
        &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::SwitchMode(const ::rtidb::nameserver::ServerMode mode,
                          std::string& msg) {
    ::rtidb::nameserver::SwitchModeRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_sm(mode);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::SwitchMode,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::AddIndex(const std::string& table_name,
                        const ::rtidb::common::ColumnKey& column_key, std::vector<rtidb::common::ColumnDesc>* cols,
                        std::string& msg) {
    ::rtidb::nameserver::AddIndexRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    ::rtidb::common::ColumnKey* cur_column_key = request.mutable_column_key();
    request.set_name(table_name);
    cur_column_key->CopyFrom(column_key);
    request.set_db(GetDb());
    if (cols != nullptr) {
        for (const auto& col : *cols) {
            rtidb::common::ColumnDesc* new_col = request.add_cols();
            new_col->CopyFrom(col);
        }
    }
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::AddIndex,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool NsClient::DeleteIndex(const std::string& db, const std::string& table_name,
                           const std::string& idx_name, std::string& msg) {
    ::rtidb::nameserver::DeleteIndexRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_table_name(table_name);
    request.set_idx_name(idx_name);
    request.set_db_name(db);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DeleteIndex,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    int code = response.code();
    return ok && code == 0;
}

bool NsClient::DeleteIndex(const std::string& table_name,
                           const std::string& idx_name, std::string& msg) {
    return DeleteIndex(GetDb(), table_name, idx_name, msg);
}

bool NsClient::ShowCatalogVersion(std::map<std::string, uint64_t>* version_map, std::string* msg) {
    if (version_map == nullptr || msg == nullptr) {
        return false;
    }
    version_map->clear();
    ::rtidb::nameserver::ShowCatalogRequest request;
    ::rtidb::nameserver::ShowCatalogResponse response;
    bool ok = client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::ShowCatalog, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    int code = response.code();
    if (ok && code == 0) {
        for (const auto& catalog_info : response.catalog()) {
            version_map->emplace(catalog_info.endpoint(), catalog_info.version());
        }
        return true;
    }
    *msg = response.msg();
    return false;
}

bool NsClient::TransformToTableDef(
    ::fesql::node::CreatePlanNode* create_node,
    ::rtidb::nameserver::TableInfo* table, fesql::plan::Status* status) {
    if (create_node == NULL || table == NULL || status == NULL) return false;
    std::string table_name = create_node->GetTableName();
    const fesql::node::NodePointVector& column_desc_list = create_node->GetColumnDescList();
    const fesql::node::NodePointVector& distribution_list = create_node->GetDistributionList();
    std::set<std::string> index_names;
    std::map<std::string, ::rtidb::common::ColumnDesc*> column_names;
    table->set_name(table_name);
    // todo: change default setting
    int replica_num = create_node->GetReplicaNum();
    if (replica_num <= 0) {
        status->msg = "CREATE common: replica_num should be bigger than 0";
        status->code = fesql::common::kSQLError;
        return false;
    }
    table->set_replica_num((uint32_t)replica_num);
    int partition_num = create_node->GetPartitionNum();
    if (partition_num <= 0) {
        status->msg = "CREATE common: partition_num should be greater than 0";
        status->code = fesql::common::kSQLError;
        return false;
    }
    table->set_partition_num(create_node->GetPartitionNum());
    table->set_format_version(1);
    ::rtidb::api::TTLDesc* ttl_desc = table->mutable_ttl_desc();
    ttl_desc->set_ttl_type(::rtidb::api::TTLType::kAbsoluteTime);
    ttl_desc->set_abs_ttl(0);
    ttl_desc->set_lat_ttl(0);
    int no_ts_cnt = 0;
    for (auto column_desc : column_desc_list) {
        switch (column_desc->GetType()) {
            case fesql::node::kColumnDesc: {
                fesql::node::ColumnDefNode* column_def =
                    (fesql::node::ColumnDefNode*)column_desc;
                ::rtidb::common::ColumnDesc* column_desc =
                    table->add_column_desc_v1();
                if (column_names.find(column_desc->name()) !=
                    column_names.end()) {
                    status->msg = "CREATE common: COLUMN NAME " +
                                  column_def->GetColumnName() + " duplicate";
                    status->code = fesql::common::kSQLError;
                    return false;
                }
                column_desc->set_name(column_def->GetColumnName());
                column_desc->set_not_null(column_def->GetIsNotNull());
                column_names.insert(
                    std::make_pair(column_def->GetColumnName(), column_desc));
                switch (column_def->GetColumnType()) {
                    case fesql::node::kBool:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kBool);
                        column_desc->set_type("bool");
                        break;
                    case fesql::node::kInt16:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kSmallInt);
                        column_desc->set_type("int16");
                        break;
                    case fesql::node::kInt32:
                        column_desc->set_data_type(rtidb::type::DataType::kInt);
                        column_desc->set_type("int32");
                        break;
                    case fesql::node::kInt64:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kBigInt);
                        column_desc->set_type("int64");
                        break;
                    case fesql::node::kFloat:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kFloat);
                        column_desc->set_type("float");
                        break;
                    case fesql::node::kDouble:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kDouble);
                        column_desc->set_type("double");
                        break;
                    case fesql::node::kTimestamp: {
                        column_desc->set_data_type(
                            rtidb::type::DataType::kTimestamp);
                        column_desc->set_type("timestamp");
                        break;
                    }
                    case fesql::node::kVarchar:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kVarchar);
                        column_desc->set_type("string");
                        break;
                    case fesql::node::kDate:
                        column_desc->set_data_type(
                            rtidb::type::DataType::kDate);
                        column_desc->set_type("date");
                        break;
                    default: {
                        status->msg = "CREATE common: column type " +
                                      fesql::node::DataTypeName(
                                          column_def->GetColumnType()) +
                                      " is not supported";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                }
                break;
            }

            case fesql::node::kColumnIndex: {
                fesql::node::ColumnIndexNode* column_index =
                    (fesql::node::ColumnIndexNode*)column_desc;

                if (column_index->GetName().empty()) {
                    column_index->SetName(fesql::plan::GenerateName(
                        "INDEX", table->column_key_size()));
                }
                if (index_names.find(column_index->GetName()) !=
                    index_names.end()) {
                    status->msg = "CREATE common: INDEX NAME " +
                                  column_index->GetName() + " duplicate";
                    status->code = fesql::common::kSQLError;
                    return false;
                }
                index_names.insert(column_index->GetName());
                ::rtidb::common::ColumnKey* index = table->add_column_key();
                index->set_index_name(column_index->GetName());

                if (column_index->GetKey().empty()) {
                    status->msg = "CREATE common: INDEX KEY empty";
                    status->code = fesql::common::kSQLError;
                    return false;
                }
                for (auto key : column_index->GetKey()) {
                    auto cit = column_names.find(key);
                    if (cit == column_names.end()) {
                        status->msg = "column " + key + " does not exist";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    cit->second->set_add_ts_idx(true);
                    index->add_col_name(key);
                }
                if (!column_index->ttl_type().empty()) {
                    std::string ttl_type = column_index->ttl_type();
                    std::transform(ttl_type.begin(), ttl_type.end(), ttl_type.begin(), ::tolower);
                    if (ttl_type == "absolute") {
                        table->mutable_ttl_desc()->set_ttl_type(
                                rtidb::api::kAbsoluteTime);
                    } else if (ttl_type == "latest") {
                        table->mutable_ttl_desc()->set_ttl_type(
                                rtidb::api::kLatestTime);
                    } else if (ttl_type == "absorlat") {
                        table->mutable_ttl_desc()->set_ttl_type(
                                rtidb::api::kAbsOrLat);
                    } else if (ttl_type == "absandlat") {
                        table->mutable_ttl_desc()->set_ttl_type(
                                rtidb::api::kAbsAndLat);
                    } else {
                        status->msg = "CREATE common: ttl_type " +
                                      column_index->ttl_type() +
                                      " not support";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                }
                if (table->ttl_desc().ttl_type() == rtidb::api::kAbsoluteTime) {
                    if (column_index->GetAbsTTL() == -1 || column_index->GetLatTTL() != -2) {
                        status->msg = "CREATE common: abs ttl format error";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    if (column_index->GetAbsTTL() == -2) {
                        table->mutable_ttl_desc()->set_abs_ttl(0);
                    } else {
                        table->mutable_ttl_desc()->set_abs_ttl(column_index->GetAbsTTL() / 60000);
                    }
                } else if (table->ttl_desc().ttl_type() == rtidb::api::kLatestTime) {
                    if (column_index->GetLatTTL() == -1 || column_index->GetAbsTTL() != -2) {
                        status->msg = "CREATE common: lat ttl format error";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    if (column_index->GetLatTTL() == -2) {
                        table->mutable_ttl_desc()->set_lat_ttl(0);
                    } else {
                        table->mutable_ttl_desc()->set_lat_ttl(column_index->GetLatTTL());
                    }
                } else {
                    if (column_index->GetAbsTTL() == -1) {
                        status->msg = "CREATE common: abs ttl format error";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    if (column_index->GetAbsTTL() == -2) {
                        table->mutable_ttl_desc()->set_abs_ttl(0);
                    } else {
                        table->mutable_ttl_desc()->set_abs_ttl(column_index->GetAbsTTL() / 60000);
                    }
                    if (column_index->GetLatTTL() == -1) {
                        status->msg = "CREATE common: lat ttl format error";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    if (column_index->GetLatTTL() == -2) {
                        table->mutable_ttl_desc()->set_lat_ttl(0);
                    } else {
                        table->mutable_ttl_desc()->set_lat_ttl(column_index->GetLatTTL());
                    }
                }
                if (!column_index->GetTs().empty()) {
                    index->add_ts_name(column_index->GetTs());
                    auto it = column_names.find(column_index->GetTs());
                    if (it == column_names.end()) {
                        status->msg = "CREATE common: TS NAME " +
                                      column_index->GetTs() + " not exists";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    switch (it->second->data_type()) {
                        case rtidb::type::DataType::kInt:
                        case rtidb::type::DataType::kSmallInt:
                        case rtidb::type::DataType::kBigInt:
                        case rtidb::type::DataType::kTimestamp: {
                            it->second->set_is_ts_col(true);
                            break;
                        }
                        default: {
                            status->msg = "CREATE common: TS Type " +
                                          rtidb::type::DataType_Name(
                                              it->second->data_type()) +
                                          " not support";
                            status->code = fesql::common::kSQLError;
                            return false;
                        }
                    }
                    if (table->ttl_desc().ttl_type() == rtidb::api::kAbsoluteTime) {
                        it->second->set_abs_ttl(table->ttl_desc().abs_ttl());
                    } else if (table->ttl_desc().ttl_type() == rtidb::api::kLatestTime) {
                        it->second->set_lat_ttl(table->ttl_desc().lat_ttl());
                    } else {
                        it->second->set_abs_ttl(table->ttl_desc().abs_ttl());
                        it->second->set_lat_ttl(table->ttl_desc().lat_ttl());
                    }
                } else {
                    no_ts_cnt++;
                }
                break;
            }

            default: {
                status->msg =
                    "can not support " +
                    fesql::node::NameOfSQLNodeType(column_desc->GetType()) +
                    " when CREATE TABLE";
                status->code = fesql::common::kSQLError;
                return false;
            }
        }
    }
    if (no_ts_cnt > 0 && no_ts_cnt != table->column_key_size()) {
        status->msg = "CREATE common: need to set ts col";
        status->code = fesql::common::kSQLError;
        return false;
    }
    if (!distribution_list.empty()) {
        if (replica_num != (int32_t)distribution_list.size()) {
            status->msg = "CREATE common: "
                "replica_num should equal to partition meta size";
            status->code = fesql::common::kSQLError;
            return false;
        }
        ::rtidb::nameserver::TablePartition* table_partition =
            table->add_table_partition();
        table_partition->set_pid(0);
        std::vector<std::string> ep_vec;
        for (auto partition_meta : distribution_list) {
            switch (partition_meta->GetType()) {
                case fesql::node::kPartitionMeta: {
                    fesql::node::PartitionMetaNode* p_meta_node =
                        (fesql::node::PartitionMetaNode*)partition_meta;
                    const std::string& ep = p_meta_node->GetEndpoint();
                    if (std::find(ep_vec.begin(), ep_vec.end(), ep) !=
                        ep_vec.end()) {
                        status->msg = "CREATE common: "
                            "partition meta endpoint duplicate";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    ep_vec.push_back(ep);
                    ::rtidb::nameserver::PartitionMeta* meta =
                        table_partition->add_partition_meta();
                    meta->set_endpoint(ep);
                    if (p_meta_node->GetRoleType() == fesql::node::kLeader) {
                        meta->set_is_leader(true);
                    } else if (p_meta_node->GetRoleType() ==
                            fesql::node::kFollower) {
                        meta->set_is_leader(false);
                    } else {
                        status->msg = "CREATE common: role_type " +
                            fesql::node::RoleTypeName(p_meta_node->GetRoleType()) +
                            " not support";
                        status->code = fesql::common::kSQLError;
                        return false;
                    }
                    break;
                }
                default: {
                    status->msg = "can not support " +
                        fesql::node::NameOfSQLNodeType(partition_meta->GetType()) +
                        " when CREATE TABLE 2";
                    status->code = fesql::common::kSQLError;
                    return false;
                }
            }
        }
    }
    return true;
}

bool NsClient::DropProcedure(const std::string& db_name,
        const std::string& sp_name, std::string& msg) {
    ::rtidb::api::DropProcedureRequest request;
    ::rtidb::nameserver::GeneralResponse response;
    request.set_db_name(db_name);
    request.set_sp_name(sp_name);
    bool ok =
        client_.SendRequest(&::rtidb::nameserver::NameServer_Stub::DropProcedure,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg = response.msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

}  // namespace client
}  // namespace rtidb
