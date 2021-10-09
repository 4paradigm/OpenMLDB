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

#include "sdk/cluster_sdk.h"

#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

#include <algorithm>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "base/hash.h"
#include "base/strings.h"
#include "boost/bind.hpp"
#include "glog/logging.h"

namespace openmldb {
namespace sdk {

ClusterSDK::ClusterSDK(const ClusterOptions& options)
    : cluster_version_(0),
      options_(options),
      nodes_root_path_(options.zk_path + "/nodes"),
      table_root_path_(options.zk_path + "/table/db_table_data"),
      notify_path_(options.zk_path + "/table/notify"),
      zk_client_(NULL),
      mu_(),
      client_manager_(std::make_shared<::openmldb::catalog::ClientManager>()),
      table_to_tablets_(),
      catalog_(new ::openmldb::catalog::SDKCatalog(client_manager_)),
      pool_(1),
      session_id_(0),
      rand_(0xdeadbeef),
      sp_root_path_(options.zk_path + "/store_procedure/db_sp_data"),
      engine_(NULL) {}

ClusterSDK::~ClusterSDK() {
    pool_.Stop(false);
    if (zk_client_ != NULL) {
        zk_client_->CloseZK();
        delete zk_client_;
        zk_client_ = NULL;
    }
    delete engine_;
}

void ClusterSDK::CheckZk() {
    if (session_id_ == 0) {
        WatchNotify();
    } else if (session_id_ != zk_client_->GetSessionTerm()) {
        LOG(WARNING) << "session changed rewatch notity";
        WatchNotify();
    }
    pool_.DelayTask(2000, boost::bind(&ClusterSDK::CheckZk, this));
}

bool ClusterSDK::Init() {
    zk_client_ = new ::openmldb::zk::ZkClient(options_.zk_cluster, "", options_.session_timeout, "", options_.zk_path);
    bool ok = zk_client_->Init();
    if (!ok) {
        LOG(WARNING) << "fail to init zk client with zk cluster " << options_.zk_cluster << " , zk path "
                     << options_.zk_path << " and session timeout " << options_.session_timeout;
        return false;
    }
    LOG(INFO) << "init zk client with zk cluster " << options_.zk_cluster << " , zk path " << options_.zk_path
              << ",session timeout " << options_.session_timeout << " and session id " << zk_client_->GetSessionTerm();

    ::hybridse::vm::EngineOptions eopt;
    eopt.set_compile_only(true);
    eopt.set_plan_only(true);
    engine_ = new ::hybridse::vm::Engine(catalog_, eopt);

    ok = InitCatalog();
    if (!ok) return false;
    CheckZk();
    return true;
}

bool ClusterSDK::Refresh() { return InitCatalog(); }

void ClusterSDK::WatchNotify() {
    LOG(INFO) << "start to watch table notify";
    session_id_ = zk_client_->GetSessionTerm();
    zk_client_->CancelWatchItem(notify_path_);
    zk_client_->WatchItem(notify_path_, boost::bind(&ClusterSDK::Refresh, this));
}

bool ClusterSDK::CreateNsClient() {
    std::string ns_node = options_.zk_path + "/leader";
    std::vector<std::string> children;
    if (!zk_client_->GetChildren(ns_node, children) || children.empty()) {
        LOG(WARNING) << "no nameserver exists";
        return false;
    }
    std::sort(children.begin(), children.end());
    std::string real_path = ns_node + "/" + children[0];
    std::string endpoint;
    if (!zk_client_->GetNodeValue(real_path, endpoint)) {
        LOG(WARNING) << "fail to get zk value with path " << real_path;
        return false;
    }
    DLOG(INFO) << "leader path " << real_path << " with value " << endpoint;

    std::string real_endpoint;
    if (!GetRealEndpoint(endpoint, &real_endpoint)) {
        return false;
    }
    auto ns_client = std::make_shared<::openmldb::client::NsClient>(endpoint, real_endpoint);
    int ret = ns_client->Init();
    if (ret != 0) {
        LOG(WARNING) << "fail to init ns client with endpoint " << endpoint;
        return false;
    } else {
        LOG(INFO) << "init ns client with endpoint " << endpoint << " done";
        std::atomic_store_explicit(&ns_client_, ns_client, std::memory_order_relaxed);
        return true;
    }
}

bool ClusterSDK::RefreshCatalog(const std::vector<std::string>& table_datas, const std::vector<std::string>& sp_datas) {
    std::vector<::openmldb::nameserver::TableInfo> tables;
    std::map<std::string, std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>>> mapping;
    auto new_catalog = std::make_shared<::openmldb::catalog::SDKCatalog>(client_manager_);
    for (uint32_t i = 0; i < table_datas.size(); i++) {
        if (table_datas[i].empty()) continue;
        std::string value;
        bool ok = zk_client_->GetNodeValue(table_root_path_ + "/" + table_datas[i], value);
        if (!ok) {
            LOG(WARNING) << "fail to get table data";
            continue;
        }
        std::shared_ptr<::openmldb::nameserver::TableInfo> table_info(new ::openmldb::nameserver::TableInfo());
        ok = table_info->ParseFromString(value);
        if (!ok) {
            LOG(WARNING) << "fail to parse table proto with " << value;
            continue;
        }
        DLOG(INFO) << "parse table " << table_info->name() << " ok";
        if (table_info->format_version() != 1) {
            continue;
        }
        tables.push_back(*(table_info));
        auto it = mapping.find(table_info->db());
        if (it == mapping.end()) {
            std::map<std::string, std::shared_ptr<::openmldb::nameserver::TableInfo>> table_in_db;
            table_in_db.insert(std::make_pair(table_info->name(), table_info));
            mapping.insert(std::make_pair(table_info->db(), table_in_db));
        } else {
            it->second.insert(std::make_pair(table_info->name(), table_info));
        }
        DLOG(INFO) << "load table info with name " << table_info->name() << " in db " << table_info->db();
    }

    Procedures db_sp_map;
    for (const auto& node : sp_datas) {
        if (node.empty()) continue;
        std::string value;
        bool ok = zk_client_->GetNodeValue(sp_root_path_ + "/" + node, value);
        if (!ok) {
            LOG(WARNING) << "fail to get procedure data. node: " << node;
            continue;
        }
        std::string uncompressed;
        ::snappy::Uncompress(value.c_str(), value.length(), &uncompressed);
        ::openmldb::api::ProcedureInfo sp_info_pb;
        ok = sp_info_pb.ParseFromString(uncompressed);
        if (!ok) {
            LOG(WARNING) << "fail to parse procedure proto. node: " << node << " value: " << value;
            continue;
        }
        DLOG(INFO) << "parse procedure " << sp_info_pb.sp_name() << " ok";
        // conver to ProcedureInfoImpl
        auto sp_info = openmldb::catalog::SchemaAdapter::ConvertProcedureInfo(sp_info_pb);
        if (!sp_info) {
            LOG(WARNING) << "convert procedure info failed, sp_name: " << sp_info_pb.sp_name()
                         << " db: " << sp_info_pb.db_name();
            continue;
        }
        auto it = db_sp_map.find(sp_info->GetDbName());
        if (it == db_sp_map.end()) {
            std::map<std::string, std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_in_db = {
                {sp_info->GetSpName(), sp_info}};
            db_sp_map.insert(std::make_pair(sp_info->GetDbName(), sp_in_db));
        } else {
            it->second.insert(std::make_pair(sp_info->GetSpName(), sp_info));
        }
        DLOG(INFO) << "load procedure info with sp name " << sp_info->GetSpName() << " in db " << sp_info->GetDbName();
    }
    if (!new_catalog->Init(tables, db_sp_map)) {
        LOG(WARNING) << "fail to init catalog";
        return false;
    }
    {
        std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
        table_to_tablets_ = mapping;
        catalog_ = new_catalog;
    }
    engine_->UpdateCatalog(new_catalog);
    return true;
}

bool ClusterSDK::InitTabletClient() {
    std::vector<std::string> tablets;
    bool ok = zk_client_->GetNodes(tablets);
    if (!ok) {
        LOG(WARNING) << "fail to get tablet";
        return false;
    }
    std::map<std::string, std::string> real_ep_map;
    for (const auto& endpoint : tablets) {
        std::string cur_endpoint = ::openmldb::base::ExtractEndpoint(endpoint);
        std::string real_endpoint;
        if (!GetRealEndpoint(cur_endpoint, &real_endpoint)) {
            return false;
        }
        real_ep_map.emplace(cur_endpoint, real_endpoint);
    }
    client_manager_->UpdateClient(real_ep_map);
    return true;
}

bool ClusterSDK::InitCatalog() {
    std::vector<std::string> table_datas;
    if (zk_client_->IsExistNode(table_root_path_) == 0) {
        bool ok = zk_client_->GetChildren(table_root_path_, table_datas);
        if (!ok) {
            LOG(WARNING) << "fail to get table list with path " << table_root_path_;
            return false;
        }
    } else {
        LOG(INFO) << "no tables in db";
    }
    std::vector<std::string> sp_datas;
    if (zk_client_->IsExistNode(sp_root_path_) == 0) {
        bool ok = zk_client_->GetChildren(sp_root_path_, sp_datas);
        if (!ok) {
            LOG(WARNING) << "fail to get procedure list with path " << sp_root_path_;
            return false;
        }
    } else {
        DLOG(INFO) << "no procedures in db";
    }
    bool ok = InitTabletClient();
    if (!ok) return false;
    return RefreshCatalog(table_datas, sp_datas);
}

uint32_t ClusterSDK::GetTableId(const std::string& db, const std::string& tname) {
    auto table_handler = GetCatalog()->GetTable(db, tname);
    ::openmldb::catalog::SDKTableHandler* sdk_table_handler =
        dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
    return sdk_table_handler->GetTid();
}

std::shared_ptr<::openmldb::nameserver::TableInfo> ClusterSDK::GetTableInfo(const std::string& db,
                                                                            const std::string& tname) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto it = table_to_tablets_.find(db);
    if (it == table_to_tablets_.end()) {
        return std::shared_ptr<::openmldb::nameserver::TableInfo>();
    }
    auto sit = it->second.find(tname);
    if (sit == it->second.end()) {
        return std::shared_ptr<::openmldb::nameserver::TableInfo>();
    }
    auto table_info = sit->second;
    return table_info;
}

std::vector<std::shared_ptr<::openmldb::nameserver::TableInfo>> ClusterSDK::GetTables(const std::string& db) {
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    std::vector<std::shared_ptr<::openmldb::nameserver::TableInfo>> tables;
    auto it = table_to_tablets_.find(db);
    if (it == table_to_tablets_.end()) {
        return tables;
    }
    auto iit = it->second.begin();
    for (; iit != it->second.end(); ++iit) {
        tables.push_back(iit->second);
    }
    return tables;
}

bool ClusterSDK::GetRealEndpoint(const std::string& endpoint, std::string* real_endpoint) {
    if (real_endpoint == nullptr) {
        return false;
    }
    std::string sdk_path = options_.zk_path + "/map/sdkendpoints/" + endpoint;
    if (zk_client_->IsExistNode(sdk_path) == 0) {
        if (!zk_client_->GetNodeValue(sdk_path, *real_endpoint)) {
            DLOG(WARNING) << "get zk failed! : sdk_path: " << sdk_path;
            return false;
        }
    }
    if (real_endpoint->empty()) {
        std::string sname_path = options_.zk_path + "/map/names/" + endpoint;
        if (zk_client_->IsExistNode(sname_path) == 0) {
            if (!zk_client_->GetNodeValue(sname_path, *real_endpoint)) {
                DLOG(WARNING) << "get zk failed! : sname_path: " << sname_path;
                return false;
            }
        }
    }
    return true;
}

std::shared_ptr<::openmldb::catalog::TabletAccessor> ClusterSDK::GetTablet() { return GetCatalog()->GetTablet(); }

std::shared_ptr<::openmldb::catalog::TabletAccessor> ClusterSDK::GetTablet(const std::string& db,
                                                                           const std::string& name) {
    auto table_handler = GetCatalog()->GetTable(db, name);
    if (table_handler) {
        ::openmldb::catalog::SDKTableHandler* sdk_table_handler =
            dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
        if (sdk_table_handler) {
            uint32_t pid_num = sdk_table_handler->GetPartitionNum();
            uint32_t pid = 0;
            if (pid_num > 0) {
                pid = rand_.Uniform(pid_num);
            }
            return sdk_table_handler->GetTablet(pid);
        }
    }
    return std::shared_ptr<::openmldb::catalog::TabletAccessor>();
}

bool ClusterSDK::GetTablet(const std::string& db, const std::string& name,
                           std::vector<std::shared_ptr<::openmldb::catalog::TabletAccessor>>* tablets) {
    auto table_handler = GetCatalog()->GetTable(db, name);
    if (table_handler) {
        ::openmldb::catalog::SDKTableHandler* sdk_table_handler =
            dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
        if (sdk_table_handler) {
            return sdk_table_handler->GetTablet(tablets);
        }
    }
    return false;
}

std::shared_ptr<::openmldb::catalog::TabletAccessor> ClusterSDK::GetTablet(const std::string& db,
                                                                           const std::string& name, uint32_t pid) {
    auto table_handler = GetCatalog()->GetTable(db, name);
    if (table_handler) {
        ::openmldb::catalog::SDKTableHandler* sdk_table_handler =
            dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
        if (sdk_table_handler) {
            return sdk_table_handler->GetTablet(pid);
        }
    }
    return std::shared_ptr<::openmldb::catalog::TabletAccessor>();
}

std::shared_ptr<::openmldb::catalog::TabletAccessor> ClusterSDK::GetTablet(const std::string& db,
                                                                           const std::string& name,
                                                                           const std::string& pk) {
    auto table_handler = GetCatalog()->GetTable(db, name);
    if (table_handler) {
        auto sdk_table_handler = dynamic_cast<::openmldb::catalog::SDKTableHandler*>(table_handler.get());
        if (sdk_table_handler) {
            uint32_t pid_num = sdk_table_handler->GetPartitionNum();
            uint32_t pid = 0;
            if (pid_num > 0) {
                pid = ::openmldb::base::hash64(pk) % pid_num;
            }
            return sdk_table_handler->GetTablet(pid);
        }
    }
    return std::shared_ptr<::openmldb::catalog::TabletAccessor>();
}

std::shared_ptr<hybridse::sdk::ProcedureInfo> ClusterSDK::GetProcedureInfo(const std::string& db,
                                                                           const std::string& sp_name,
                                                                           std::string* msg) {
    if (msg == nullptr) {
        *msg = "null ptr";
        return nullptr;
    }
    if (db.empty() || sp_name.empty()) {
        *msg = "db or sp_name is empty";
        return nullptr;
    } else {
        std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
        auto sp = catalog_->GetProcedureInfo(db, sp_name);
        if (!sp) {
            *msg = sp_name + " does not exist in " + db;
            return nullptr;
        }
        return sp;
    }
}

std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> ClusterSDK::GetProcedureInfo(std::string* msg) {
    std::vector<std::shared_ptr<hybridse::sdk::ProcedureInfo>> sp_infos;
    if (msg == nullptr) {
        *msg = "null ptr";
        return std::move(sp_infos);
    }
    std::lock_guard<::openmldb::base::SpinMutex> lock(mu_);
    auto& db_sp_map = catalog_->GetProcedures();
    for (const auto& db_kv : db_sp_map) {
        for (const auto& sp_kv : db_kv.second) {
            sp_infos.push_back(sp_kv.second);
        }
    }
    if (sp_infos.empty()) {
        *msg = "procedure set is empty";
        return std::move(sp_infos);
    }
    return std::move(sp_infos);
}

}  // namespace sdk
}  // namespace openmldb
