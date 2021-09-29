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

#ifndef SRC_NAMESERVER_CLUSTER_INFO_H_
#define SRC_NAMESERVER_CLUSTER_INFO_H_

#include <map>
#include <memory>
#include <string>
#include <vector>
#include "client/ns_client.h"
#include "proto/name_server.pb.h"
#include "proto/tablet.pb.h"
#include "zk/zk_client.h"

namespace openmldb {
namespace nameserver {

class ClusterInfo {
 public:
    explicit ClusterInfo(const ::openmldb::nameserver::ClusterAddress& cdp);

    void CheckZkClient();

    void UpdateNSClient(const std::vector<std::string>& children);

    int Init(std::string& msg);  // NOLINT

    bool CreateTableRemote(const ::openmldb::api::TaskInfo& task_info,
                           const ::openmldb::nameserver::TableInfo& table_info,
                           const ::openmldb::nameserver::ZoneInfo& zone_info);

    bool DropTableRemote(const ::openmldb::api::TaskInfo& task_info, const std::string& name, const std::string& db,
                         const ::openmldb::nameserver::ZoneInfo& zone_info);

    bool AddReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
                               std::string& msg);  // NOLINT

    bool RemoveReplicaClusterByNs(const std::string& alias, const std::string& zone_name, const uint64_t term,
                                  int& code,          // NOLINT
                                  std::string& msg);  // NOLINT

    bool UpdateRemoteRealEpMap();

    std::shared_ptr<::openmldb::client::NsClient> client_;
    std::map<std::string, std::map<std::string, std::vector<TablePartition>>> last_status;
    ::openmldb::nameserver::ClusterAddress cluster_add_;
    uint64_t ctime_;
    std::atomic<ClusterStatus> state_;
    std::shared_ptr<std::map<std::string, std::string>> remote_real_ep_map_;

 private:
    std::shared_ptr<::openmldb::zk::ZkClient> zk_client_;
    uint64_t session_term_;
    // todo :: add statsus variable show replicas status
};

}  // namespace nameserver
}  // namespace openmldb
#endif  // SRC_NAMESERVER_CLUSTER_INFO_H_
