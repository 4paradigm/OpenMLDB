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

#ifndef SRC_CLIENT_TASKMANAGER_CLIENT_H_
#define SRC_CLIENT_TASKMANAGER_CLIENT_H_

#include <map>
#include <memory>
#include <string>
#include <vector>

#include "base/status.h"
#include "client/client.h"
#include "proto/common.pb.h"
#include "proto/taskmanager.pb.h"
#include "rpc/rpc_client.h"

namespace openmldb::client {

// We set the job timeout(rpc request timeout) in every function, cuz the taskmanager client may be recreated in
// DBSDK(SQLClusterRouter class member), but the timeout setting(session variables) is held by SQLClusterRouter.
class TaskManagerClient : public Client {
 public:
    TaskManagerClient(const std::string& endpoint, const std::string& real_endpoint, bool use_sleep_policy)
        : Client(endpoint, real_endpoint),
          client_(real_endpoint.empty() ? endpoint : real_endpoint, use_sleep_policy) {}

    TaskManagerClient(const std::string& endpoint, const std::string& real_endpoint)
        : Client(endpoint, real_endpoint), client_(real_endpoint.empty() ? endpoint : real_endpoint, true) {}

    ~TaskManagerClient() override = default;

    int Init() override { return client_.Init(); }

    ::openmldb::base::Status ShowJobs(bool only_unfinished, int job_timeout,
                                      std::vector<::openmldb::taskmanager::JobInfo>* job_infos);

    ::openmldb::base::Status ShowJob(int id, int job_timeout, ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status StopJob(int id, int job_timeout, ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status RunBatchSql(const std::string& sql, const std::map<std::string, std::string>& config,
                                         const std::string& default_db, int job_timeout,
                                         std::string* output);

    ::openmldb::base::Status RunBatchAndShow(const std::string& sql, const std::map<std::string, std::string>& config,
                                             const std::string& default_db, bool sync_job, int job_timeout,
                                             ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status ImportOnlineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                              const std::string& default_db, bool sync_job, int job_timeout,
                                              ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status ImportOfflineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                               const std::string& default_db, bool sync_job, int job_timeout,
                                               ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status ExportOfflineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                               const std::string& default_db, bool sync_job, int job_timeout,
                                               ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status InsertOfflineData(const std::string& sql, const std::map<std::string, std::string>& config,
                                               const std::string& default_db, bool sync_job, int job_timeout,
                                               ::openmldb::taskmanager::JobInfo* job_info);

    ::openmldb::base::Status DropOfflineTable(const std::string& db, const std::string& table, int job_timeout);

    ::openmldb::base::Status CreateFunction(const std::shared_ptr<::openmldb::common::ExternalFun>& fun,
                                            int job_timeout);

    ::openmldb::base::Status DropFunction(const std::string& name, int job_timeout);

    std::string GetJobLog(int id, int job_timeout, ::openmldb::base::Status* status);

 private:
    ::openmldb::RpcClient<::openmldb::taskmanager::TaskManagerServer_Stub> client_;
};

}  // namespace openmldb::client

#endif  // SRC_CLIENT_TASKMANAGER_CLIENT_H_
