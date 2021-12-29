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

#include "client/taskmanager_client.h"

DECLARE_int32(request_timeout_ms);

namespace openmldb {
namespace client {

::openmldb::base::Status TaskManagerClient::ShowJobs(const bool only_unfinished,
                                                     std::vector<::openmldb::taskmanager::JobInfo>& job_infos) {
    ::openmldb::taskmanager::ShowJobsRequest request;
    ::openmldb::taskmanager::ShowJobsResponse response;

    request.set_unfinished(only_unfinished);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::ShowJobs, &request, &response,
                                  FLAGS_request_timeout_ms, 1);

    if (ok) {
        if (response.code() == 0) {
            for (int32_t i = 0; i < response.jobs_size(); i++) {
                ::openmldb::taskmanager::JobInfo job_info;
                job_info.CopyFrom(response.jobs(i));
                job_infos.push_back(job_info);
            }
        }
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}

::openmldb::base::Status TaskManagerClient::ShowJob(const int id, ::openmldb::taskmanager::JobInfo& job_info) {
    ::openmldb::taskmanager::ShowJobRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_id(id);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::ShowJob, &request, &response,
                                  FLAGS_request_timeout_ms, 1);

    if (ok) {
        if(response.code() == 0) {
            if (response.has_job()) {
                job_info.CopyFrom(response.job());
            }
        }
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}

::openmldb::base::Status TaskManagerClient::StopJob(const int id, ::openmldb::taskmanager::JobInfo& job_info) {
    ::openmldb::taskmanager::StopJobRequest request;
    ::openmldb::taskmanager::StopJobResponse response;

    request.set_id(id);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::StopJob, &request, &response,
                                  FLAGS_request_timeout_ms, 1);

    if (ok) {
        if(response.code() == 0) {
            if (response.has_job()) {
                job_info.CopyFrom(response.job());
            }
        }
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}

::openmldb::base::Status TaskManagerClient::RunBatchAndShow(const std::string& sql,
                                                            const std::map<std::string, std::string>& config,
                                                            const std::string& default_db,
                                                            ::openmldb::taskmanager::JobInfo& job_info) {
    ::openmldb::taskmanager::RunBatchAndShowRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);
    // TODO: Set map of config

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::RunBatchAndShow, &request,
                                  &response, FLAGS_request_timeout_ms, 1);

    if (ok) {
        if(response.code() == 0) {
            if (response.has_job()) {
                job_info.CopyFrom(response.job());
            }
        }
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}

::openmldb::base::Status TaskManagerClient::ImportOnlineData(const std::string& sql,
                                                             const std::map<std::string, std::string>& config,
                                                             const std::string& default_db,
                                                             ::openmldb::taskmanager::JobInfo& job_info) {
    ::openmldb::taskmanager::ImportOnlineDataRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::ImportOnlineData, &request,
                                  &response, FLAGS_request_timeout_ms, 1);

    if (ok) {
        if(response.code() == 0) {
            if (response.has_job()) {
                job_info.CopyFrom(response.job());
            }
        }
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}

::openmldb::base::Status TaskManagerClient::ImportOfflineData(const std::string& sql,
                                                              const std::map<std::string, std::string>& config,
                                                              const std::string& default_db,
                                                              ::openmldb::taskmanager::JobInfo& job_info) {
    ::openmldb::taskmanager::ImportOfflineDataRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::ImportOfflineData, &request,
                                  &response, FLAGS_request_timeout_ms, 1);

    if (ok) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info.CopyFrom(response.job());
            }
        }
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}

::openmldb::base::Status TaskManagerClient::DropOfflineTable(const std::string& db, const std::string& table) {
    ::openmldb::taskmanager::DropOfflineTableRequest request;
    ::openmldb::taskmanager::DropOfflineTableResponse response;

    request.set_db(db);
    request.set_table(table);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::DropOfflineTable, &request,
                                  &response, FLAGS_request_timeout_ms, 1);

    if (ok) {
        return ::openmldb::base::Status(response.code(), response.msg());
    } else {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
}


}  // namespace client
}  // namespace openmldb
