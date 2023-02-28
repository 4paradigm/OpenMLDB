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

#include <map>
#include <vector>

namespace openmldb::client {

::openmldb::base::Status TaskManagerClient::ShowJobs(bool only_unfinished, int job_timeout,
                                                     std::vector<::openmldb::taskmanager::JobInfo>* job_infos) {
    ::openmldb::taskmanager::ShowJobsRequest request;
    ::openmldb::taskmanager::ShowJobsResponse response;

    request.set_unfinished(only_unfinished);

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::ShowJobs, &request, &response,
                                    job_timeout, 1);
    // if ok, cntl won't failed, only check repsonse
    if (st.OK()) {
        if (response.code() == 0) {
            for (int32_t i = 0; i < response.jobs_size(); i++) {
                ::openmldb::taskmanager::JobInfo job_info;
                job_info.CopyFrom(response.jobs(i));
                job_infos->push_back(job_info);
            }
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::ShowJob(const int id, int job_timeout,
                                                    ::openmldb::taskmanager::JobInfo* job_info) {
    ::openmldb::taskmanager::ShowJobRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_id(id);

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::ShowJob, &request, &response,
                                    job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info->CopyFrom(response.job());
            }
        }
        return {response.code(), response.msg()};
    } else {
        return {base::ReturnCode::kServerConnError, "RPC request (to TaskManager) failed(stub is null)"};
    }
}

::openmldb::base::Status TaskManagerClient::StopJob(const int id, int job_timeout,
                                                    ::openmldb::taskmanager::JobInfo* job_info) {
    ::openmldb::taskmanager::StopJobRequest request;
    ::openmldb::taskmanager::StopJobResponse response;

    request.set_id(id);

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::StopJob, &request, &response,
                                    job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info->CopyFrom(response.job());
            }
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::RunBatchSql(const std::string& sql,
                                                        const std::map<std::string, std::string>& config,
                                                        const std::string& default_db, int job_timeout,
                                                        std::string* output) {
    ::openmldb::taskmanager::RunBatchSqlRequest request;
    ::openmldb::taskmanager::RunBatchSqlResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);
    for (const auto& it : config) {
        (*request.mutable_conf())[it.first] = it.second;
    }

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::RunBatchSql, &request, &response,
                                    job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            *output = response.output();
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::RunBatchAndShow(const std::string& sql,
                                                            const std::map<std::string, std::string>& config,
                                                            const std::string& default_db, bool sync_job,
                                                            int job_timeout,
                                                            ::openmldb::taskmanager::JobInfo* job_info) {
    ::openmldb::taskmanager::RunBatchAndShowRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);
    request.set_sync_job(sync_job);
    for (const auto& it : config) {
        (*request.mutable_conf())[it.first] = it.second;
    }

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::RunBatchAndShow, &request,
                                    &response, job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info->CopyFrom(response.job());
            }
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::ImportOnlineData(const std::string& sql,
                                                             const std::map<std::string, std::string>& config,
                                                             const std::string& default_db, bool sync_job,
                                                             int job_timeout,
                                                             ::openmldb::taskmanager::JobInfo* job_info) {
    ::openmldb::taskmanager::ImportOnlineDataRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);
    request.set_sync_job(sync_job);
    for (const auto& it : config) {
        (*request.mutable_conf())[it.first] = it.second;
    }

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::ImportOnlineData, &request,
                                    &response, job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info->CopyFrom(response.job());
            }
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::ImportOfflineData(const std::string& sql,
                                                              const std::map<std::string, std::string>& config,
                                                              const std::string& default_db, bool sync_job,
                                                              int job_timeout,
                                                              ::openmldb::taskmanager::JobInfo* job_info) {
    ::openmldb::taskmanager::ImportOfflineDataRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);
    request.set_sync_job(sync_job);
    for (const auto& it : config) {
        (*request.mutable_conf())[it.first] = it.second;
    }

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::ImportOfflineData, &request,
                                    &response, job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info->CopyFrom(response.job());
            }
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::ExportOfflineData(const std::string& sql,
                                                              const std::map<std::string, std::string>& config,
                                                              const std::string& default_db, bool sync_job,
                                                              int job_timeout,
                                                              ::openmldb::taskmanager::JobInfo* job_info) {
    ::openmldb::taskmanager::ExportOfflineDataRequest request;
    ::openmldb::taskmanager::ShowJobResponse response;

    request.set_sql(sql);
    request.set_default_db(default_db);
    request.set_sync_job(sync_job);
    for (const auto& it : config) {
        (*request.mutable_conf())[it.first] = it.second;
    }

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::ExportOfflineData, &request,
                                    &response, job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_job()) {
                job_info->CopyFrom(response.job());
            }
        }
        return {response.code(), response.msg()};
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::DropOfflineTable(const std::string& db, const std::string& table,
                                                             int job_timeout) {
    ::openmldb::taskmanager::DropOfflineTableRequest request;
    ::openmldb::taskmanager::DropOfflineTableResponse response;

    request.set_db(db);
    request.set_table(table);

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::DropOfflineTable, &request,
                                    &response, job_timeout, 1);

    if (st.OK()) {
        return {response.code(), response.msg()};
    }
    return st;
}

std::string TaskManagerClient::GetJobLog(const int id, int job_timeout, ::openmldb::base::Status* status) {
    ::openmldb::taskmanager::GetJobLogRequest request;
    ::openmldb::taskmanager::GetJobLogResponse response;

    request.set_id(id);

    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::GetJobLog, &request, &response,
                                    job_timeout, 1);

    if (st.OK()) {
        if (response.code() == 0) {
            if (response.has_log()) {
                return response.log();
            }
        }
        status->code = response.code();
        status->msg = response.msg();
        return "";
    }
    status->code = st.GetCode();
    status->msg = st.GetMsg();
    return "";
}

::openmldb::base::Status TaskManagerClient::CreateFunction(const std::shared_ptr<::openmldb::common::ExternalFun>& fun,
                                                           int job_timeout) {
    if (!fun) {
        return {base::kError, "input nullptr"};
    }
    ::openmldb::taskmanager::CreateFunctionRequest request;
    ::openmldb::taskmanager::CreateFunctionResponse response;
    request.mutable_fun()->CopyFrom(*fun);
    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::CreateFunction, &request,
                                    &response, job_timeout, 1);
    if (st.OK()) {
        if (response.code() == 0) {
            return {};
        } else {
            // base::ReturnCode
            return {response.code(), response.msg()};
        }
    }
    return st;
}

::openmldb::base::Status TaskManagerClient::DropFunction(const std::string& name, int job_timeout) {
    ::openmldb::taskmanager::DropFunctionRequest request;
    request.set_name(name);
    ::openmldb::taskmanager::DropFunctionResponse response;
    auto st = client_.SendRequestSt(&::openmldb::taskmanager::TaskManagerServer_Stub::DropFunction, &request, &response,
                                    job_timeout, 1);
    if (st.OK()) {
        if (response.code() == 0) {
            return {};
        } else {
            return {response.code(), response.msg()};
        }
    }
    return st;
}

}  // namespace openmldb::client
