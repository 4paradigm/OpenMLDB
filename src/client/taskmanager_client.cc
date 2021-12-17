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

::openmldb::base::Status TaskManagerClient::ShowJobs(const bool onlyUnfinished) {
    ::openmldb::taskmanager::ShowJobsRequest request;
    ::openmldb::taskmanager::ShowJobsResponse response;

    request.set_unfinished(onlyUnfinished);

    bool ok = client_.SendRequest(&::openmldb::taskmanager::TaskManagerServer_Stub::ShowJobs, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (!ok) {
        return ::openmldb::base::Status(-1, "Fail to request TaskManager server");
    }
    return ::openmldb::base::Status(response.code(), response.msg());
}

}  // namespace client
}  // namespace openmldb
