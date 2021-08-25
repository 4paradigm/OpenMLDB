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

#include "client/nearline_tablet_client.h"

DECLARE_int32(request_timeout_ms);
namespace openmldb {
namespace client {

bool NearLineTabletClient::CreateTable(const std::string& db_name, const std::string& table_name,
        const std::string& partition_key, const Schema& schema) {
    if (db_name.empty() || table_name.empty() || partition_key.empty() || schema.size() == 0) {
        return false;
    }
    ::openmldb::nltablet::CreateTableRequest request;
    ::openmldb::nltablet::CreateTableResponse response;
    request.set_db_name(db_name);
    request.set_table_name(table_name);
    request.set_partition_key(partition_key);
    for (int i = 0; i < schema.size(); i++) {
        request.add_column_desc()->CopyFrom(schema.Get(i));
    }
    bool ok = client_.SendRequest(&::openmldb::nltablet::NLTabletServer_Stub::CreateTable, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

}  // namespace client
}  // namespace openmldb
