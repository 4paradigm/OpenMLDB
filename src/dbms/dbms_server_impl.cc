/*
 * dbms_server_impl.cc
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

#include "dbms/dbms_server_impl.h"
#include "absl/time/time.h"
#include "brpc/server.h"

namespace fesql {
namespace dbms {

DBMSServerImpl::DBMSServerImpl()
    : tablet_sdk(nullptr), tid_(0), tablets_() {}
DBMSServerImpl::~DBMSServerImpl() { delete tablet_sdk; }

void DBMSServerImpl::AddGroup(RpcController* ctr,
                              const AddGroupRequest* request,
                              AddGroupResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->name().empty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kBadRequest);
        status->set_msg("group name is empty");
        LOG(WARNING) << "create group failed for name is empty";
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    Groups::iterator it = groups_.find(request->name());
    if (it != groups_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kNameExists);
        status->set_msg("group name exists ");
        LOG(WARNING) << "create group failed for name existing";
        return;
    }

    ::fesql::type::Group& group = groups_[request->name()];
    group.set_name(request->name());
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
    DLOG(INFO) << "create group " << request->name() << " done";
}

void DBMSServerImpl::AddTable(RpcController* ctr,
                              const AddTableRequest* request,
                              AddTableResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->table().name().empty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kBadRequest);
        status->set_msg("table name is empty");
        LOG(WARNING) << "create table failed for table name is empty";
        return;
    }

    type::Database* db;
    {
        common::Status get_db_status;
        db = GetDatabase(request->db_name(), get_db_status);
        if (0 != get_db_status.code()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(get_db_status.code());
            status->set_msg(get_db_status.msg());
            return;
        }
        if (nullptr == db) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(fesql::common::kNoDatabase);
            status->set_msg("Database doesn't exist");
            return;
        }
    }

    std::lock_guard<std::mutex> lock(mu_);

    for (auto table : db->tables()) {
        if (table.name() == request->table().name()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(::fesql::common::kTableExists);
            status->set_msg("table already exists");
            LOG(WARNING) << "create table failed for table exists";
            return;
        }
    }

    if (nullptr == tablet_sdk) {
        if (tablets_.empty()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(::fesql::common::kConnError);
            status->set_msg("can't connect tablet endpoint is empty");
            LOG(WARNING) << status->msg();
            return;
        }
        tablet_sdk = new fesql::tablet::TabletInternalSDK(*(tablets_.begin()));
        if (tablet_sdk == NULL) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(::fesql::common::kConnError);
            status->set_msg(
                "Fail to connect to tablet (maybe you should check "
                "tablet_endpoint");
            LOG(WARNING) << status->msg();
            return;
        }
        if (false == tablet_sdk->Init()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(::fesql::common::kConnError);
            status->set_msg(
                "Fail to init tablet (maybe you should check tablet_endpoint");
            LOG(WARNING) << status->msg();
            return;
        }
    }

    // TODO(chenjing): 后续是否需要从tablet同步表schema数据
    {
        fesql::common::Status create_table_status;
        fesql::tablet::CreateTableRequest create_table_request;
        create_table_request.set_db(request->db_name());
        create_table_request.set_tid(tid_ + 1);
        // TODO(chenjing): pid setting
        create_table_request.add_pids(0);
        *(create_table_request.mutable_table()) = request->table();
        create_table_request.mutable_table()->set_catalog(request->db_name());
        tablet_sdk->CreateTable(&create_table_request, create_table_status);
        if (0 != create_table_status.code()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(create_table_status.code());
            status->set_msg(create_table_status.msg());
            return;
        }
    }

    ::fesql::type::TableDef* table = db->add_tables();
    // TODO(chenjing): add create time
    table->set_name(request->table().name());
    for (auto column : request->table().columns()) {
        *(table->add_columns()) = column;
    }
    for (auto index : request->table().indexes()) {
        *(table->add_indexes()) = index;
    }
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
    tid_ += 1;
    DLOG(INFO) << "create table " << request->table().name() << " done";
}
void DBMSServerImpl::GetSchema(RpcController* ctr,
                               const GetSchemaRequest* request,
                               GetSchemaResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->name().empty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kBadRequest);
        status->set_msg("table name is empty");
        LOG(WARNING) << "create table failed for table name is empty";
        return;
    }

    type::Database* db;
    {
        common::Status get_db_status;
        db = GetDatabase(request->db_name(), get_db_status);
        if (0 != get_db_status.code()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(get_db_status.code());
            status->set_msg(get_db_status.msg());
            return;
        }
        if (nullptr == db) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(fesql::common::kNoDatabase);
            status->set_msg("Database doesn't exist");
            return;
        }
    }
    std::lock_guard<std::mutex> lock(mu_);
    for (auto table : db->tables()) {
        if (table.name() == request->name()) {
            *(response->mutable_table()) = table;
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(::fesql::common::kOk);
            status->set_msg("ok");
            DLOG(INFO) << "show table " << request->name() << " done";
            return;
        }
    }

    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kTableExists);
    status->set_msg("table doesn't exist");
    LOG(WARNING) << "show table failed for table doesn't exist";
    return;
}
void DBMSServerImpl::AddDatabase(RpcController* ctr,
                                 const AddDatabaseRequest* request,
                                 AddDatabaseResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->name().empty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kBadRequest);
        status->set_msg("database name is empty");
        LOG(WARNING) << "create database failed for name is empty";
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    Databases::iterator it = databases_.find(request->name());
    if (it != databases_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kNameExists);
        status->set_msg("database name exists");
        LOG(WARNING) << "create database failed for name existing";
        return;
    }

    ::fesql::type::Database& database = databases_[request->name()];
    database.set_name(request->name());
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
    LOG(INFO) << "create database " << request->name() << " done";
}

void DBMSServerImpl::IsExistDatabase(RpcController* ctr,
                                     const IsExistRequest* request,
                                     IsExistResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (request->name().empty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kBadRequest);
        status->set_msg("database name is empty");
        LOG(WARNING) << "enter database failed for name is empty";
        return;
    }

    // TODO(chenjing): case intensive
    ::fesql::common::Status* status = response->mutable_status();
    std::lock_guard<std::mutex> lock(mu_);
    Databases::iterator it = databases_.find(request->name());
    if (it == databases_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kOk);
        response->set_exist(false);
        return;
    } else {
        status->set_code(::fesql::common::kOk);
        response->set_exist(true);
    }
}

void DBMSServerImpl::GetDatabases(RpcController* controller,
                                  const GetDatabasesRequest* request,
                                  GetDatabasesResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // TODO(chenjing): case intensive
    ::fesql::common::Status* status = response->mutable_status();

    std::lock_guard<std::mutex> lock(mu_);
    for (auto entry : databases_) {
        response->add_names(entry.first);
    }
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
}

void DBMSServerImpl::GetTables(RpcController* controller,
                               const GetTablesRequest* request,
                               GetTablesResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    type::Database* db;
    {
        common::Status get_db_status;
        db = GetDatabase(request->db_name(), get_db_status);
        if (0 != get_db_status.code()) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(get_db_status.code());
            status->set_msg(get_db_status.msg());
            return;
        }
        if (nullptr == db) {
            ::fesql::common::Status* status = response->mutable_status();
            status->set_code(fesql::common::kNoDatabase);
            status->set_msg("Database doesn't exist");
            return;
        }
    }
    ::fesql::common::Status* status = response->mutable_status();
    std::lock_guard<std::mutex> lock(mu_);
    for (auto table : db->tables()) {
        response->add_tables()->CopyFrom(table);
    }
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
}

void DBMSServerImpl::InitTable(type::Database* db, Tables& tables) {
    tables.clear();
    for (auto table : db->tables()) {
        tables[table.name()] = &table;
    }
}

type::Database* DBMSServerImpl::GetDatabase(const std::string db_name,
                                            common::Status& status) {
    if (db_name.empty()) {
        status.set_code(::fesql::common::kNoDatabase);
        status.set_msg("Database name is empty");
        LOG(WARNING) << "get database failed for database name is empty";
        return nullptr;
    }
    std::lock_guard<std::mutex> lock(mu_);
    Databases::iterator it = databases_.find(db_name);
    if (it == databases_.end()) {
        status.set_code(::fesql::common::kNameExists);
        status.set_msg("Database doesn't exist");
        LOG(WARNING) << "get database failed for database doesn't exist";
        return nullptr;
    }
    return &it->second;
}

void DBMSServerImpl::KeepAlive(RpcController* ctrl, 
        const KeepAliveRequest *request,
    KeepAliveResponse *response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    tablets_.insert(request->endpoint());
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
}

void DBMSServerImpl::GetTablet(RpcController* ctrl,
        const GetTabletRequest *request,
        GetTabletResponse *response,
        Closure *done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    std::set<std::string>::iterator it = tablets_.begin();
    for (; it != tablets_.end(); ++it) {
        response->add_endpoints(*it);
    }
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
}

}  // namespace dbms
}  // namespace fesql
