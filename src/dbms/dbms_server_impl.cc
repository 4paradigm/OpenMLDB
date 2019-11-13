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

DBMSServerImpl::DBMSServerImpl() : db_(nullptr) {}
DBMSServerImpl::~DBMSServerImpl() {}

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
    LOG(INFO) << "create group " << request->name() << " done";
}

void DBMSServerImpl::AddTable(RpcController* ctr,
                              const AddTableRequest* request,
                              AddTableResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (nullptr == db_) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kNoDatabase);
        status->set_msg("No database selected");
        LOG(WARNING) << "create table failed for out of database";
        return;
    }
    if (request->table().name().empty()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kBadRequest);
        status->set_msg("table name is empty");
        LOG(WARNING) << "create table failed for table name is empty";
        return;
    }
    std::lock_guard<std::mutex> lock(mu_);

    if (tables_.find(request->table().name()) != tables_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kTableExists);
        status->set_msg("table already exists");
        LOG(WARNING) << "create table failed for table exists";
        return;
    }
    ::fesql::type::TableDef* table = db_->add_tables();
    // TODO(chenjing):add create time
    table->set_name(request->table().name());
    for (auto column : request->table().columns()) {
        *(table->add_columns()) = column;
    }
    for (auto index : request->table().indexes()) {
        *(table->add_indexes()) = index;
    }
    tables_[table->name()] = table;
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
    LOG(INFO) << "create table " << request->table().name() << " done";
}
void DBMSServerImpl::GetSchema(RpcController* ctr,
                                const GetSchemaRequest* request,
                                GetSchemaResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    std::lock_guard<std::mutex> lock(mu_);
    if (tables_.find(request->name()) == tables_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kTableExists);
        status->set_msg("table doesn't exist");
        LOG(WARNING) << "show table failed for table doesn't exist";
        return;
    }

    *(response->mutable_table()) = *(tables_.at(request->name()));
    ::fesql::common::Status* status = response->mutable_status();
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
    LOG(INFO) << "show table " << request->name() << " done";
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
void DBMSServerImpl::EnterDatabase(RpcController* ctr,
                                   const EnterDatabaseRequest* request,
                                   EnterDatabaseResponse* response,
                                   Closure* done) {
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
    if (nullptr != db_ && 0 == db_->name().compare(request->name())) {
        status->set_code(::fesql::common::kOk);
        status->set_msg("ok");
        return;
    }

    std::lock_guard<std::mutex> lock(mu_);
    Databases::iterator it = databases_.find(request->name());
    if (it == databases_.end()) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kNameExists);
        status->set_msg("database doesn't exist");
        LOG(WARNING) << "enter database failed for database doesn't exist";
        return;
    }

    db_ = &(databases_[request->name()]);
    InitTable(db_, tables_);
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
    LOG(INFO) << "create database " << request->name() << " done";
}

void DBMSServerImpl::GetDatabases(RpcController* controller,
                                   const GetItemsRequest* request,
                                   GetItemsResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    // TODO(chenjing): case intensive
    ::fesql::common::Status* status = response->mutable_status();

    std::lock_guard<std::mutex> lock(mu_);
    for (auto entry : databases_) {
        response->add_items(entry.first);
    }
    status->set_code(::fesql::common::kOk);
    status->set_msg("ok");
}

void DBMSServerImpl::GetTables(RpcController* controller,
                                const GetItemsRequest* request,
                                GetItemsResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    if (nullptr == db_) {
        ::fesql::common::Status* status = response->mutable_status();
        status->set_code(::fesql::common::kNoDatabase);
        status->set_msg("No database selected");
        LOG(WARNING) << "show tables failed for out of database";
        return;
    }
    ::fesql::common::Status* status = response->mutable_status();
    std::lock_guard<std::mutex> lock(mu_);
    for (auto entry: tables_) {
        response->add_items(entry.first);
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
}  // namespace dbms
}  // namespace fesql
