/*
 * Copyright 2021 4Paradigm
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

#include "tablet/tablet_server_impl.h"

#include <map>
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_strings.h"
#include "brpc/controller.h"
#include "butil/iobuf.h"
#include "codec/fe_schema_codec.h"
#include "gflags/gflags.h"

DECLARE_string(dbms_endpoint);
DECLARE_string(toydb_endpoint);
DECLARE_int32(toydb_port);
DECLARE_bool(enable_keep_alive);
DECLARE_bool(enable_trace);

namespace hybridse {
namespace tablet {

TabletServerImpl::TabletServerImpl()
    : slock_(), engine_(), catalog_(), dbms_ch_(NULL) {}

TabletServerImpl::~TabletServerImpl() { delete dbms_ch_; }

bool TabletServerImpl::Init() {
    catalog_ = std::shared_ptr<TabletCatalog>(new TabletCatalog());
    bool ok = catalog_->Init();
    if (!ok) {
        LOG(WARNING) << "fail to init catalog ";
        return false;
    }
    engine_ = std::unique_ptr<vm::Engine>(new vm::Engine(catalog_));
    if (FLAGS_enable_keep_alive) {
        dbms_ch_ = new ::brpc::Channel();
        brpc::ChannelOptions options;
        int ret = dbms_ch_->Init(FLAGS_dbms_endpoint.c_str(), &options);
        if (ret != 0) {
            return false;
        }
        KeepAlive();
    }
    LOG(INFO) << "init tablet ok";
    return true;
}

void TabletServerImpl::KeepAlive() {
    dbms::DBMSServer_Stub stub(dbms_ch_);
    std::string endpoint = FLAGS_toydb_endpoint;
    dbms::KeepAliveRequest request;
    request.set_endpoint(endpoint);
    dbms::KeepAliveResponse response;
    brpc::Controller cntl;
    stub.KeepAlive(&cntl, &request, &response, NULL);
}

void TabletServerImpl::CreateTable(RpcController* ctrl,
                                   const CreateTableRequest* request,
                                   CreateTableResponse* response,
                                   Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ::hybridse::common::Status* status = response->mutable_status();
    if (request->pids_size() == 0) {
        status->set_code(common::kRequestError);
        status->set_msg("create table without pid");
        return;
    }
    if (request->tid() <= 0) {
        status->set_code(common::kRequestError);
        status->set_msg("create table with invalid tid " +
                        std::to_string(request->tid()));
        return;
    }

    for (int32_t i = 0; i < request->pids_size(); ++i) {
        std::shared_ptr<storage::Table> table(new storage::Table(
            request->tid(), request->pids(i), request->table()));
        bool ok = table->Init();
        if (!ok) {
            LOG(WARNING) << "fail to init table storage for table "
                         << request->table().name();
            status->set_code(common::kRequestError);
            status->set_msg("fail to init table storage");
            return;
        }
        ok = AddTableLocked(table);
        if (!ok) {
            LOG(WARNING) << "table with name " << request->table().name()
                         << " exists";
            status->set_code(common::kTableExists);
            status->set_msg("table exist");
            return;
        }
        // TODO(wangtaize) just one partition
        break;
    }
    status->set_code(common::kOk);
    DLOG(INFO) << "create table with name " << request->table().name()
               << " done";
}

void TabletServerImpl::Insert(RpcController* ctrl, const InsertRequest* request,
                              InsertResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ::hybridse::common::Status* status = response->mutable_status();
    if (request->db().empty() || request->table().empty()) {
        status->set_code(common::kRequestError);
        status->set_msg("db or table name is empty");
        return;
    }
    std::shared_ptr<TabletTableHandler> handler =
        GetTableLocked(request->db(), request->table());

    if (!handler) {
        status->set_code(common::kTableNotFound);
        status->set_msg("table is not found");
        return;
    }

    bool ok =
        handler->GetTable()->Put(request->row().c_str(), request->row().size());
    if (!ok) {
        status->set_code(common::kTablePutFailed);
        status->set_msg("fail to put row");
        LOG(WARNING) << "fail to put data to table " << request->table()
                     << " with key " << request->key();
        return;
    }
    status->set_code(common::kOk);
}

void TabletServerImpl::Query(RpcController* ctrl, const QueryRequest* request,
                             QueryResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    common::Status* status = response->mutable_status();
    status->set_code(common::kOk);
    status->set_msg("ok");
    brpc::Controller* cntl = static_cast<brpc::Controller*>(ctrl);
    butil::IOBuf& buf = cntl->response_attachment();
    if (request->is_batch()) {
        vm::BatchRunSession session;
        session.SetParameterSchema(request->parameter_schema());
        {
            base::Status base_status;
            bool ok = engine_->Get(request->sql(), request->db(), session,
                                   base_status);
            if (!ok) {
                status->set_code(base_status.code);
                if (FLAGS_enable_trace) {
                    status->set_msg(base_status.str());
                } else {
                    status->set_msg(base_status.GetMsg());
                }
                return;
            }
        }

        if (request->is_debug()) {
            session.EnableDebug();
        }
        codec::Row parameter(request->parameter_row());
        std::vector<hybridse::codec::Row> outputs;
        int32_t ret = session.Run(parameter, outputs);

        if (0 != ret) {
            LOG(WARNING) << "fail to run sql " << request->sql();
            status->set_code(common::kRunSessionError);
            status->set_msg("fail to run sql");
            return;
        }

        uint32_t byte_size = 0;
        uint32_t count = 0;
        for (auto& row : outputs) {
            byte_size += row.size();
            buf.append(reinterpret_cast<void*>(row.buf()), row.size());
            count += 1;
        }
        response->set_schema(session.GetEncodedSchema());
        response->set_byte_size(byte_size);
        response->set_count(count);
        status->set_code(common::kOk);
    } else {
        if (request->row().empty()) {
            status->set_code(common::kRequestError);
            status->set_msg("input row is empty");
            return;
        }
        vm::RequestRunSession session;
        {
            base::Status base_status;
            bool ok = engine_->Get(request->sql(), request->db(), session,
                                   base_status);
            if (!ok) {
                status->set_code(base_status.code);
                if (FLAGS_enable_trace) {
                    status->set_msg(base_status.str());
                } else {
                    status->set_msg(base_status.GetMsg());
                }
                return;
            }
        }
        if (request->is_debug()) {
            session.EnableDebug();
        }
        codec::Row row(request->row());
        codec::Row output;
        int32_t ret = session.Run(request->task_id(), row, &output);
        if (ret != 0) {
            LOG(WARNING) << "fail to run sql " << request->sql();
            status->set_code(common::kRunSessionError);
            status->set_msg("fail to run sql");
            return;
        }
        buf.append(reinterpret_cast<void*>(output.buf()), output.size());
        response->set_schema(session.GetEncodedSchema());
        response->set_byte_size(output.size());
        response->set_count(1);
        status->set_code(common::kOk);
    }
}

void TabletServerImpl::Explain(RpcController* ctrl,
                               const ExplainRequest* request,
                               ExplainResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    common::Status* status = response->mutable_status();
    vm::ExplainOutput output;
    base::Status base_status;
    bool ok = engine_->Explain(request->sql(), request->db(), vm::kRequestMode,
                               request->parameter_schema(),
                               &output, &base_status);
    if (!ok || base_status.code != 0) {
        if (FLAGS_enable_trace) {
            status->set_msg(base_status.str());
        } else {
            status->set_msg(base_status.GetMsg());
        }
        status->set_code(base_status.code);
        return;
    }
    ok = codec::SchemaCodec::Encode(output.input_schema,
                                    response->mutable_input_schema());
    if (!ok) {
        status->set_msg("Fail encode input schema");
        status->set_code(common::kSchemaCodecError);
        return;
    }
    ok = codec::SchemaCodec::Encode(output.output_schema,
                                    response->mutable_output_schema());
    if (!ok) {
        status->set_msg("fail encode output schema");
        status->set_code(common::kSchemaCodecError);
        return;
    }
    response->set_ir(output.ir);
    response->set_logical_plan(output.logical_plan);
    response->set_physical_plan(output.physical_plan);
    status->set_code(common::kOk);
}

void TabletServerImpl::GetTableSchema(RpcController* ctrl,
                                      const GetTablesSchemaRequest* request,
                                      GetTableSchemaReponse* response,
                                      Closure* done) {
    brpc::ClosureGuard done_guard(done);
    ::hybridse::common::Status* status = response->mutable_status();
    std::shared_ptr<TabletTableHandler> handler =
        GetTableLocked(request->db(), request->name());
    if (!handler) {
        status->set_code(common::kTableNotFound);
        status->set_msg("table is not found");
        return;
    }
    response->mutable_schema()->CopyFrom(handler->GetTable()->GetTableDef());
}

}  // namespace tablet
}  // namespace hybridse
