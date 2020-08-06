//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-08

#include "blob_proxy/blob_proxy_impl.h"

#include <fcntl.h>
#include <gflags/gflags.h>

#include <memory>
#include <string>
#include <vector>

#include "boost/algorithm/string/classification.hpp"
#include "boost/algorithm/string/split.hpp"

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace blobproxy {
BlobProxyImpl::BlobProxyImpl()
    : mu_(), server_(NULL), client_(NULL) {}

BlobProxyImpl::~BlobProxyImpl() {
    if (client_ != NULL) {
        delete client_;
    }
}

bool BlobProxyImpl::Init() {
    std::lock_guard<std::mutex> lock(mu_);
    if (FLAGS_zk_cluster.empty()) {
        PDLOG(WARNING, "zk cluster disabled");
        return false;
    }
    client_ = new BaseClient(FLAGS_zk_cluster, FLAGS_zk_root_path,
            FLAGS_endpoint, FLAGS_zk_session_timeout,
            FLAGS_zk_keep_alive_check_interval);
    std::string msg;
    bool ok = client_->Init(&msg);
    if (!ok) {
        PDLOG(WARNING, "%s", msg.c_str());
        return false;
    }
    return true;
}

void BlobProxyImpl::Get(RpcController* controller, const HttpRequest* request,
                        HttpResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string table;
    int64_t blob_id = 0;
    std::string unresolve_path = cntl->http_request().unresolved_path();
    std::vector<std::string> vec;
    boost::split(vec, unresolve_path, boost::is_any_of("/"));
    if (vec.size() == 1) {
        table = unresolve_path;
        const std::string* id = cntl->http_request().uri().GetQuery("blob_id");
        if (id == NULL) {
            cntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            return;
        }
        try {
            blob_id = boost::lexical_cast<int64_t>(*id);
        } catch (boost::bad_lexical_cast&) {
            cntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            return;
        }
    } else if (vec.size() == 2) {
        table = vec[0];

        try {
            blob_id = boost::lexical_cast<int64_t>(vec[1]);
        } catch (boost::bad_lexical_cast&) {
            cntl->http_response().set_status_code(
                brpc::HTTP_STATUS_BAD_REQUEST);
            return;
        }
    } else {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        return;
    }
    auto& response_writer = cntl->response_attachment();
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(table);
    if (!th) {
        PDLOG(INFO, "table %s not found", table.c_str());
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        response_writer.append("table not found");
        return;
    }
    if (th->blob_partition.empty()) {
        PDLOG(INFO, "table %s is not blob store", table.c_str());
        cntl->http_request().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        response_writer.append("table is not object store");
        return;
    }
    std::shared_ptr<rtidb::client::BsClient> blob;
    std::string err_msg;
    if (th->blob_partition[0].leader.empty()) {
        PDLOG(INFO, "table[%s] pid[%u] not found available endpoint",
              table.c_str(), 0);
    }
    blob = client_->GetBlobClient(th->blob_partition[0].leader, &err_msg);
    if (!blob) {
        cntl->http_response().set_status_code(
            brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        response_writer.append(err_msg);
        return;
    }
    butil::IOBuf buff;
    bool ok = blob->Get(th->table_info->tid(), 0, blob_id, &err_msg, &buff);
    if (!ok) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        response_writer.append(err_msg);
        return;
    }
    response_writer.append(buff);
}

}  // namespace blobproxy
}  // namespace rtidb
