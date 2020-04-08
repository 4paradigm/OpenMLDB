//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-08

#include "httpserver/httpserver.h"
#include <gflags/gflags.h>
#include <string>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace http {
HttpImpl::HttpImpl() : mu_(), zk_client_(NULL), server_(NULL), client(NULL) {}

HttpImpl::~HttpImpl() {
    if (client != NULL) {
        delete client;
    }
}

bool HttpImpl::Init() {
    std::lock_guard<std::mutex> lock(mu_);
    if (FLAGS_zk_cluster.empty()) {
        PDLOG(WARNING, "zk cluster disabled");
        return false;
    }
    client = new BaseClient(FLAGS_zk_cluster, FLAGS_zk_root_path,
                            FLAGS_endpoint, FLAGS_zk_session_timeout,
                            FLAGS_zk_keep_alive_check_interval);
    std::string msg;
    bool ok = client->Init(&msg);
    if (!ok) {
        PDLOG(WARNING, "%s", msg.c_str());
        return false;
    }
    return true;
}

bool HttpImpl::RegisterZk() {
    std::string msg;
    bool ok = client->RegisterZK(&msg);
    if (!ok) {
        PDLOG(WARNING, "register zk error: %s", msg.c_str());
    }
    return ok;
}

void HttpImpl::Get(RpcController* controller,
                const ::rtidb::httpserver::HttpRequest* request,
                ::rtidb::httpserver::HttpResponse* response, Closure* done) {
    brpc::ClosureGuard done_guard(done);
    brpc::Controller* cntl = static_cast<brpc::Controller*>(controller);
    std::string table;
    const std::string* pic_id;
    std::string unresolve_path = cntl->http_request().unresolved_path();
    std::vector<std::string> vec;
    boost::split(vec, unresolve_path, boost::is_any_of("/"));
    if (vec.size() == 1) {
        table = unresolve_path;
        pic_id = cntl->http_request().uri().GetQuery("pic_id");
        if (pic_id == NULL) {
            cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
            return;
        }
    } else if (vec.size() == 2) {
        table = vec[0];
        pic_id = &vec[2];
    } else {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        return;
    }
    cntl->response_attachment().append("Getting file: ");
    cntl->response_attachment().append(table);
    cntl->response_attachment().append(" " + *pic_id);
}

}  // namespace http
}  // namespace rtidb
