//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-08

#include "httpserver/httpserver.h"
#include <gflags/gflags.h>
#include <string>
#include <boost/algorithm/string/split.hpp>
#include <boost/algorithm/string/classification.hpp>
#include <fcntl.h>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);

namespace rtidb {
namespace http {
HttpImpl::HttpImpl() : mu_(), zk_client_(NULL), server_(NULL), client_(NULL) {}

HttpImpl::~HttpImpl() {
    if (client_ != NULL) {
        delete client_;
    }
}

bool HttpImpl::Init() {
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
        pic_id = &vec[1];
    } else {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_BAD_REQUEST);
        return;
    }
    auto& response_writer = cntl->response_attachment();
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(table);
    if (!th) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        response_writer.append("tablet not found");
        return;
    }
    std::string err_msg;
    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
    if (!tablet) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        response_writer.append(err_msg);
        return;
    }
    uint64_t ts;
    std::string value;
    bool ok = tablet->Get(th->table_info->tid(), 0, *pic_id, 0, "", "", value, ts, err_msg);
    if (!ok) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        response_writer.append(err_msg);
        return;
    }
    rtidb::base::RowView rv(*(th->columns));
    const char* buffer = value.c_str();
    rv.Reset(reinterpret_cast<int8_t*>(const_cast<char*>(buffer)), value.length());
    int i = 0;
    /*
    for (; i < th->columns->size(); i++) {
        auto& cur_col = th->columns->Get(i);
        if (cur_col.data_type() == rtidb::type::kBlob) {
            break;
        }
    }
    if (i >= th->columns->size()) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_NOT_FOUND);
        response_writer.append("not found blob field");
        return;
    }
     */
    i = 1; // current do not process blob
    char* ch = NULL;
    uint32_t length = 0;
    int ret = rv.GetString(i, &ch, &length);
    if (ret != 0) {
        cntl->http_response().set_status_code(brpc::HTTP_STATUS_INTERNAL_SERVER_ERROR);
        response_writer.append("get blob failed");
        return;
    }

    response_writer.append(ch, length);
}

}  // namespace http
}  // namespace rtidb
