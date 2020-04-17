//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "blobserver/blobserver_impl.h"
#include "boost/bind.hpp"
#include "logging.h"
#include <gflags/gflags.h>

DECLARE_string(endpoint);
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_int32(zk_session_timeout);
DECLARE_int32(zk_keep_alive_check_interval);
DECLARE_string(hdd_root_path);

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

namespace rtidb {
namespace blobserver {

BlobServerImpl::BlobServerImpl() : mu_(), zk_client_(NULL), server_(NULL), keep_alive_pool_(1), object_stores_() {}

BlobServerImpl::~BlobServerImpl() {
    if (zk_client_ != NULL) {
        delete zk_client_;
    }
}

bool BlobServerImpl::Init() {
    std::lock_guard<std::mutex> lock(mu_);
    if (!FLAGS_zk_cluster.empty()) {
        std::string oss_zk_path = FLAGS_zk_root_path + "/ossnodes";
        zk_client_ =
            new ZkClient(FLAGS_zk_cluster, FLAGS_zk_session_timeout,
                         FLAGS_endpoint, FLAGS_zk_root_path, oss_zk_path);
        bool ok = zk_client_->Init();
        if (!ok) {
            PDLOG(WARNING, "fail to init zookeeper with cluster %s",
                  FLAGS_zk_cluster.c_str());
        }
    } else {
        PDLOG(INFO, "zk cluster disabled");
    }
    if (FLAGS_hdd_root_path.empty()) {
        PDLOG(WARNING, "hdd root path did not set");
        return false;
    }
    return true;
}

bool BlobServerImpl::RegisterZK() {
    if (!FLAGS_zk_cluster.empty()) {
        if (!zk_client_->Register(true)) {
            PDLOG(WARNING, "fail to register tablet with endpoint %s",
                  FLAGS_endpoint.c_str());
            return false;
        }
        PDLOG(INFO, "oss with endpoint %s register to zk cluster %s ok",
              FLAGS_endpoint.c_str(), FLAGS_zk_cluster.c_str());
        keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval,
                                   boost::bind(&BlobServerImpl::CheckZkClient, this));
    }
    return true;
}

void BlobServerImpl::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        PDLOG(WARNING, "reconnect zk");
        if (zk_client_->Reconnect() && zk_client_->Register()) {
            PDLOG(INFO, "reconnect zk ok");
        }
    } else if (!zk_client_->IsRegisted()) {
        PDLOG(WARNING, "registe zk");
        if (zk_client_->Register()) {
            PDLOG(INFO, "registe zk ok");
        }
    }
    keep_alive_pool_.DelayTask(FLAGS_zk_keep_alive_check_interval,
                               boost::bind(&BlobServerImpl::CheckZkClient, this));
}

void BlobServerImpl::CreateTable(RpcController *controller,
                          const CreateTableRequest *request,
                          GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void BlobServerImpl::Get(RpcController *controller, const GeneralRequest *request,
                  GetResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void BlobServerImpl::Put(RpcController *controller, const PutRequest *request,
                  GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void BlobServerImpl::Delete(RpcController *controller, const GeneralRequest *request,
                     GeneralResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

void BlobServerImpl::Stats(RpcController *controller, const StatsRequest *request,
                    StatsResponse *response, Closure *done) {
    brpc::ClosureGuard done_guard(done);
}

}  // namespace oss
}  // namespace rtidb
