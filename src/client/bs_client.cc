//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "client/bs_client.h"
#include "proto/blob_server.pb.h"

DECLARE_int32(request_timeout_ms);

using rtidb::blobserver::BlobServer_Stub;

namespace rtidb {
namespace client {

BsClient::BsClient(const std::string &endpoint):endpoint_(endpoint),
    client_(endpoint){
}

BsClient::BsClient(const std::string &endpoint, bool use_sleep_policy):
    endpoint_(endpoint), client_(endpoint, use_sleep_policy){
}

int BsClient::Init() {
    return client_.Init();
}

std::string BsClient::GetEndpoint() {
    return endpoint_;
}

bool BsClient::CreateTable(const TableMeta &table_meta, std::string *msg) {
    ::rtidb::blobserver::CreateTableRequest request;
    auto meta = request.mutable_table_meta();
    meta->CopyFrom(table_meta);
    ::rtidb::blobserver::GeneralResponse response;
    response.set_allocated_msg(msg);
    bool ok = client_.SendRequest(&BlobServer_Stub::CreateTable, &request, &response, FLAGS_request_sleep_time, 1);
    response.release_msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Put(uint32_t tid, uint32_t pid, const std::string &key, const std::string &value, std::string *msg) {
    ::rtidb::blobserver::PutRequest request;
    ::rtidb::blobserver::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(key);
    request.set_pairs(value);
    response.set_allocated_msg(msg);
    bool ok = client_.SendRequest(&BlobServer_Stub::Put, &request, &response, FLAGS_request_timeout_ms, 1);
    response.release_msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Get(uint32_t tid, uint32_t pid, const std::string &key, std::string *value, std::string *msg) {
    ::rtidb::blobserver::GeneralRequest request;
    ::rtidb::blobserver::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(key);
    response.set_allocated_pairs(value);
    response.set_allocated_msg(msg);
    bool ok = client_.SendRequest(&BlobServer_Stub::Get, &request, &response, FLAGS_request_timeout_ms, 1);
    response.release_msg();
    response.release_pairs();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Delete(uint32_t tid, uint32_t pid, const std::string &key, std::string *msg) {
    ::rtidb::blobserver::GeneralRequest request;
    ::rtidb::blobserver::GeneralResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    response.set_allocated_msg(msg);
    bool ok = client_.SendRequest(&BlobServer_Stub::Delete, &request, &response, FLAGS_request_timeout_ms, 1);
    response.release_msg();
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Stats(uint32_t tid, uint32_t pid, uint64_t *count, uint64_t *total_space, uint64_t *avail_space, std::string* msg) {
    ::rtidb::blobserver::StatsRequest request;
    ::rtidb::blobserver::StatsResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    response.set_allocated_msg(msg);
    bool ok = client_.SendRequest(&BlobServer_Stub::Stats, &request, &response, FLAGS_request_timeout_ms, 1);
    response.release_msg();
    if (ok && response.code() == 0) {
        *count = response.count();
        *total_space = response.total_space();
        *avail_space = response.avail_space();
        return true;
    }
    return false;
}

bool BsClient::GetStoreStatus(::rtidb::blobserver::GetStoreStatusResponse* response) {
    ::rtidb::blobserver::GetStoreStatusRequest request;
    bool ok = client_.SendRequest(&BlobServer_Stub::GetStoreStatus, &request, response, FLAGS_request_timeout_ms, 1);
    if (ok || response->code() == 0) {
        return  true;
    }
    return false;
}

bool BsClient::GetStoreStatus(uint32_t tid, uint32_t pid, ::rtidb::blobserver::StoreStatus* status) {
    ::rtidb::blobserver::GetStoreStatusRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::blobserver::GetStoreStatusResponse response;
    bool ok = client_.SendRequest(&BlobServer_Stub::GetStoreStatus, &request, &response, FLAGS_request_timeout_ms, 1);
    if (ok || response.code() == 0) {
        if (response.all_status_size() < 0) {
            return false;
        }
        const auto& temp_status = response.all_status(0);
        status->CopyFrom(temp_status);
        return  true;
    }
    return false;
}

}
}
