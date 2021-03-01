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

BsClient::BsClient(const std::string &endpoint, const std::string& real_endpoint)
    : endpoint_(endpoint), client_(endpoint) {
        if (!real_endpoint.empty()) {
            client_ = ::rtidb::RpcClient<BlobServer_Stub>(real_endpoint);
        }
    }

BsClient::BsClient(const std::string &endpoint, const std::string& real_endpoint, bool use_sleep_policy)
    : endpoint_(endpoint), client_(endpoint, use_sleep_policy) {
        if (!real_endpoint.empty()) {
            client_ = ::rtidb::RpcClient<BlobServer_Stub>(real_endpoint, use_sleep_policy);
        }
    }

int BsClient::Init() { return client_.Init(); }

std::string BsClient::GetEndpoint() { return endpoint_; }

bool BsClient::CreateTable(const TableMeta &table_meta, std::string *msg) {
    ::rtidb::blobserver::CreateTableRequest request;
    auto meta = request.mutable_table_meta();
    meta->CopyFrom(table_meta);
    ::rtidb::blobserver::CreateTableResponse response;
    bool ok = client_.SendRequest(&BlobServer_Stub::CreateTable, &request,
                                  &response, FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::LoadTable(uint32_t tid, uint32_t pid, std::string *msg) {
    ::rtidb::blobserver::LoadTableRequest request;
    ::rtidb::blobserver::TableMeta* table_meta = request.mutable_table_meta();
    table_meta->set_tid(tid);
    table_meta->set_pid(pid);
    table_meta->set_table_type(::rtidb::type::kObjectStore);
    ::rtidb::blobserver::LoadTableResponse response;
    bool ok =
        client_.SendRequest(&::rtidb::blobserver::BlobServer_Stub::LoadTable,
                            &request, &response, FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Put(uint32_t tid, uint32_t pid, int64_t key,
                   const std::string &value, std::string *msg) {
    ::rtidb::blobserver::PutRequest request;
    ::rtidb::blobserver::PutResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(key);
    request.set_allocated_data(const_cast<std::string *>(&value));
    bool ok = client_.SendRequest(&BlobServer_Stub::Put, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    request.release_data();
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Put(uint32_t tid, uint32_t pid, const std::string &value,
                   int64_t* key, std::string *msg) {
    ::rtidb::blobserver::PutRequest request;
    ::rtidb::blobserver::PutResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_allocated_data(const_cast<std::string *>(&value));
    bool ok = client_.SendRequest(&BlobServer_Stub::Put, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    request.release_data();
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        *key = response.key();
        return true;
    }
    return false;
}

bool BsClient::Put(uint32_t tid, uint32_t pid, char* value, int64_t len,
                   int64_t* key, std::string *msg) {
    ::rtidb::blobserver::PutRequest request;
    ::rtidb::blobserver::PutResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_data(static_cast<void*>(value), len);
    bool ok = client_.SendRequest(&BlobServer_Stub::Put, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        *key = response.key();
        return true;
    }
    return false;
}

bool BsClient::Get(uint32_t tid, uint32_t pid, int64_t key,
                   std::string *value, std::string *msg) {
    ::rtidb::blobserver::GetRequest request;
    ::rtidb::blobserver::GetResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(key);
    bool ok = client_.SendRequest(&BlobServer_Stub::Get, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        value->swap(*response.mutable_data());
        return true;
    }
    return false;
}

bool BsClient::Get(uint32_t tid, uint32_t pid, int64_t key,
                   std::string *msg, butil::IOBuf *buff) {
    ::rtidb::blobserver::GetRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(key);
    request.set_use_attachment(true);
    ::rtidb::blobserver::GetResponse response;
    bool ok = client_.SendRequestGetAttachment(
        &BlobServer_Stub::Get, &request, &response, FLAGS_request_timeout_ms, 1,
        buff);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::Delete(uint32_t tid, uint32_t pid, int64_t key,
                      std::string *msg) {
    ::rtidb::blobserver::DeleteRequest request;
    ::rtidb::blobserver::DeleteResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    request.set_key(key);
    bool ok = client_.SendRequest(&BlobServer_Stub::Delete, &request, &response,
                                  FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::DropTable(uint32_t tid, uint32_t pid, std::string *msg) {
    ::rtidb::blobserver::DropTableRequest request;
    ::rtidb::blobserver::DropTableResponse response;
    request.set_tid(tid);
    request.set_pid(pid);
    bool ok = client_.SendRequest(&BlobServer_Stub::DropTable, &request,
                                  &response, FLAGS_request_timeout_ms, 1);
    msg->swap(*response.mutable_msg());
    if (ok && response.code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::GetStoreStatus(
    ::rtidb::blobserver::GetStoreStatusResponse *response) {
    ::rtidb::blobserver::GetStoreStatusRequest request;
    bool ok = client_.SendRequest(&BlobServer_Stub::GetStoreStatus, &request,
                                  response, FLAGS_request_timeout_ms, 1);
    if (ok || response->code() == 0) {
        return true;
    }
    return false;
}

bool BsClient::GetStoreStatus(uint32_t tid, uint32_t pid,
                              ::rtidb::blobserver::StoreStatus *status) {
    ::rtidb::blobserver::GetStoreStatusRequest request;
    request.set_tid(tid);
    request.set_pid(pid);
    ::rtidb::blobserver::GetStoreStatusResponse response;
    bool ok = client_.SendRequest(&BlobServer_Stub::GetStoreStatus, &request,
                                  &response, FLAGS_request_timeout_ms, 1);
    if (ok || response.code() == 0) {
        if (response.all_status_size() < 0) {
            return false;
        }
        const auto &temp_status = response.all_status(0);
        status->CopyFrom(temp_status);
        return true;
    }
    return false;
}

}  // namespace client
}  // namespace rtidb
