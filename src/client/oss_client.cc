//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "client/oss_client.h"

namespace rtidb {
namespace client {

OssClient::OssClient(const std::string &endpoint):endpoint_(endpoint),
    client_(endpoint){
}

OssClient::OssClient(const std::string &endpoint, bool use_sleep_policy):
    endpoint_(endpoint), client_(endpoint, use_sleep_policy){
}

int OssClient::Init() {
    return 0;
}

std::string OssClient::GetEndpoint() {
    return endpoint_;
}

bool OssClient::CreateTable(const OssTableMeta &table_meta, std::string *msg) {
    return true;
}

bool OssClient::Put(uint32_t tid, uint32_t pid, const std::string &key, const std::string &value, std::string *msg) {
    return true;
}

bool OssClient::Get(uint32_t tid, uint32_t pid, const std::string &key, std::string *value, std::string *msg) {
    return true;
}

bool OssClient::Delete(uint32_t tid, uint32_t pid, const std::string &key, std::string *msg) {
    return true;
}

bool OssClient::Stats(uint32_t tid, uint32_t pid, uint64_t *count, uint64_t *total_space, uint64_t *avail_space) {
    return true;
}

}
}
