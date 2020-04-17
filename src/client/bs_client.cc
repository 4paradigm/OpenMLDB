//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-16

#include "client/bs_client.h"

namespace rtidb {
namespace client {

BsClient::BsClient(const std::string &endpoint):endpoint_(endpoint),
    client_(endpoint){
}

BsClient::BsClient(const std::string &endpoint, bool use_sleep_policy):
    endpoint_(endpoint), client_(endpoint, use_sleep_policy){
}

int BsClient::Init() {
    return 0;
}

std::string BsClient::GetEndpoint() {
    return endpoint_;
}

bool BsClient::CreateTable(const TableMeta &table_meta, std::string *msg) {
    return true;
}

bool BsClient::Put(uint32_t tid, uint32_t pid, const std::string &key, const std::string &value, std::string *msg) {
    return true;
}

bool BsClient::Get(uint32_t tid, uint32_t pid, const std::string &key, std::string *value, std::string *msg) {
    return true;
}

bool BsClient::Delete(uint32_t tid, uint32_t pid, const std::string &key, std::string *msg) {
    return true;
}

bool BsClient::Stats(uint32_t tid, uint32_t pid, uint64_t *count, uint64_t *total_space, uint64_t *avail_space) {
    return true;
}

}
}
