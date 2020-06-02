//
// Copyright 2020 4paradigm
//
#pragma once
#include <string>
#include <memory>

#include "client/bs_client.h"

class BlobInfoResult {
 public:
    BlobInfoResult(): code_(0), msg_(), client_(nullptr), tid_(0), key_(0) {}

    void SetError(int code, const std::string& msg) {
        code_ = code;
        msg_ = msg;
    }
    const char* GetMsg() { return msg_.data(); }

    int code_;
    std::string msg_;
    std::shared_ptr<rtidb::client::BsClient> client_;
    uint32_t tid_;
    int64_t key_;
};

struct BlobOPResult {
    const char* GetMsg() { return msg_.data(); }
    int code_;
    std::string msg_;
};
