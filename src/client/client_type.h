//
// Copyright 2020 4paradigm
//
#pragma once
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "client/bs_client.h"
#include "client/tablet_client.h"
#include "codec/codec.h"
#include "codec/schema_codec.h"
#include "zk/zk_client.h"

struct WriteOption {
    WriteOption() {
        update_if_equal = true;
        update_if_exist = true;
    }

    bool update_if_exist;
    bool update_if_equal;
};

struct ReadFilter {
    std::string column;
    uint8_t type;
    std::string value;
};

struct PartitionInfo {
    std::string leader;
    std::vector<std::string> follower;
};

struct TableHandler {
    std::shared_ptr<rtidb::nameserver::TableInfo> table_info;
    std::shared_ptr<google::protobuf::RepeatedPtrField<
        rtidb::common::ColumnDesc>> columns;
    std::vector<PartitionInfo> partition;
    std::string auto_gen_pk_;
    std::vector<int32_t> blobSuffix;
    std::vector<std::string> blobFieldNames;
    std::string auto_gen_pk;
    std::map<std::string, ::rtidb::type::DataType> name_type_map;
};

struct GeneralResult {
    GeneralResult() : code(0), msg() {}

    explicit GeneralResult(int err_num) : code(err_num), msg() {}

    GeneralResult(int err_num, const std::string& error_msg)
        : code(err_num), msg(error_msg) {}

    void SetError(int err_num, const std::string& error_msg) {
        code = err_num;
        msg = error_msg;
    }
    int code;
    std::string msg;
};

struct PutResult : public GeneralResult {
    void SetAutoGenPk(int64_t num) {
        has_auto_gen_pk = true;
        auto_gen_pk = num;
    }
    int64_t auto_gen_pk;
    bool has_auto_gen_pk = false;
};

struct ReadOption {
    explicit ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    }

    ReadOption() : index(), read_filter(), col_set(), limit(0) {}

    ~ReadOption() = default;

    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
};

class BlobInfoResult {
 public:
    BlobInfoResult(): code_(0), msg_(), client_(nullptr), tid_(0), key_(0) {}

    void SetError(int code, const std::string& msg) {
        code_ = code;
        msg_ = msg;
    }

    int code_;
    std::string msg_;
    std::shared_ptr<rtidb::client::BsClient> client_;
    uint32_t tid_;
    int64_t key_;
};

struct BlobOPResult {
    int code_;
    std::string msg_;

};

