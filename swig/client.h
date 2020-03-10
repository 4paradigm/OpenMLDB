#pragma once
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "zk/zk_client.h"
#include <string>
#include <set>

struct WriteOption {
    bool updateIfExist;
    bool updateIfEqual;
    WriteOption() {
        updateIfEqual = true;
        updateIfEqual = true;
    }
};

struct ReadFilter {
    std::string column;
    uint8_t type;
    std::string value;
};

struct GetColumn {
    uint8_t type;
    std::string buffer;
};

struct QueryResult {
    int code;
    std::string msg;
    std::vector<std::map<std::string, GetColumn>> values;
    QueryResult():code(0), msg() {
    };
    void SetError(int err_code, const std::string& err_msg) {
        code = err_code;
        msg = err_msg;
    }
};

struct GeneralResult {
    int code;
    std::string msg;
    GeneralResult():code(0), msg() {}
    GeneralResult(int err_num):code(err_num), msg() {}
    GeneralResult(int err_num, const std::string error_msg):code(err_num), msg(error_msg) {}
    void SetError(int err_num, const std::string& error_msg) {
        code = err_num;
        msg = error_msg;
    }
};

struct ReadOption {
    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
    ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    };
};

struct PartitionInfo {
    std::string leader;
    std::vector<std::string> follower;
};

struct TableHandler {
    std::shared_ptr<rtidb::nameserver::TableInfo>  table_info;
    std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>> columns;
    std::vector<PartitionInfo> partition;
};

class RtidbClient {
public:
    RtidbClient();
    ~RtidbClient();
    GeneralResult Init(const std::string& zk_cluster, const std::string& zk_path);
    QueryResult Query(const std::string& name, struct ReadOption& ro);
    GeneralResult Put(const std::string& name, const std::map<std::string, std::string>& values, const WriteOption& wo);
    GeneralResult Delete(const std::string& name, const std::map<std::string, std::string>& values);
    GeneralResult Update(const std::string& name, const std::map<std::string, std::string>& condition, const std::map<std::string, std::string> values, const WriteOption& wo);
private:
    std::shared_ptr<rtidb::client::TabletClient> GetTabletClient(const std::string& endpoint, std::string& msg);
    void CheckZkClient();
    void UpdateEndpoint(const std::set<std::string>& alive_endpoints);
    bool RefreshNodeList();
    void RefreshTable();
    void DoFresh(const std::vector<std::string>& events) {
        RefreshNodeList();
        RefreshTable();
    }

private:
    std::shared_ptr<rtidb::zk::ZkClient> zk_client_;
    std::shared_ptr<rtidb::client::NsClient> client_;
    std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>> tablets_;
    std::mutex mu_;
    std::string zk_cluster_;
    std::string zk_path_;
    std::string zk_table_data_path_;
    std::string zk_table_notify_path_;
    std::map<std::string, std::shared_ptr<TableHandler>> tables_;
};
