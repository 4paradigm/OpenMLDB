#pragma once
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "zk/zk_client.h"
#include <string>

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

struct ReadOption {
    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
    ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    };
};
class RtidbNSClient {
private:
    rtidb::zk::ZkClient* zk_client_;
    std::shared_ptr<rtidb::client::NsClient> client_;
    std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>> tablets_;
    std::shared_ptr<rtidb::client::TabletClient> GetTabletClient(const std::string& endpoint);
    std::mutex mu_;

public:
    RtidbNSClient();
    ~RtidbNSClient() {
        if (zk_client_ != NULL) {
            delete zk_client_;
        }
    };
    bool Init(const std::string& zk_cluster, const std::string& zk_path);
    std::vector<std::string>* ShowTable(const std::string& name);
    std::map<std::string, GetColumn> Get(const std::string& name, struct ReadOption& ro);
    bool Put(const std::string& name, const std::map<std::string, std::string>& values, const WriteOption& wo);
    bool Delete(const std::string& name, const std::map<std::string, std::string>& values);
    bool Update(const std::string& name, const std::map<std::string, std::string>& condition, const std::map<std::string, std::string> value, const WriteOption& wo);

};
