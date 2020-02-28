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

public:
    RtidbNSClient();
    ~RtidbNSClient() {
        if (zk_client_ != NULL) {
            delete zk_client_;
        }
    };
    bool Init(const std::string& zk_cluster, const std::string& zk_path);
    std::vector<std::string>* ShowTable(const std::string& name);
    std::vector<std::map<std::string, rtidb::base::Column>> Get(const std::string& name, struct ReadOption& ro);

};

class RtidbTabletClient {
private:
    rtidb::client::TabletClient* client_;
public:
    RtidbTabletClient();
    ~RtidbTabletClient() {
        if (client_ != NULL) {
            delete client_;
        }
    };
    bool Init(const std::string& endpoint);
    bool Put(const uint32_t tid, const uint32_t pid, const std::string& pk, const uint64_t time, const std::string& value);
    std::string Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time);
};
