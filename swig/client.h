#include "client/ns_client.h"
#include "zk/zk_client.h"
#include <string>

class RtidbNSClient {
private:
    rtidb::zk::ZkClient* zk_client_;
    std::shared_ptr<rtidb::client::NsClient> client_;

public:
    RtidbNSClient();
    ~RtidbNSClient() {};
    bool Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint);
    std::vector<std::string>* ShowTable(const std::string& name);

};
