#include "client/ns_client.h"
#include "zk/zk_client.h"
#include <string>

class RtidbNSClient {
private:
    std::shared_ptr<rtidb::zk::ZkClient> zk_client;
    std::shared_ptr<rtidb::client::NsClient> client;

public:
    ~RtidbNSClient() {};
    RtidbNSClient(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint);
    std::vector<std::string>* ShowTable(const std::string& name);

};
