#include "client.h"

rtidb::client::NsClient* InitNsClient(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint) {
    std::shared_ptr<rtidb::zk::ZkClient> zk_client;
    std::string value;
    if (!zk_cluster.empty()) {
        zk_client = std::make_shared<rtidb::zk::ZkClient>(zk_cluster, 1000, "", zk_path);
        if (!zk_client->Init()) {
            std::cerr << "zk client init failed" << std::endl;
            return NULL;
        }
        std::string node_path = zk_path + "/leader";
        std::vector<std::string> children;
        if (!zk_client->GetChildren(node_path, children) || children.empty()) {
            std::cerr << "get children failed" << std::endl;
            return NULL;
        }
        std::string leader_path = node_path + "/" + children[0];
        if (!zk_client->GetNodeValue(leader_path, value)) {
            std::cerr << "get leader failed" << std::endl;
            return NULL;
        }
        std::cout << "ns leader: " << value << std::endl;
    } else if (!endpoint.empty()) {
        value = endpoint;
    } else {
        std::cerr << "start failed! not set endpoint or zk_cluster";
        return NULL;
    }

    rtidb::client::NsClient* client = new rtidb::client::NsClient(value);
    if (client->Init() < 0) {
        std::cerr << "client init failed" << std::endl;
        delete client;
        return NULL;
    }
    return client;
}

std::vector<std::string>* ShowTable(rtidb::client::NsClient* client, const std::string& name) {
    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    std::vector<std::string>* table_names = new std::vector<std::string>;
    bool ok = client->ShowTable(name, tables, msg);
    if (ok) {
        for (uint32_t i = 0; i < tables.size(); i++) {
            std::string name = tables[i].name();
            table_names->push_back(name);
        }
    }
    return table_names;
}
