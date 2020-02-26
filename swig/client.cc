#include "client.h"

RtidbNSClient::RtidbNSClient() {

}

bool RtidbNSClient::Init(const std::string& zk_cluster, const std::string& zk_path, const std::string& endpoint) {
    std::string value;
    if (!zk_cluster.empty()) {
        zk_client_ = new rtidb::zk::ZkClient(zk_cluster, 1000, "", zk_path);
        if (!zk_client_->Init()) {
            delete zk_client_;
            std::cerr << "zk client init failed" << std::endl;
            return false;
        }
        std::string node_path = zk_path + "/leader";
        std::vector<std::string> children;
        if (!zk_client_->GetChildren(node_path, children) || children.empty()) {
            std::cerr << "get children failed" << std::endl;
            delete zk_client_;
            return false;
        }
        std::string leader_path = node_path + "/" + children[0];
        if (!zk_client_->GetNodeValue(leader_path, value)) {
            std::cerr << "get leader failed" << std::endl;
            delete zk_client_;
            return false;
        }
        std::cout << "ns leader: " << value << std::endl;
    } else if (!endpoint.empty()) {
        value = endpoint;
    } else {
        std::cerr << "start failed! not set endpoint or zk_cluster";
        delete zk_client_;
        return false;
    }

    client_ = std::make_shared<rtidb::client::NsClient>(value);
    if (client_->Init() < 0) {
        delete zk_client_;
        client_.reset();
        std::cerr << "client init failed" << std::endl;
        return false;
    }
    return true;
}

std::vector<std::string>* RtidbNSClient::ShowTable(const std::string& name) {
    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    std::vector<std::string>* table_names = new std::vector<std::string>;
    bool ok = client_->ShowTable(name, tables, msg);
    if (ok) {
        for (uint32_t i = 0; i < tables.size(); i++) {
            std::string name = tables[i].name();
            table_names->push_back(name);
        }
    }
    return table_names;
}

