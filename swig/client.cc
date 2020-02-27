#include "client.h"

RtidbNSClient::RtidbNSClient() {
    zk_client_ = NULL;
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

RtidbTabletClient::RtidbTabletClient() {
    client_ = NULL;
};

bool RtidbTabletClient::Init(const std::string& endpoint) {
    if (endpoint.empty()) {
        return false;
    }
    client_ = new rtidb::client::TabletClient(endpoint);
    if (client_->Init() < 0) {
        delete client_;
        std::cerr << "client init failed" << std::endl;
        return false;
    }
    return true;
};

bool RtidbTabletClient::Put(const uint32_t tid, const uint32_t pid, const std::string& pk, const uint64_t time, const std::string& value) {
    return client_->Put(tid, pid, pk.c_str(), time, value.c_str(), value.size());
};

std::string RtidbTabletClient::Get(uint32_t tid, uint32_t pid, const std::string& pk, uint64_t time) {
    std::string value, msg;
    uint64_t ts;
    bool ok = client_->Get(tid, pid, pk, time, value, ts, msg);
    if (!ok) {
        return value;
    }
    return value;
};
