#include "client.h"
#include "base/flat_array.h"
#include <boost/lexical_cast.hpp>

RtidbNSClient::RtidbNSClient() {
    zk_client_ = NULL;
}

bool RtidbNSClient::Init(const std::string& zk_cluster, const std::string& zk_path) {
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

std::vector<std::map<std::string, rtidb::base::Column>> RtidbNSClient::Get(const std::string& name, struct ReadOption& ro) {
    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    std::vector<std::map<std::string, rtidb::base::Column>> result;
    bool ok = client_->ShowTable(name, tables, msg);
    if (!ok) {
        std::cerr << "get table failed, error msg: " << msg << std::endl;
        return result;
    }
    if (tables.empty()) {
        std::cerr << "failed to get table info, error msg: " << msg << std::endl;
    }
    if (tables[0].table_type() != rtidb::type::TableType::kRelational) {
        std::cerr << "not support is not relation table" << std::endl;
        return result;
    }
    std::string tablet_endpoint;

    for (const auto& part : tables[0].table_partition()) {
        for (const auto& meta : part.partition_meta()) {
            if (meta.is_alive() && meta.is_leader()) {
                tablet_endpoint = meta.endpoint();
            }
        }
    }
    if (tablet_endpoint.empty()) {
        std::cerr << "failed to get table server endpoint" << std::endl;
        return result;
    }
    std::vector<rtidb::base::ColumnDesc> columns;
    if (tables[0].added_column_desc_size() > 0) {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns, tables[0].added_column_desc_size()) < 0) {
            std::cout << "convert table column desc failed" << std::endl;
            return result;
        }
    } else {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cout << "convert table column desc failed" << std::endl;
            return result;
        }
    }
    rtidb::client::TabletClient tablet_client(tablet_endpoint);
    for (const auto& iter : ro.index) {
        std::string value;
        uint64_t ts;
        ok = tablet_client.Get(tables[0].tid(), 0, iter.second, 0, iter.first, "", value, ts, msg);
        if (!ok) {
            std::cerr << "failed to get index " << iter.first << "value " << std::endl;
            continue;
        }
        rtidb::base::FlatArrayIterator fit(value.data(), value.size(), columns.size());
        std::vector<std::string> values;
        while (fit.Valid()) {
            std::string col;
            if (fit.GetType() == ::rtidb::base::ColType::kString) {
                fit.GetString(&col);
            }else if (fit.GetType() == ::rtidb::base::ColType::kInt32) {
                int32_t int32_col = 0;
                fit.GetInt32(&int32_col);
                col = boost::lexical_cast<std::string>(int32_col);
            }else if (fit.GetType() == ::rtidb::base::ColType::kInt64) {
                int64_t int64_col = 0;
                fit.GetInt64(&int64_col);
                col = boost::lexical_cast<std::string>(int64_col);
            }else if (fit.GetType() == ::rtidb::base::ColType::kUInt32) {
                uint32_t uint32_col = 0;
                fit.GetUInt32(&uint32_col);
                col = boost::lexical_cast<std::string>(uint32_col);
            }else if (fit.GetType() == ::rtidb::base::ColType::kUInt64) {
                uint64_t uint64_col = 0;
                fit.GetUInt64(&uint64_col);
                col = boost::lexical_cast<std::string>(uint64_col);
            }else if (fit.GetType() == ::rtidb::base::ColType::kDouble) {
                double double_col = 0.0;
                fit.GetDouble(&double_col);
                col = boost::lexical_cast<std::string>(double_col);
            }else if (fit.GetType() == ::rtidb::base::ColType::kFloat) {
                float float_col = 0.0f;
                fit.GetFloat(&float_col);
                col = boost::lexical_cast<std::string>(float_col);
            }
            fit.Next();
            values.push_back(col);
        }
        std::map<std::string, rtidb::base::Column> resp;
        for (int i = 0; i < columns.size(); i++) {
            std::string value = "";
            if (i < values.size()) {
                value = values[i];
            }
            rtidb::base::Column result;
            result.type = columns[i].type;
            result.buffer = value;
            resp.insert(std::make_pair(columns[i].name, result));
        }
        result.push_back(resp);
    }
    return result;
};

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
