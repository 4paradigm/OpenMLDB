#include "client.h"
#include "base/flat_array.h"
#include <boost/lexical_cast.hpp>
#include "base/hash.h"
#include <boost/algorithm/string.hpp>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

int PutData(uint32_t tid, const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
            const std::vector<uint64_t>& ts_dimensions, uint64_t ts, const std::string& value,
            const google::protobuf::RepeatedPtrField<::rtidb::nameserver::TablePartition>& table_partition) {
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient>> clients;
    for (auto iter = dimensions.begin(); iter != dimensions.end(); iter++) {
        uint32_t pid = iter->first;
        std::string endpoint;
        for (const auto& cur_table_partition : table_partition) {
            if (cur_table_partition.pid() != pid) {
                continue;
            }
            for (int inner_idx = 0; inner_idx < cur_table_partition.partition_meta_size(); inner_idx++) {
                if (cur_table_partition.partition_meta(inner_idx).is_leader() &&
                    cur_table_partition.partition_meta(inner_idx).is_alive()) {
                    endpoint = cur_table_partition.partition_meta(inner_idx).endpoint();
                    break;
                }
            }
            break;
        }
        if (endpoint.empty()) {
            printf("put error. cannot find healthy endpoint. pid is %u\n", pid);
            return -1;
        }
        if (clients.find(endpoint) == clients.end()) {
            clients.insert(std::make_pair(endpoint, std::make_shared<::rtidb::client::TabletClient>(endpoint)));
            if (clients[endpoint]->Init() < 0) {
                printf("tablet client init failed, endpoint is %s\n", endpoint.c_str());
                return -1;
            }
        }
        if (ts_dimensions.empty()) {
            if (!clients[endpoint]->Put(tid, pid, ts, value, iter->second)) {
                printf("put failed. tid %u pid %u endpoint %s\n", tid, pid, endpoint.c_str());
                return -1;
            }
        } else {
            if (!clients[endpoint]->Put(tid, pid, iter->second, ts_dimensions, value)) {
                printf("put failed. tid %u pid %u endpoint %s\n", tid, pid, endpoint.c_str());
                return -1;
            }
        }
    }
    std::cout << "Put ok" << std::endl;
    return 0;
}
int SetDimensionData(const std::map<std::string, std::string>& raw_data,
                     const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnKey>& column_key_field,
                     uint32_t pid_num,
                     std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions) {
    uint32_t dimension_idx = 0;
    std::set<std::string> index_name_set;
    for (const auto& column_key : column_key_field) {
        std::string index_name = column_key.index_name();
        if (index_name_set.find(index_name) != index_name_set.end()) {
            continue;
        }
        index_name_set.insert(index_name);
        std::string key;
        for (int i = 0; i < column_key.col_name_size(); i++) {
            auto pos = raw_data.find(column_key.col_name(i));
            if (pos == raw_data.end()) {
                return -1;
            }
            if (!key.empty()) {
                key += "|";
            }
            key += pos->second;
        }
        if (key.empty()) {
            auto pos = raw_data.find(index_name);
            if (pos == raw_data.end()) {
                return -1;
            }
            key = pos->second;
        }
        uint32_t pid = 0;
        if (pid_num > 0) {
            pid = (uint32_t)(::rtidb::base::hash64(key) % pid_num);
        }
        if (dimensions.find(pid) == dimensions.end()) {
            dimensions.insert(std::make_pair(pid, std::vector<std::pair<std::string, uint32_t>>()));
        }
        dimensions[pid].push_back(std::make_pair(key, dimension_idx));
        dimension_idx++;
    }
    return 0;
}
int EncodeMultiDimensionData(const std::vector<std::string>& data,
                             const std::vector<::rtidb::base::ColumnDesc>& columns,
                             uint32_t pid_num,
                             std::string& value,
                             std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
                             std::vector<uint64_t>& ts_dimensions,
                             int modify_times) {
    if (data.size() != columns.size()) {
        return -1;
    }
    uint8_t cnt = (uint8_t)data.size();
    ::rtidb::base::FlatArrayCodec codec;
    if (modify_times == 0) {
        ::rtidb::base::FlatArrayCodec codec_tmp(&value, cnt);
        codec = codec_tmp;
    } else {
        ::rtidb::base::FlatArrayCodec codec_tmp(&value, cnt, modify_times);
        codec = codec_tmp;
    }
    uint32_t idx_cnt = 0;
    for (uint32_t i = 0; i < data.size(); i++) {
        if (columns[i].add_ts_idx) {
            uint32_t pid = 0;
            if (pid_num > 0) {
                pid = (uint32_t)(::rtidb::base::hash64(data[i]) % pid_num);
            }
            if (dimensions.find(pid) == dimensions.end()) {
                dimensions.insert(std::make_pair(pid, std::vector<std::pair<std::string, uint32_t>>()));
            }
            dimensions[pid].push_back(std::make_pair(data[i], idx_cnt));
            idx_cnt ++;
        }
        bool codec_ok = false;
        try {
            if (columns[i].is_ts_col) {
                ts_dimensions.push_back(boost::lexical_cast<uint64_t>(data[i]));
            }
            if (columns[i].type == ::rtidb::base::ColType::kInt32) {
                codec_ok = codec.Append(boost::lexical_cast<int32_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kInt64) {
                codec_ok = codec.Append(boost::lexical_cast<int64_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kUInt32) {
                if (!boost::algorithm::starts_with(data[i], "-")) {
                    codec_ok = codec.Append(boost::lexical_cast<uint32_t>(data[i]));
                }
            } else if (columns[i].type == ::rtidb::base::ColType::kUInt64) {
                if (!boost::algorithm::starts_with(data[i], "-")) {
                    codec_ok = codec.Append(boost::lexical_cast<uint64_t>(data[i]));
                }
            } else if (columns[i].type == ::rtidb::base::ColType::kFloat) {
                codec_ok = codec.Append(boost::lexical_cast<float>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kDouble) {
                codec_ok = codec.Append(boost::lexical_cast<double>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kString) {
                codec_ok = codec.Append(data[i]);
            } else if (columns[i].type == ::rtidb::base::ColType::kTimestamp) {
                codec_ok = codec.AppendTimestamp(boost::lexical_cast<uint64_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kDate) {
                std::string date = data[i] + " 00:00:00";
                tm tm_s;
                time_t time;
                char buf[20]= {0};
                strcpy(buf, date.c_str());
                char* result = strptime(buf, "%Y-%m-%d %H:%M:%S", &tm_s);
                if (result == NULL) {
                    printf("date format is YY-MM-DD. ex: 2018-06-01\n");
                    return -1;
                }
                tm_s.tm_isdst = -1;
                time = mktime(&tm_s) * 1000;
                codec_ok = codec.AppendDate(uint64_t(time));
            } else if (columns[i].type == ::rtidb::base::ColType::kInt16) {
                codec_ok = codec.Append(boost::lexical_cast<int16_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kUInt16) {
                codec_ok = codec.Append(boost::lexical_cast<uint16_t>(data[i]));
            } else if (columns[i].type == ::rtidb::base::ColType::kBool) {
                bool value = false;
                std::string raw_value = data[i];
                std::transform(raw_value.begin(), raw_value.end(), raw_value.begin(), ::tolower);
                if (raw_value == "true") {
                    value = true;
                } else if (raw_value == "false") {
                    value = false;
                } else {
                    return -1;
                }
                codec_ok = codec.Append(value);
            } else {
                codec_ok = codec.AppendNull();
            }
        } catch(std::exception const& e) {
            std::cout << e.what() << std::endl;
            return -1;
        }
        if (!codec_ok) {
            return -1;
        }
    }
    codec.Build();
    return 0;
}

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

std::map<std::string, GetColumn> RtidbNSClient::Get(const std::string& name, struct ReadOption& ro) {
    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    std::map<std::string, GetColumn> result;
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
            std::cerr << "convert table column desc failed" << std::endl;
            return result;
        }
    } else {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cerr << "convert table column desc failed" << std::endl;
            return result;
        }
    }
    rtidb::client::TabletClient tablet_client(tablet_endpoint);
    int code = tablet_client.Init();
    if (code < 0) {
        std::cerr << "failed init table client" << std::endl;
        return result;
    }
    for (const auto& iter : ro.index) {
        std::string value;
        uint64_t ts = 0;
        // TODO: current server do not support multi dimesnions
        ok = tablet_client.Get(tables[0].tid(), 0, iter.second, 0, "", "", value, ts, msg);
        if (!ok) {
            std::cerr << "failed to get index " << iter.first << " " << iter.second << " value " << std::endl;
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
        for (int i = 0; i < columns.size(); i++) {
            std::string value = "";
            if (i < values.size()) {
                value = values[i];
            }
            GetColumn col;
            col.type = columns[i].type;
            col.buffer = value;
            result.insert(std::make_pair(columns[i].name, col));
        }
        // TODO: current server do not support multi dimesions
        break;
    }
    return result;
};

bool RtidbNSClient::Put(const std::string& name, const std::map<std::string, std::string>& value, const WriteOption& ro) {
    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ok = client_->ShowTable(name, tables, msg);
    if (!ok) {
        std::cerr << "get table failed, error msg: " << msg << std::endl;
        return false;
    }
    if (tables.empty()) {
        std::cerr << "failed to get table info, error msg: " << msg << std::endl;
        return false;
    }
    if (tables[0].table_type() != rtidb::type::TableType::kRelational) {
        std::cerr << "not support is not relation table" << std::endl;
        return false;
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
        return false;
    }
    auto column_descs = tables[0].column_desc_v1();
    auto add_column_descs = tables[0].added_column_desc();
    if (value.size() - column_descs.size() - add_column_descs.size() != 0) {
        std::cerr << "lost field" << std::endl;
        return false;
    }
    for (int i = 0;i < add_column_descs.size();i++) {
        column_descs.Add()->CopyFrom(add_column_descs.Get(i));
    }
    std::vector<::rtidb::base::ColumnDesc> columns;
    if (rtidb::base::SchemaCodec::ConvertColumnDesc(column_descs, columns) < 0) {
        std::cerr << "conmver table column desc failed!" << std::endl;
    }
    std::string buffer;
    std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions;
    rtidb::client::TabletClient tablet_client(tablet_endpoint);
    std::vector<std::string> values;
    std::vector<uint64_t> ts_dimensions;
    std::set<std::string> keys;
    for (auto& key : tables[0].column_key()) {
        for (auto& col : key.col_name()) {
            keys.insert(col);
        }
        std::map<std::string, std::string> raw_value;
        for (auto& column : columns) {
            auto iter = value.find(column.name);
            if (iter == value.end()) {
                std::cerr << column.name << " not found, put error" << std::endl;
                return false;
            }
            values.push_back(iter->second);
            auto set_iter = keys.find(column.name);
            if (set_iter != keys.end()) {
                raw_value.insert(std::make_pair(column.name, iter->second));
            }
        }
        int code = EncodeMultiDimensionData(values, columns, tables[0].table_partition_size(), buffer, dimensions, ts_dimensions, add_column_descs.size());
        if (code < 0) {
            std::cerr << "encode data error" << std::endl;
            return false;
        }
        if (keys.size() > 0) {
            int code = SetDimensionData(raw_value, tables[0].column_key(), tables[0].table_partition_size(), dimensions);
            if (code < 0) {
                std::cerr << "set dimension data error" << std::endl;
                return false;
            }
        }
        std::string actual_value = buffer;
        if (tables[0].compress_type() == rtidb::nameserver::kSnappy) {
            std::string compressed;
            snappy::Compress(actual_value.c_str(), actual_value.length(), &compressed);
            actual_value = compressed;
        }
        code = PutData(tables[0].tid(), dimensions, ts_dimensions, 0, actual_value, tables[0].table_partition());
        if (code < 0) {
            std::cerr << "put data error" << std::endl;
            return false;
        }
        keys.clear();
    }
    return true;
}

bool RtidbNSClient::Delete(const std::string& name, const std::map<std::string, std::string>& values) {

    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    bool ok = client_->ShowTable(name, tables, msg);
    if (!ok) {
        std::cerr << "get table failed, error msg: " << msg << std::endl;
        return false;
    }
    if (tables.empty()) {
        std::cerr << "failed to get table info, error msg: " << msg << std::endl;
        return false;
    }
    if (tables[0].table_type() != rtidb::type::TableType::kRelational) {
        std::cerr << "not support is not relation table" << std::endl;
        return false;
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
        return false;
    }
    rtidb::client::TabletClient tablet(tablet_endpoint);
    int code = tablet.Init();
    if (code < 0) {
        std::cerr << "init table client failed!" << std::endl;
        return false;
    }
    msg.clear();
    for (auto& iter : values) {
        ok = tablet.Delete(tables[0].tid(), 0, iter.second, iter.first, msg);
        if (!ok) {
            std::cerr << "delete " << iter.first << " " << iter.second << std::endl;
            return false;
        } else {
            std::cout << "delete ok " << iter.second << std::endl;
        }
        msg.clear();
    }
    return true;
}

bool RtidbNSClient::Update(const std::string& name, const std::map<std::string, std::string>& condition, const std::map<std::string, std::string> value) {
    return true;
    /*
    std::vector<rtidb::nameserver::TableInfo> tables;
    std::string msg;
    std::map<std::string, GetColumn> result;
    bool ok = client_->ShowTable(name, tables, msg);
    if (!ok) {
        std::cerr << "get table failed, error msg: " << msg << std::endl;
        return false;
    }
    if (tables.empty()) {
        std::cerr << "failed to get table info, error msg: " << msg << std::endl;
    }
    if (tables[0].table_type() != rtidb::type::TableType::kRelational) {
        std::cerr << "not support is not relation table" << std::endl;
        return false;
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
        return false;
    }
    if (tables[0].added_column_desc_size() > 0) {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns, tables[0].added_column_desc_size()) < 0) {
            std::cerr << "convert table column desc failed" << std::endl;
            return result;
        }
    } else {
        if (::rtidb::base::SchemaCodec::ConvertColumnDesc(tables[0], columns) < 0) {
            std::cerr << "convert table column desc failed" << std::endl;
            return result;
        }
    }
    rtidb::client::TabletClient tablet_client(tablet_endpoint);
    int code = tablet_client.Init();
    if (code < 0) {
        std::cerr << "failed init table client" << std::endl;
        return result;
    }
    for (const auto& iter : ro.index) {
        std::string value;
        uint64_t ts = 0;
        // TODO: current server do not support multi dimesnions
        ok = tablet_client.Get(tables[0].tid(), 0, iter.second, 0, "", "", value, ts, msg);
        if (!ok) {
            std::cerr << "failed to get index " << iter.first << " " << iter.second << " value " << std::endl;
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
        for (int i = 0; i < columns.size(); i++) {
            std::string value = "";
            if (i < values.size()) {
                value = values[i];
            }
            auto& iter = condition.find(columns[i].name);
            if (iter == condition.end()) {
                continue;
            }
            GetColumn col;
            col.type = columns[i].type;
            col.buffer = value;
            result.insert(std::make_pair(columns[i].name, col));
        }
        // TODO: current server do not support multi dimesions
        break;
    }
    std::vector<rtidb::base::ColumnDesc> columns;
    return false;
     */
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
