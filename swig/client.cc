#include "client.h"
#include "base/flat_array.h"
#include "base/codec.h"
#include <boost/lexical_cast.hpp>
#include "base/hash.h"
#include <boost/algorithm/string.hpp>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

const std::map<std::string, rtidb::type::DataType> NameToDataType = {
        {"string", rtidb::type::DataType::kVarchar},
        {"int16", rtidb::type::DataType::kInt16},
        {"int32", rtidb::type::DataType::kInt32},
        {"int64", rtidb::type::DataType::kInt64},
        {"float", rtidb::type::DataType::kFloat},
        {"double", rtidb::type::DataType::kDouble},
        {"date", rtidb::type::DataType::kDate},
        {"timestamp", rtidb::type::DataType::kTimestamp}
};

static int ConvertColumnDesc(
        const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& column_desc_field,
        google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>& columns,
        const google::protobuf::RepeatedPtrField<::rtidb::common::ColumnDesc>& added_column_field) {
    columns.Clear();
    for (const auto& cur_column_desc : column_desc_field) {
        rtidb::common::ColumnDesc* column_desc = columns.Add();
        column_desc->CopyFrom(cur_column_desc);
        if (!cur_column_desc.has_data_type()) {
            auto iter = NameToDataType.find(cur_column_desc.type());
            if (iter == NameToDataType.end()) {
                column_desc->set_data_type(rtidb::type::DataType::kVoid);
            } else {
                column_desc->set_data_type(iter->second);
            }
        }
    }
    for (const auto& cur_column_desc : added_column_field) {
        rtidb::common::ColumnDesc* column_desc = columns.Add();
        column_desc->CopyFrom(cur_column_desc);
        if (!cur_column_desc.has_data_type()) {
            auto iter = NameToDataType.find(cur_column_desc.type());
            if (iter == NameToDataType.end()) {
                column_desc->set_data_type(rtidb::type::DataType::kVoid);
            } else {
                column_desc->set_data_type(iter->second);
            }
        }
    }
    return 0;
}

int PutData(uint32_t tid, const std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>>& dimensions,
            const std::vector<uint64_t>& ts_dimensions, uint64_t ts, const std::string& value,
            const google::protobuf::RepeatedPtrField<::rtidb::nameserver::TablePartition>& table_partition,
            std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>>& clients) {
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

RtidbClient::RtidbClient():zk_client_(), client_(), tablets_(), mu_(), zk_cluster_(), zk_path_(), tables_() {
}

RtidbClient::~RtidbClient() {

}

void RtidbClient::CheckZkClient() {
    if (!zk_client_->IsConnected())  {
        std::cout << "reconnect zk" << std::endl;
        if (zk_client_->Reconnect()) {
            std::cout << "reconnect zk ok" << std::endl;
        }
    }
}

void RtidbClient::UpdateEndpoint(const std::set<std::string>& alive_endpoints) {
    std::lock_guard<std::mutex> mx(mu_);
    decltype(tablets_) old_tablets = tablets_;
    decltype(tablets_) new_tablets;
    for (const auto& endpoint : alive_endpoints) {
        auto iter = tablets_.find(endpoint);
        if (iter == tablets_.end()) {
            std::shared_ptr<rtidb::client::TabletClient> tablet = std::make_shared<rtidb::client::TabletClient>(endpoint);
            if (tablet->Init() != 0) {
                std::cerr << endpoint << " initial failed!" << std::endl;
                continue;
            }
            new_tablets.insert(std::make_pair(endpoint, tablet));
        } else {
            new_tablets.insert(std::make_pair(endpoint, iter->second));
        }
    }

    old_tablets.clear();
    tablets_ = new_tablets;
}

bool RtidbClient::RefreshNodeList() {
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        return false;
    }
    std::set<std::string> endpoint_set;
    for (const auto& endpoint : endpoints) {
        endpoint_set.insert(endpoint);
    }
    UpdateEndpoint(endpoint_set);
    return true;
}

void RtidbClient::RefreshTable() {
    std::vector<std::string> table_vec;
    if (!zk_client_->GetChildren(zk_table_data_path_, table_vec)) {
        if (zk_client_->IsExistNode(zk_path_)) {
            return;
        }
    }

    std::lock_guard<std::mutex> mx(mu_);
    decltype(tables_) old_tables = tables_;
    decltype(tables_) new_tables;
    for (const auto& table_name : table_vec) {
        std::string value;
        std::string table_node = zk_table_data_path_ + "/" + table_name;
        if (!zk_client_->GetNodeValue(table_node, value)) {
            std::cerr << "get table info failed! name " << table_name <<  " table node " << table_node << std::endl;
            continue;
        }
        std::shared_ptr<rtidb::nameserver::TableInfo> table_info = std::make_shared<rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            std::cerr << "parse table info failed! name " << table_name << std::endl;
            continue;
        }
        if (table_info->table_type() != rtidb::type::TableType::kRelational) {
            continue;
        }
        std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>
        columns = std::make_shared<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>();
        ConvertColumnDesc(table_info->column_desc_v1(), *columns, table_info->added_column_desc());
        TableHandler handler;
        handler.table_info = table_info;
        handler.columns = columns;
        new_tables.insert(std::make_pair(table_name, handler));
    }
    old_tables.clear();
    tables_ = new_tables;
    return;
}

GeneralResult RtidbClient::Init(const std::string& zk_cluster, const std::string& zk_path) {
    GeneralResult result;
    std::string value;
    std::shared_ptr<rtidb::zk::ZkClient> zk_client;
    if (!zk_cluster.empty()) {
        zk_client = std::make_shared<rtidb::zk::ZkClient>(zk_cluster, 1000, "", zk_path);
        if (!zk_client->Init()) {
            result.SetError(-1, "zk client init failed");
            return result;
        }
    } else {
        result.SetError(-1, "initial failed! not set zk_cluster");
        return result;
    }

    zk_client_ = zk_client;
    zk_cluster_ = zk_cluster;
    zk_path_ = zk_path;
    zk_table_data_path_ = zk_path + "/table/table_data";
    RefreshNodeList();
    RefreshTable();
    zk_client_->WatchChildren(zk_path + "/notify", boost::bind(&RtidbClient::DoFresh, this, _1));
    return result;
}

QueryResult RtidbClient::Query(const std::string& name, struct ReadOption& ro) {
    QueryResult result;
    return result;
}

std::shared_ptr<rtidb::client::TabletClient> RtidbClient::GetTabletClient(const std::string& endpoint, std::string& msg) {
    std::lock_guard<std::mutex> mx(mu_);
    auto iter = tablets_.find(endpoint);
    if (iter != tablets_.end()) {
        return iter->second;
    }
    std::shared_ptr<rtidb::client::TabletClient> tablet = std::make_shared<rtidb::client::TabletClient>(endpoint);
    int code = tablet->Init();
    if (code < 0) {
        msg = "failed init table client";
        return NULL;
    }
    tablets_.insert(std::make_pair(endpoint, tablet));
    return tablet;
}
// TODO: feature not complete
GeneralResult RtidbClient::Put(const std::string& name, const std::map<std::string, std::string>& value, const WriteOption& wo) {
    GeneralResult result;
    std::shared_ptr<TableHandler> th;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tables_.find(name);
        if (iter == tables_.end()) {
            result.SetError(-1, "table not found");
            return result;
        }
        th = iter->second;
    }

    if (value.size() != th->columns->size()) {
        result.SetError(-1, "value size not equal columns size");
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
        result.SetError(-1, "failed to get table server endpoint");
        return result;
    }
    std::shared_ptr<rtidb::client::TabletClient> tablet = GetTabletClient(tablet_endpoint, msg);
    if (tablet == NULL) {
        result.SetError(-1, msg);
        return result;
    }
    std::set<std::string> keys;
    for (auto& key : tables[0].column_key()) {
        for (auto &col : key.col_name()) {
            keys.insert(col);
        }
    }
    std::vector<std::string> values;
    std::uint32_t string_length = 0;
    std::map<std::string, std::string> raw_value;
    for (auto& column : columns) {
        auto iter = value.find(column.name());
        if (iter == value.end()) {
            result.SetError(-1, column.name() + " not found, put error");
            return result;
        }
        values.push_back(iter->second);
        string_length += iter->second.size();
        auto set_iter = keys.find(column.name());
        if (set_iter != keys.end()) {
            raw_value.insert(std::make_pair(column.name(), iter->second));
        }
    }
    rtidb::base::RowBuilder rb(columns);
    uint32_t total_size = rb.CalTotalLength(string_length);
    std::string buffer;
    buffer.resize(total_size);
    rb.SetBuffer(reinterpret_cast<int8_t*>(&buffer[0]), total_size);
    for (int32_t i = 0; i < columns.size(); i++) {
        switch (columns.Get(i).data_type()) {
            case rtidb::type::kInt32:
                rb.AppendInt32(boost::lexical_cast<int32_t>(values[i]));
                break;
            case rtidb::type::kTimestamp:
                rb.AppendTimestamp(boost::lexical_cast<int64_t>(values[i]));
                break;
            case rtidb::type::kInt64:
                rb.AppendInt64(boost::lexical_cast<int64_t>(values[i]));
                break;
            case rtidb::type::kBool:
                rb.AppendBool(boost::lexical_cast<bool>(values[i]));
                break;
            case rtidb::type::kFloat:
                rb.AppendFloat(boost::lexical_cast<float>(values[i]));
                break;
            case rtidb::type::kInt16:
                rb.AppendInt16(boost::lexical_cast<int16_t>(values[i]));
                break;
            case rtidb::type::kDouble:
                rb.AppendDouble(boost::lexical_cast<double>(values[i]));
                break;
            case rtidb::type::kVarchar:
                rb.AppendString(values[i].data(), values[i].size());
                break;
            case rtidb::type::kDate:
                rb.AppendNULL();
                break;
            case rtidb::type::kVoid:
                rb.AppendNULL();
                break;
            default:
                rb.AppendNULL();
        }
    }
    std::string actual_value = buffer;
    if (tables[0].compress_type() == rtidb::nameserver::kSnappy) {
        std::string compressed;
        snappy::Compress(buffer.data(), buffer.size(), &compressed);
        actual_value = compressed;
    }
    std::map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> dimensions;
    std::vector<uint64_t> ts_dimensions;
    if (keys.size() > 0) {
        int code = SetDimensionData(raw_value, tables[0].column_key(), tables[0].table_partition_size(), dimensions);
        if (code < 0) {
            result.SetError(-1, "set dimesion data error");
            return result;
        }
    }
    int code = 0;
    {
        std::lock_guard<std::mutex> mx(mu_);
        code = PutData(tables[0].tid(), dimensions, ts_dimensions, 0, actual_value, tables[0].table_partition(), tablets_);
    }
    if (code < 0) {
        result.SetError(-1, msg);
        return result;
    }
    return result;
}

GeneralResult RtidbClient::Delete(const std::string& name, const std::map<std::string, std::string>& values) {
    GeneralResult result;
    return result;
}

GeneralResult RtidbClient::Update(const std::string& name, const std::map<std::string, std::string>& condition, const std::map<std::string, std::string> value, const WriteOption& wo) {
    GeneralResult result;
    return result;
}


