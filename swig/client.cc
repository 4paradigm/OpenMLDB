#include "client.h"
#include "base/flat_array.h"
#include <boost/lexical_cast.hpp>
#include "base/hash.h"
#include <boost/algorithm/string.hpp>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

#define NONETOKEN "None#*@!"

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


int Encode(google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>& schema,
        const std::vector<std::string>& value_vec, uint32_t string_length, std::string& buffer) {
    if (value_vec.size() != schema.size()) {
        return -1;
    }
    rtidb::base::RowBuilder rb(schema);
    uint32_t total_size = rb.CalTotalLength(string_length);
    buffer.clear();
    buffer.resize(total_size);
    rb.SetBuffer(reinterpret_cast<int8_t*>(&buffer[0]), total_size);
    for (int32_t i = 0; i < schema.size(); i++) {
        if (value_vec[i] == NONETOKEN) {
            rb.AppendNULL();
            continue;
        }
        auto type = schema.Get(i).data_type();
        try {
            if (type == rtidb::type::kInt32) {
                rb.AppendInt32(boost::lexical_cast<int32_t>(value_vec[i]));
            } else if (type == rtidb::type::kTimestamp) {
                rb.AppendTimestamp(boost::lexical_cast<int64_t>(value_vec[i]));
            } else if (type == rtidb::type::kInt64) {
                rb.AppendInt64(boost::lexical_cast<int64_t>(value_vec[i]));
            } else if (type == rtidb::type::kBool) {
                rb.AppendBool(boost::lexical_cast<bool>(value_vec[i]));
            } else if (type == rtidb::type::kFloat) {
                rb.AppendFloat(boost::lexical_cast<float>(value_vec[i]));
            } else if (type == rtidb::type::kInt16) {
                rb.AppendInt16(boost::lexical_cast<int16_t>(value_vec[i]));
            } else if (type == rtidb::type::kDouble) {
                rb.AppendDouble(boost::lexical_cast<double>(value_vec[i]));
            } else if (type == rtidb::type::kVarchar) {
                rb.AppendString(value_vec[i].data(), value_vec[i].size());
            } else if (type == rtidb::type::kDate) {
                rb.AppendNULL();
            } else {
                return -1;
            }
        } catch (boost::bad_lexical_cast &) {
            return -1;
        }
    }
    return 0;
}

void Decode(google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>& schema, const std::string& value,
        std::vector<std::string>& value_vec) {
    rtidb::base::RowView rv(schema, reinterpret_cast<int8_t*>(const_cast<char*>(&value[0])), value.size());
    for (int32_t i = 0; i < schema.size(); i++) {
        if (rv.IsNULL(i)) {
            value_vec.push_back(NONETOKEN);
            continue;
        }
        std::string col = "";
        auto type = schema.Get(i).data_type();
        if (type == rtidb::type::kInt32) {
            int32_t val;
            int ret = rv.GetInt32(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kTimestamp) {
            int64_t val;
            int ret = rv.GetTimestamp(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kInt64) {
            int64_t val;
            int ret = rv.GetInt64(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kBool) {
            bool val;
            int ret = rv.GetBool(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kFloat) {
            float val;
            int ret = rv.GetFloat(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kInt16) {
            int16_t val;
            int ret = rv.GetInt16(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kDouble) {
            double val;
            int ret = rv.GetDouble(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kVarchar) {
            char *ch = NULL;
            uint32_t length = 0;
            int ret = rv.GetString(i, &ch, &length);
            if (ret == 0) {
                col.assign(ch, length);
            }
        }
        value_vec.push_back(col);
    }
}

RtidbClient::RtidbClient():zk_client_(), client_(), tablets_(), mu_(), zk_cluster_(), zk_path_(), tables_() {
}

RtidbClient::~RtidbClient() {
}

RowViewResult RtidbClient::GetRowView(const std::string &name) {
    RowViewResult result;
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
    result.SetRV(*(th->columns));
    return result;
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
    decltype(tablets_) old_tablets;
    decltype(tablets_) new_tablets;
    {
        std::lock_guard<std::mutex> mx(mu_);
        old_tablets = tablets_;
    }
    for (const auto& endpoint : alive_endpoints) {
        auto iter = old_tablets.find(endpoint);
        if (iter == old_tablets.end()) {
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
    std::lock_guard<std::mutex> mx(mu_);
    tablets_.clear();
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
        std::shared_ptr<TableHandler> handler = std::make_shared<TableHandler>();
        handler->partition.resize(table_info->partition_num());
        int id = 0;
        for (const auto& part : table_info->table_partition()) {
            for (const auto& meta : part.partition_meta()) {
                if (meta.is_leader() && meta.is_alive()) {
                    handler->partition[id].leader = meta.endpoint();
                } else {
                    handler->partition[id].follower.push_back(meta.endpoint());
                }
            }
            id++;
        }
        handler->table_info = table_info;
        handler->columns = columns;
        new_tables.insert(std::make_pair(table_name, handler));
    }
    std::lock_guard<std::mutex> mx(mu_);
    tables_ = new_tables;
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
    bool ok = zk_client_->WatchChildren(zk_path + "/table/notify", boost::bind(&RtidbClient::DoFresh, this, _1));
    if (!ok) {
        zk_client_->CloseZK();
        zk_client_.reset();
    }
    return result;
}

QueryResult RtidbClient::Query(const std::string& name, struct ReadOption& ro) {
    QueryResult result;
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
    std::string err_msg;
    auto tablet = GetTabletClient(th->partition[0].leader, err_msg);
    if (tablet == NULL) {
        result.SetError(-1, err_msg);
        return result;
    }
    for (const auto& iter : ro.index) {
        std::string value;
        uint64_t ts;
        bool ok = tablet->Get(th->table_info->tid(), 0, iter.second, 0, "", "", value, ts, err_msg);
        if (!ok) {
            continue;
        }
        std::vector<std::string> value_vec;
        Decode(*th->columns, value, value_vec);
        std::map<std::string, GetColumn> value_map;
        for (int32_t i = 0; i < th->columns->size(); i++) {
            GetColumn col;
            col.type = th->columns->Get(i).data_type();
            col.buffer = value_vec[i];
            value_map.insert(std::make_pair(th->columns->Get(i).name(), col));
        }
        result.values.push_back(value_map);
    }
    return result;
}

RowViewResult RtidbClient::QueryRaw(const std::string& name, struct ReadOption& ro) {
    RowViewResult result;
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
    std::string err_msg;
    auto tablet = GetTabletClient(th->partition[0].leader, err_msg);
    if (tablet == NULL) {
        result.SetError(-1, err_msg);
        return result;
    }
    for (const auto& iter : ro.index) {
        std::string value;
        uint64_t ts;
        bool ok = tablet->Get(th->table_info->tid(), 0, iter.second, 0, "", "", value, ts, err_msg);
        if (!ok) {
            continue;
        }
        result.values.push_back(value);
    }
    result.SetRV(*th->columns);
    return result;
}

std::shared_ptr<rtidb::client::TabletClient> RtidbClient::GetTabletClient(const std::string& endpoint, std::string& msg) {
    {
        std::lock_guard<std::mutex> mx(mu_);
        auto iter = tablets_.find(endpoint);
        if (iter != tablets_.end()) {
            return iter->second;
        }
    }
    std::shared_ptr<rtidb::client::TabletClient> tablet = std::make_shared<rtidb::client::TabletClient>(endpoint);
    int code = tablet->Init();
    if (code < 0) {
        msg = "failed init table client";
        return NULL;
    }
    {
        std::lock_guard<std::mutex> mx(mu_);
        tablets_.insert(std::make_pair(endpoint, tablet));
    }
    return tablet;
}

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

    std::set<std::string> keys_column;
    for (auto& key : th->table_info->column_key()) {
        for (auto &col : key.col_name()) {
            if (value.find(col) == value.end()) {
                result.SetError(-1, "key col must have ");
                return result;
            }
            keys_column.insert(col);
        }
    }
    std::vector<std::string> value_vec;
    std::uint32_t string_length = 0;
    for (auto& column : *(th->columns)) {
        auto iter = value.find(column.name());
        auto set_iter = keys_column.find(column.name());
        if (iter == value.end()) {
            if (set_iter == keys_column.end()) {
                std::string err_msg = "miss column " + column.name();
                result.SetError(-1, err_msg);
            }
            value_vec.push_back("");
            // TODO: add auto gen key columm
        } else {
            value_vec.push_back(iter->second);
            if ((column.data_type() == rtidb::type::kVarchar) && (iter->second != NONETOKEN)) {
                string_length += iter->second.size();
            }
        }
    }
    std::string buffer;
    int code = Encode(*(th->columns), value_vec, string_length, buffer);
    if (code != 0) {
        result.SetError(code, "encode error");
        return result;
    }
    std::string err_msg;
    auto tablet = GetTabletClient(th->partition[0].leader, err_msg);
    if (tablet == NULL) {
        result.SetError(-1, err_msg);
        return result;
    }
    bool ok = tablet->Put(th->table_info->tid(), 0, "", 0, buffer);
    if (!ok) {
        result.SetError(-1, "put error");
        return result;
    }

    return result;
}

GeneralResult RtidbClient::Delete(const std::string& name, const std::map<std::string, std::string>& values) {
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
    std::string msg;
    auto tablet = GetTabletClient(th->partition[0].leader, msg);
    if (tablet == NULL) {
        result.SetError(-1, msg);
        return result;
    }
    auto iter = values.begin();
    bool ok = tablet->Delete(th->table_info->tid(), 0, iter->second, iter->first, msg);
    if (!ok) {
        result.SetError(1, msg);
        return result;
    }
    return result;
}

GeneralResult RtidbClient::Update(const std::string& name, const std::map<std::string, std::string>& condition,
        const std::map<std::string, std::string> values, const WriteOption& wo) {
    GeneralResult result;
    return result;
}

