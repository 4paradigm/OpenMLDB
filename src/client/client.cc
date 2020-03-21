#include "client.h"
#include "base/flat_array.h"
#include "base/hash.h"
#include <boost/algorithm/string.hpp>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>
#include <memory>




int64_t QueryResult::GetInt(uint32_t idx) {
    int64_t val = 0;
    auto type = columns_->Get(idx).data_type();
    if (type == rtidb::type::kSmallInt) {
        int16_t st_val;
        rv_->GetInt16(idx, &st_val);
        val = st_val;
    } else if (type == rtidb::type::kInt) {
        int32_t tt_val;
        rv_->GetInt32(idx, &tt_val);
        val = tt_val;
    } else {
        int64_t val;
        rv_->GetInt64(idx, &val);
    }
    return val;
}

std::map<std::string, std::string> QueryResult::DecodeData() {
    std::map<std::string, std::string> result;
    for (int i = 0; i < columns_->size(); i++) {
        std::string col_name = columns_->Get(i).name();
        if (rv_->IsNULL(i)) {
            result.insert(std::make_pair(col_name, rtidb::base::NONETOKEN));
        }
        std::string col = "";
        auto type = columns_->Get(i).data_type();
        if (type == rtidb::type::kInt) {
            int32_t val;
            int ret = rv_->GetInt32(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kTimestamp) {
            int64_t val;
            int ret = rv_->GetTimestamp(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kBigInt) {
            int64_t val;
            int ret = rv_->GetInt64(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kBool) {
            bool val;
            int ret = rv_->GetBool(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kFloat) {
            float val;
            int ret = rv_->GetFloat(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kSmallInt) {
            int16_t val;
            int ret = rv_->GetInt16(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kDouble) {
            double val;
            int ret = rv_->GetDouble(i, &val);
            if (ret == 0) {
                col = std::to_string(val);
            }
        } else if (type == rtidb::type::kVarchar) {
            char *ch = NULL;
            uint32_t length = 0;
            int ret = rv_->GetString(i, &ch, &length);
            if (ret == 0) {
                col.assign(ch, length);
            }
        } else if (type == rtidb::type::kBlob) {
            char *ch = NULL;
            uint32_t length = 0;
            int ret = rv_->GetString(i, &ch, &length);
            if (ret == 0) {
                col.assign(ch, length);
            }
        }
        result.insert(std::make_pair(col_name, col));
    }
    return result;
}

RtidbClient::RtidbClient():zk_client_(), client_(), tablets_(), mu_(), zk_cluster_(), zk_path_(), tables_(), task_thread_pool_(1), zk_keep_alive_check_(15000) {
}

RtidbClient::~RtidbClient() {
    task_thread_pool_.Stop(true);
}

void RtidbClient::CheckZkClient() {
    if (!zk_client_->IsConnected())  {
        std::cout << "reconnect zk" << std::endl;
        if (zk_client_->Reconnect()) {
            std::cout << "reconnect zk ok" << std::endl;
        }
    }
    task_thread_pool_.DelayTask(zk_keep_alive_check_, boost::bind(&RtidbClient::CheckZkClient, this));
}

void RtidbClient::SetZkCheckInterval(int32_t interval) {
    if (interval <= 1000) {
        return;
    }
    zk_keep_alive_check_ = interval;
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
        int code = rtidb::base::RowSchemaCodec::ConvertColumnDesc(table_info->column_desc_v1(), *columns, table_info->added_column_desc());
        if (code != 0) {
            continue;
        }
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
        for (int i = 0; i < table_info->column_key_size(); i++) {
            if (table_info->column_key(i).has_index_type() &&
                table_info->column_key(i).index_type() == ::rtidb::type::IndexType::kAutoGen) {
                handler->auto_gen_pk_ = table_info->column_key(i).index_name();
                break;
            }
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
    task_thread_pool_.DelayTask(zk_keep_alive_check_, boost::bind(&RtidbClient::CheckZkClient, this));
    return result;
}


QueryResult RtidbClient::Query(const std::string& name, const struct ReadOption& ro) {
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
    std::shared_ptr<std::string> value = std::make_shared<std::string>();
    uint64_t ts;
    bool ok = tablet->Get(th->table_info->tid(), 0, ro.index.begin()->second, 0, "", "", *value, ts, err_msg);
    if (!ok) {
        result.SetError(-1, err_msg);
        return result;
    }
    result.AddValue(value);
    result.SetRV(th->columns);
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

GeneralResult RtidbClient::Update(const std::string& table_name, 
        const std::map<std::string, std::string> condition_columns_map, 
        const std::map<std::string, std::string> value_columns_map,
        const WriteOption& wo) {
    GeneralResult result;
    std::shared_ptr<TableHandler> th;
    {
        std::lock_guard<std::mutex> lock(mu_);
        auto iter = tables_.find(table_name);
        if (iter == tables_.end()) {
            result.SetError(-1, "table not found");
            return result;
        }
        th = iter->second;
    }
    if (condition_columns_map.empty() || value_columns_map.empty()) {
        result.SetError(-1, "condition_columns_map or value_columns_map is empty");
        return result;
    }
    auto cd_iter = condition_columns_map.begin();
    std::string pk = cd_iter->second;

    uint32_t tid = th->table_info->tid();
    uint32_t pid = (uint32_t)(::rtidb::base::hash64(pk) % th->table_info->table_partition_size());
    google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc> new_cd_schema;
    ::rtidb::base::RowSchemaCodec::GetSchemaData(condition_columns_map, *(th->columns), new_cd_schema);
    std::string cd_value;
    ::rtidb::base::ResultMsg cd_rm = ::rtidb::base::RowSchemaCodec::Encode(condition_columns_map, new_cd_schema, cd_value);
    if(cd_rm.code < 0) {
        result.SetError(cd_rm.code, "encode error, msg: " + cd_rm.msg);
        return result;
    }
    if (th->table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        std::string compressed;
        ::snappy::Compress(cd_value.c_str(), cd_value.length(), &compressed);
        cd_value = compressed;
    }
    google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>  new_value_schema;
    ::rtidb::base::RowSchemaCodec::GetSchemaData(value_columns_map,*(th->columns), new_value_schema);
    std::string value;
    ::rtidb::base::ResultMsg value_rm = ::rtidb::base::RowSchemaCodec::Encode(value_columns_map, new_value_schema, value);
    if(value_rm.code < 0) {
        result.SetError(value_rm.code, "encode error, msg: " + value_rm.msg);
        return result;
    }
    if (th->table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        std::string compressed;
        ::snappy::Compress(value.c_str(), value.length(), &compressed);
        value = compressed;
    }
    std::string msg;
    auto tablet_client = GetTabletClient(th->partition[0].leader, msg);
    if (tablet_client == NULL) {
        result.SetError(-1, msg);
        return result;
    }
    bool ok = tablet_client->Update(tid, pid, new_cd_schema, new_value_schema, cd_value, value, msg);
    if (!ok) {
        result.SetError(-1, msg);
    }
    return result;
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
            if (value.find(col) == value.end() && 
                    th->auto_gen_pk_.empty()) {
                result.SetError(-1, "key col must have ");
                return result;
            }
            keys_column.insert(col);
        }
    }
    std::map<std::string, std::string> val;
    for (auto& column : *(th->columns)) {
        auto iter = value.find(column.name());
        auto set_iter = keys_column.find(column.name());
        if (iter == value.end()) {
            if (set_iter == keys_column.end()) {
                std::string err_msg = "miss column " + column.name();
                result.SetError(-1, err_msg);
                return result;
            } else if (th->auto_gen_pk_.empty()) {
                result.SetError(-1, "input value error");
                return result;
            } else {
                val = value;
                val.insert(std::make_pair(th->auto_gen_pk_, ::rtidb::base::DEFAULT_LONG));
            }
        } else if (column.name() == th->auto_gen_pk_) {
            result.SetError(-1, "should not input autoGenPk column");
            return result;
        }
    }
    std::string buffer;
    rtidb::base::ResultMsg rm;
    if (!th->auto_gen_pk_.empty()) {
        rm = rtidb::base::RowSchemaCodec::Encode(val, *(th->columns), buffer);
    } else {
        rm = rtidb::base::RowSchemaCodec::Encode(value, *(th->columns), buffer);
    }
    if (rm.code != 0) {
        result.SetError(rm.code, "encode error, msg: " + rm.msg);
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


