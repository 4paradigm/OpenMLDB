//
// Copyright 2020 4paradigm
//
#include "client/client.h"
#include <boost/algorithm/string.hpp>
#include "base/flat_array.h"
#include "base/hash.h"
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>
#include <memory>
#include <utility>

int64_t ViewResult::GetInt(uint32_t idx) {
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

void TraverseResult::Init(RtidbClient* client, std::string* table_name,
                          struct ReadOption* ro, uint32_t count) {
    client_ = client;
    table_name_ = table_name;
    ro_ = ro;
    offset_ = 0;
    count_ = count;
}

TraverseResult::~TraverseResult() {
    delete table_name_;
    delete ro_;
}

bool TraverseResult::TraverseNext() {
    bool ok = client_->Traverse(*table_name_, *ro_, value_.get(), &count_,
                                last_pk_, &is_finish_);
    return ok;
}

bool TraverseResult::Next() {
    if (count_ < 1) {
        if (is_finish_) {
            return false;
        }
        last_pk_.clear();
        last_pk_ = GetKey();
        value_->clear();
        bool ok = TraverseNext();
        if (!ok) {
            return ok;
        }
        offset_ = 0;
    }
    const char* buffer = value_->c_str();
    buffer += offset_;
    uint32_t size = 0;
    memcpy(static_cast<void*>(&size), buffer, 4);
    memrev32ifbe(static_cast<void*>(&size));
    buffer += 4;
    bool ok =
        rv_->Reset(reinterpret_cast<int8_t*>(const_cast<char*>(buffer)), size);
    if (ok) {
        offset_ += 4 + size;
    }
    count_--;
    return ok;
}

RtidbClient::RtidbClient()
    : zk_client_(),
      client_(),
      tablets_(),
      mu_(),
      zk_cluster_(),
      zk_path_(),
      tables_(),
      task_thread_pool_(1),
      zk_keep_alive_check_(15000) {}

RtidbClient::~RtidbClient() { task_thread_pool_.Stop(true); }

void RtidbClient::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        std::cout << "reconnect zk" << std::endl;
        if (zk_client_->Reconnect()) {
            std::cout << "reconnect zk ok" << std::endl;
        }
    }
    task_thread_pool_.DelayTask(zk_keep_alive_check_,
                                boost::bind(&RtidbClient::CheckZkClient, this));
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
            std::shared_ptr<rtidb::client::TabletClient> tablet =
                std::make_shared<rtidb::client::TabletClient>(endpoint);
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
        std::string pk_col_name;
        for (const auto& column_key : table_info->column_key()) {
            if (column_key.index_type() == rtidb::type::kPrimaryKey) {
                pk_col_name = column_key.index_name();
                break;
            }
        }
        int pk_index = 0;
        rtidb::type::DataType pk_type = rtidb::type::kVarchar;
        for (int i = 0; i < table_info->column_desc_size(); i++) {
            if (table_info->column_desc_v1(i).name() == pk_col_name) {
                pk_index = i;
                pk_type = table_info->column_desc_v1(i).data_type();
                break;
            }
        }
        if (pk_type_set.find(pk_type) == pk_type_set.end()) {
            continue;
        }
        handler->table_info = table_info;
        handler->columns = columns;
        handler->pk_index = pk_index;
        handler->pk_type = pk_type;
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
    std::shared_ptr<TableHandler> th = GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    auto tablet = GetTabletClient(th->partition[0].leader, &result.msg_);
    if (tablet == NULL) {
        result.code_ = -1;
        return result;
    }
    std::shared_ptr<std::string> value = std::make_shared<std::string>();
    uint64_t ts;
    bool ok = tablet->Get(th->table_info->tid(), 0, ro.index.begin()->second, 0, "", "", *value, ts, result.msg_);
    if (!ok) {
        result.code_ = -1;
        return result;
    }
    result.AddValue(value);
    result.SetRv(th);
    return result;
}

std::shared_ptr<TableHandler> RtidbClient::GetTableHandler(const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = tables_.find(name);
    if (iter == tables_.end()) {
        return NULL;
    }
    return iter->second;
}

TraverseResult RtidbClient::Traverse(const std::string& name, const struct ReadOption& ro) {
    TraverseResult result;
    std::shared_ptr<TableHandler> th = GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }

    uint32_t count = 0;
    std::string* raw_data = new std::string;
    std::string pk = "";
    if (ro.index.size() > 0) {
        pk = ro.index.begin()->second;
    }
    bool is_finish = true;
    bool ok = Traverse(name, ro, raw_data, &count, pk, &is_finish);
    if (!ok) {
        delete raw_data;
        result.code_ = -1;
        result.msg_ = "traverse data error";
        return result;
    }
    std::string* table_name = new std::string(name);
    struct ReadOption* ro_ptr = new ReadOption(ro);
    result.Init(this, table_name, ro_ptr, count);
    result.SetRv(th);
    result.SetValue(raw_data, is_finish);
    return result;
}

bool RtidbClient::Traverse(const std::string& name, const struct ReadOption& ro,
                           std::string* data, uint32_t* count,
                           const std::string& last_key, bool* is_finish) {
    std::shared_ptr<TableHandler> th = GetTableHandler(name);
    if (th == NULL) {
        return false;
    }
    std::string err_msg;
    auto tablet = GetTabletClient(th->partition[0].leader, &err_msg);
    if (tablet == NULL) {
        return false;
    }
    bool ok = tablet->Traverse(th->table_info->tid(), 0, last_key, 1000, count, &err_msg, data, is_finish);
    if (!ok) {
        return false;
    }
    return true;
}

std::shared_ptr<rtidb::client::TabletClient> RtidbClient::GetTabletClient(
    const std::string& endpoint, std::string* msg) {
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
        *msg = "failed init table client";
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

    std::shared_ptr<TableHandler> th = GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    std::set<std::string> keys_column;
    for (auto& key : th->table_info->column_key()) {
        for (auto& col : key.col_name()) {
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
            // TODO(kongquan): add auto gen key columm
        } else {
            value_vec.push_back(iter->second);
            if ((column.data_type() == rtidb::type::kVarchar) && (iter->second != rtidb::base::NONETOKEN)) {
                string_length += iter->second.size();
            }
        }
    }
    std::string buffer;
    int code = rtidb::base::RowSchemaCodec::Encode(*(th->columns), value_vec, string_length, buffer);
    if (code != 0) {
        result.SetError(code, "encode error");
        return result;
    }
    std::string err_msg;
    auto tablet = GetTabletClient(th->partition[0].leader, &err_msg);
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
    std::shared_ptr<TableHandler> th = GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    auto tablet = GetTabletClient(th->partition[0].leader, &result.msg);
    if (tablet == NULL) {
        result.code = -1;
        return result;
    }
    auto iter = values.begin();
    bool ok = tablet->Delete(th->table_info->tid(), 0, iter->second, iter->first, result.msg);
    if (!ok) {
        result.code = -1;
        return result;
    }
    return result;
}

GeneralResult RtidbClient::Update(const std::string& name, const std::map<std::string, std::string>& condition,
        const std::map<std::string, std::string> values, const WriteOption& wo) {
    GeneralResult result;
    return result;
}
