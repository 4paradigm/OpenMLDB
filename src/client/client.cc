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
                          struct ReadOption* ro, uint32_t count,
                          uint64_t snapshot_id) {
    client_ = client;
    table_name_.reset(table_name);
    ro_.reset(ro);
    offset_ = 0;
    count_ = count;
    snapshot_id_ = snapshot_id;
}

void BatchQueryResult::Init(RtidbClient* client, std::string* table_name,
                            const std::vector<std::string>& keys,
                            uint32_t count) {
    client_ = client;
    table_name_.reset(table_name);
    keys_ = std::make_shared<std::vector<std::string>>(keys);
    offset_ = 0;
    count_ = count;
}

TraverseResult::~TraverseResult() {}

BatchQueryResult::~BatchQueryResult() {}

bool TraverseResult::TraverseNext() {
    bool ok = client_->Traverse(*table_name_, *ro_, value_.get(), &count_,
                                last_pk_, &is_finish_, &snapshot_id_);
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

bool BatchQueryResult::Next() {
    if (count_ < 1) {
        if (is_finish_) {
            return false;
        }
        value_->clear();
        offset_ = 0;
        std::vector<std::string> get_keys;
        for (const auto key : *keys_) {
            if (already_get_.find(key) != already_get_.end()) {
                continue;
            }
            get_keys.push_back(key);
        }
        if (get_keys.size() == 0) {
            return false;
        }
        value_->clear();
        bool ok = BatchQueryNext(get_keys);
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
    already_get_.insert(GetKey());
    return ok;
}

bool BatchQueryResult::BatchQueryNext(const std::vector<std::string>& get_key) {
    return client_->BatchQuery(*table_name_, get_key, value_.get(), &is_finish_,
                               &count_);
}

bool BaseClient::Init(std::string* msg) {
    zk_client_ = new rtidb::zk::ZkClient(zk_cluster_, zk_session_timeout_,
                                         endpoint_, zk_root_path_);
    if (!zk_client_->Init()) {
        if (!zk_client_->Init()) {
            *msg = "zk client init failed";
            return false;
        }
    }
    zk_table_data_path_ = zk_root_path_ + "/table/table_data";
    RefreshNodeList();
    RefreshTable();
    bool ok =
        zk_client_->WatchChildren(zk_root_path_ + "/table/notify",
                                  boost::bind(&BaseClient::DoFresh, this, _1));
    if (!ok) {
        zk_client_->CloseZK();
        *msg = "zk watch table notify failed";
        return false;
    }
    task_thread_pool_.DelayTask(zk_keep_alive_check_,
                                boost::bind(&BaseClient::CheckZkClient, this));
    return true;
}

void BaseClient::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        std::cout << "reconnect zk" << std::endl;
        if (zk_client_->Reconnect()) {
            std::cout << "reconnect zk ok" << std::endl;
        }
    }
    task_thread_pool_.DelayTask(zk_keep_alive_check_,
                                boost::bind(&BaseClient::CheckZkClient, this));
}

bool BaseClient::RefreshNodeList() {
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        return false;
    }
    std::set<std::string> endpoint_set;
    for (const auto& endpoint : endpoints) {
        endpoint_set.insert(endpoint);
    }
    UpdateEndpoint(endpoint_set);
    endpoints.clear();
    if (!zk_client_->GetChildren(zk_root_path_ + "/ossnodes", endpoints)) {
        return false;
    }
    endpoint_set.clear();
    for (const auto& endpoint : endpoints) {
        endpoint_set.insert(endpoint);
    }
    UpdateBlobEndpoint(endpoint_set);
    return true;
}

void BaseClient::UpdateEndpoint(const std::set<std::string>& alive_endpoints) {
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

void BaseClient::UpdateBlobEndpoint(const std::set<std::string>& alive_endpoints) {
    decltype(blobs_) old_blobs;
    decltype(blobs_) new_blobs;
    {
        std::lock_guard<std::mutex> mx(mu_);
        old_blobs = blobs_;
    }
    for (const auto& endpoint : alive_endpoints) {
        auto iter = old_blobs.find(endpoint);
        if (iter == old_blobs.end()) {
            std::shared_ptr<rtidb::client::BsClient> blob =
                std::make_shared<rtidb::client::BsClient>(endpoint);
        if (blob->Init() != 0) {
            std::cerr << endpoint << " initial failed!" << std::endl;
            continue;
        }
        new_blobs.insert(std::make_pair(endpoint, blob));
        } else {
            new_blobs.insert(std::make_pair(endpoint, iter->second));
        }
    }
    std::lock_guard<std::mutex> mx(mu_);
    blobs_.clear();
    blobs_ = new_blobs;
}

void BaseClient::RefreshTable() {
    std::vector<std::string> table_vec;
    if (!zk_client_->GetChildren(zk_table_data_path_, table_vec)) {
        if (zk_client_->IsExistNode(zk_root_path_)) {
            return;
        }
    }

    decltype(tables_) new_tables;
    for (const auto& table_name : table_vec) {
        std::string value;
        std::string table_node = zk_table_data_path_ + "/" + table_name;
        if (!zk_client_->GetNodeValue(table_node, value)) {
            std::cerr << "get table info failed! name " << table_name
                      << " table node " << table_node << std::endl;
            continue;
        }
        std::shared_ptr<rtidb::nameserver::TableInfo> table_info =
            std::make_shared<rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            std::cerr << "parse table info failed! name " << table_name
                      << std::endl;
            continue;
        }
        rtidb::type::TableType tb = table_info->table_type();
        if (tb != rtidb::type::TableType::kRelational && tb != rtidb::type::TableType::kObjectStore) {
            continue;
        }
        std::shared_ptr<
            google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>
            columns = std::make_shared<google::protobuf::RepeatedPtrField<
                rtidb::common::ColumnDesc>>();
        int code = rtidb::base::RowSchemaCodec::ConvertColumnDesc(
            table_info->column_desc_v1(), *columns,
            table_info->added_column_desc());
        if (code != 0) {
            continue;
        }
        std::shared_ptr<TableHandler> handler =
            std::make_shared<TableHandler>();
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
        for (int i = 0; i < table_info->column_key_size(); i++) {
            if (table_info->column_key(i).has_index_type() &&
                table_info->column_key(i).index_type() ==
                    ::rtidb::type::IndexType::kAutoGen) {
                handler->auto_gen_pk_ = table_info->column_key(i).index_name();
                break;
            }
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

void BaseClient::SetZkCheckInterval(int32_t interval) {
    zk_keep_alive_check_ = interval;
}

void BaseClient::DoFresh(const std::vector<std::string>& events) {
    RefreshNodeList();
    RefreshTable();
}

bool BaseClient::RegisterZK(std::string* msg) {
    if (zk_cluster_.empty()) {
        *msg = "zk cluster is empty";
        return false;
    }
    if (!zk_client_->Register(true)) {
        *msg = "fail to register client with endpoint " + endpoint_;
        return false;
    }
    return true;
}

BaseClient::~BaseClient() {
    task_thread_pool_.Stop(true);
    if (zk_client_ != NULL) {
        delete zk_client_;
    }
}

std::shared_ptr<rtidb::client::TabletClient> BaseClient::GetTabletClient(
    const std::string& endpoint, std::string* msg) {
    {
        std::lock_guard<std::mutex> mx(mu_);
        auto iter = tablets_.find(endpoint);
        if (iter != tablets_.end()) {
            return iter->second;
        }
    }
    std::shared_ptr<rtidb::client::TabletClient> tablet =
        std::make_shared<rtidb::client::TabletClient>(endpoint);
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

std::shared_ptr<rtidb::client::BsClient> BaseClient::GetBlobClient(const std::string& endpoint, std::string* msg) {
    {
        std::lock_guard<std::mutex> mx(mu_);
        auto iter = blobs_.find(endpoint);
        if (iter != blobs_.end()) {
            return iter->second;
        }
    }
    std::shared_ptr<rtidb::client::BsClient> blob = std::make_shared<rtidb::client::BsClient>(endpoint);
    int code = blob->Init();
    if (code < 0) {
        *msg = "failed init blob client";
        return NULL;
    }
    {
        std::lock_guard<std::mutex> mx(mu_);
        blobs_.insert(std::make_pair(endpoint, blob));
    }
    return blob;
}

std::shared_ptr<TableHandler> BaseClient::GetTableHandler(
    const std::string& name) {
    std::lock_guard<std::mutex> lock(mu_);
    auto iter = tables_.find(name);
    if (iter == tables_.end()) {
        return NULL;
    }
    return iter->second;
}

RtidbClient::RtidbClient() : client_(NULL) {}

RtidbClient::~RtidbClient() {
    if (client_ != NULL) {
        delete client_;
    }
}

void RtidbClient::SetZkCheckInterval(int32_t interval) {
    client_->SetZkCheckInterval(interval);
}

GeneralResult RtidbClient::Init(const std::string& zk_cluster,
                                const std::string& zk_path) {
    GeneralResult result;
    std::string value;
    std::shared_ptr<rtidb::zk::ZkClient> zk_client;
    if (zk_cluster.empty()) {
        result.SetError(-1, "initial failed! not set zk_cluster");
        return result;
    }
    client_ = new BaseClient(zk_cluster, zk_path, "", 1000, 15000);

    std::string msg;
    bool ok = client_->Init(&msg);
    if (!ok) {
        result.SetError(-1, msg);
    }
    return result;
}

QueryResult RtidbClient::Query(const std::string& name,
                               const struct ReadOption& ro) {
    QueryResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    auto tablet =
        client_->GetTabletClient(th->partition[0].leader, &result.msg_);
    if (tablet == NULL) {
        result.code_ = -1;
        return result;
    }
    std::shared_ptr<std::string> value = std::make_shared<std::string>();
    uint64_t ts;
    bool ok = tablet->Get(th->table_info->tid(), 0, ro.index.begin()->second, 0,
                          "", "", *value, ts, result.msg_);
    if (!ok) {
        result.code_ = -1;
        return result;
    }
    result.AddValue(value);
    result.SetRv(th);
    return result;
}

TraverseResult RtidbClient::Traverse(const std::string& name,
                                     const struct ReadOption& ro) {
    TraverseResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
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
    uint64_t snapshot_id = 0;
    bool ok =
        Traverse(name, ro, raw_data, &count, pk, &is_finish, &snapshot_id);
    if (!ok) {
        delete raw_data;
        result.code_ = -1;
        result.msg_ = "traverse data error";
        return result;
    }
    std::string* table_name = new std::string(name);
    struct ReadOption* ro_ptr = new ReadOption(ro);
    result.Init(this, table_name, ro_ptr, count, snapshot_id);
    result.SetRv(th);
    result.SetValue(raw_data, is_finish);
    return result;
}

bool RtidbClient::Traverse(const std::string& name, const struct ReadOption& ro,
                           std::string* data, uint32_t* count,
                           const std::string& last_key, bool* is_finish,
                           uint64_t* snapshot_id) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == NULL) {
        return false;
    }
    std::string err_msg;
    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
    if (tablet == NULL) {
        return false;
    }
    bool ok = tablet->Traverse(th->table_info->tid(), 0, last_key, 1000, count,
                               &err_msg, data, is_finish, snapshot_id);
    return ok;
}

GeneralResult RtidbClient::Update(
    const std::string& table_name,
    const std::map<std::string, std::string>& condition_columns_map,
    const std::map<std::string, std::string>& value_columns_map,
    const WriteOption& wo) {
    GeneralResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(table_name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    if (condition_columns_map.empty() || value_columns_map.empty()) {
        result.SetError(-1,
                        "condition_columns_map or value_columns_map is empty");
        return result;
    }
    auto cd_iter = condition_columns_map.begin();
    std::string pk = cd_iter->second;

    uint32_t tid = th->table_info->tid();
    uint32_t pid = (uint32_t)(::rtidb::base::hash64(pk) %
                              th->table_info->table_partition_size());
    google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc> new_cd_schema;
    ::rtidb::base::RowSchemaCodec::GetSchemaData(condition_columns_map,
                                                 *(th->columns), new_cd_schema);
    std::string cd_value;
    ::rtidb::base::ResultMsg cd_rm = ::rtidb::base::RowSchemaCodec::Encode(
        condition_columns_map, new_cd_schema, cd_value);
    if (cd_rm.code < 0) {
        result.SetError(cd_rm.code, "encode error, msg: " + cd_rm.msg);
        return result;
    }
    if (th->table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        std::string compressed;
        ::snappy::Compress(cd_value.c_str(), cd_value.length(), &compressed);
        cd_value = compressed;
    }
    google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>
        new_value_schema;
    ::rtidb::base::RowSchemaCodec::GetSchemaData(
        value_columns_map, *(th->columns), new_value_schema);
    std::string value;
    ::rtidb::base::ResultMsg value_rm = ::rtidb::base::RowSchemaCodec::Encode(
        value_columns_map, new_value_schema, value);
    if (value_rm.code < 0) {
        result.SetError(value_rm.code, "encode error, msg: " + value_rm.msg);
        return result;
    }
    if (th->table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        std::string compressed;
        ::snappy::Compress(value.c_str(), value.length(), &compressed);
        value = compressed;
    }
    std::string msg;
    auto tablet_client =
        client_->GetTabletClient(th->partition[0].leader, &msg);
    if (tablet_client == NULL) {
        result.SetError(-1, msg);
        return result;
    }
    bool ok = tablet_client->Update(tid, pid, new_cd_schema, new_value_schema,
                                    cd_value, value, msg);
    if (!ok) {
        result.SetError(-1, msg);
    }
    return result;
}

GeneralResult RtidbClient::Put(const std::string& name,
                               const std::map<std::string, std::string>& value,
                               const WriteOption& wo) {
    GeneralResult result;

    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    std::set<std::string> keys_column;
    for (auto& key : th->table_info->column_key()) {
        for (auto& col : key.col_name()) {
            if (value.find(col) == value.end() && th->auto_gen_pk_.empty()) {
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
                val.insert(std::make_pair(th->auto_gen_pk_,
                                          ::rtidb::base::DEFAULT_LONG));
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
    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
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

GeneralResult RtidbClient::Delete(
    const std::string& name, const std::map<std::string, std::string>& values) {
    GeneralResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    auto tablet =
        client_->GetTabletClient(th->partition[0].leader, &result.msg);
    if (tablet == NULL) {
        result.code = -1;
        return result;
    }
    auto iter = values.begin();
    bool ok = tablet->Delete(th->table_info->tid(), 0, iter->second,
                             iter->first, result.msg);
    if (!ok) {
        result.code = -1;
        return result;
    }
    return result;
}

BatchQueryResult RtidbClient::BatchQuery(const std::string& name,
                                         const std::vector<ReadOption>& ros) {
    std::vector<std::string> keys;
    for (const auto& it : ros) {
        if (it.index.size() < 1) {
            continue;
        }
        keys.push_back(it.index.begin()->second);
    }
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    BatchQueryResult result;
    if (th == NULL) {
        result.SetError(-1, "table not found");
        return result;
    }
    std::string* data = new std::string();
    bool is_finish;
    uint32_t count;
    bool ok = BatchQuery(name, keys, data, &is_finish, &count);
    if (!ok) {
        delete data;
        result.SetError(-1, "batchquery error");
        return result;
    }
    std::string* table_name = new std::string(name);
    result.Init(this, table_name, keys, count);
    result.SetRv(th);
    result.SetValue(data, is_finish);
    return result;
}

bool RtidbClient::BatchQuery(const std::string& name,
                             const std::vector<std::string>& keys,
                             std::string* data, bool* is_finish,
                             uint32_t* count) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == NULL) {
        return false;
    }
    std::string err_msg;
    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
    if (tablet == NULL) {
        return false;
    }
    return tablet->BatchQuery(th->table_info->tid(), 0, "", keys, &err_msg,
                              data, is_finish, count);
}
