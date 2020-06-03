//
// Copyright 2020 4paradigm
//
#include "client/client.h"

#include <boost/algorithm/string.hpp>

#include "base/hash.h"
#include "codec/flat_array.h"
#include "codec/row_codec.h"
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>

#include <memory>
#include <utility>

#include "base/strings.h"

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

BlobInfoResult ViewResult::GetBlobInfo() {
    if (table_name_.empty() && client_ == nullptr) {
        BlobInfoResult result;
        result.SetError(-1,
                        "do not have table name or rtidb client");
        return result;
    }
    return client_->GetBlobInfo(table_name_);
}

void TraverseResult::Init(RtidbClient* client, std::string* table_name,
                          struct ReadOption* ro, uint32_t count,
                          uint64_t snapshot_id) {
    client_ = client;
    SetClient(client);
    table_name_.reset(table_name);
    ro_.reset(ro);
    offset_ = 0;
    count_ = count;
    snapshot_id_ = snapshot_id;
}

bool TraverseResult::TraverseNext() {
    if (!ro_->index.empty()) ro_->index.clear();
    bool ok = client_->Traverse(*table_name_, *ro_, value_.get(), &count_,
                                &last_pk_, &is_finish_, &snapshot_id_);
    return ok;
}

bool TraverseResult::Next() {
    if (count_ < 1) {
        if (is_finish_) {
            return false;
        }
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
    if (offset_ + 4 >= value_->size()) {
        return false;
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
    return ok;
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
    bool ok = zk_client_->WatchChildren(
        zk_root_path_ + "/table", boost::bind(&BaseClient::DoFresh, this, _1));
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
            RefreshNodeList();
            RefreshTable();
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
    std::set<std::string> tablet_set;
    std::set<std::string> blob_set;
    for (const auto& endpoint : endpoints) {
        if (endpoint.size() < rtidb::base::BLOB_PREFIX.size()) {
            tablet_set.insert(endpoint);
            continue;
        }
        const char* t_ch = endpoint.data();
        const char* b_ch = rtidb::base::BLOB_PREFIX.data();
        int ret = memcmp(t_ch, b_ch, rtidb::base::BLOB_PREFIX.size());
        if (ret == 0) {
            std::string ep(t_ch+rtidb::base::BLOB_PREFIX.size(),
                           endpoint.size() - rtidb::base::BLOB_PREFIX.size());
            blob_set.insert(endpoint);
        } else {
            tablet_set.insert(endpoint);
        }
    }
    UpdateEndpoint(tablet_set);
    UpdateBlobEndpoint(blob_set);
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

void BaseClient::UpdateBlobEndpoint(
    const std::set<std::string>& alive_endpoints) {
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
        if (tb != rtidb::type::TableType::kRelational &&
            tb != rtidb::type::TableType::kObjectStore) {
            continue;
        }
        std::shared_ptr<TableHandler> handler =
            std::make_shared<TableHandler>();
        handler->partition.resize(table_info->table_partition_size());
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
        if (table_info->has_blob_info()) {
            handler->blob_partition.resize(
                table_info->blob_info().blob_partition_size());
            int id = 0;
            for (const auto& part : table_info->blob_info().blob_partition()) {
                for (const auto& meta : part.partition_meta()) {
                    if (!meta.is_alive()) {
                        continue;
                    }
                    if (meta.is_leader()) {
                        handler->blob_partition[id].leader = meta.endpoint();
                    } else {
                        handler->blob_partition[id].follower.push_back(
                            meta.endpoint());
                    }
                }
                id++;
            }
        }
        if (table_info->table_type() == rtidb::type::kObjectStore) {
            handler->table_info = table_info;
            new_tables.insert(std::make_pair(table_name, handler));
            continue;
        }
        for (int i = 0; i < table_info->column_key_size(); i++) {
            if (table_info->column_key(i).has_index_type() &&
                table_info->column_key(i).index_type() ==
                    ::rtidb::type::IndexType::kAutoGen) {
                handler->auto_gen_pk = table_info->column_key(i).col_name(0);
                break;
            }
        }
        std::shared_ptr<google::protobuf::RepeatedPtrField<
            rtidb::common::ColumnDesc>> columns =
            std::make_shared<google::protobuf::RepeatedPtrField<
                rtidb::common::ColumnDesc>>();
        int code = rtidb::codec::SchemaCodec::ConvertColumnDesc(
            table_info->column_desc_v1(), *columns,
            table_info->added_column_desc());
        if (code != 0) {
            continue;
        }
        if (table_info->has_blob_info()) {
            for (int i = 0; i < columns->size(); i++) {
                const auto& type = columns->Get(i).data_type();
                if (type == rtidb::type::kBlob) {
                    handler->blobSuffix.push_back(i);
                    const std::string& col_name = columns->Get(i).name();
                    handler->blobFieldNames.push_back(col_name);
                }
            }
        }
        std::map<std::string, ::rtidb::type::DataType> map;
        for (const auto& col_desc : *columns) {
            map.insert(std::make_pair(col_desc.name(), col_desc.data_type()));
        }
        handler->name_type_map = std::move(map);
        handler->table_info = table_info;
        handler->columns = columns;
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
    delete zk_client_;
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
        return nullptr;
    }
    {
        std::lock_guard<std::mutex> mx(mu_);
        tablets_.insert(std::make_pair(endpoint, tablet));
    }
    return tablet;
}

std::shared_ptr<rtidb::client::BsClient> BaseClient::GetBlobClient(
    const std::string& endpoint, std::string* msg) {
    {
        std::lock_guard<std::mutex> mx(mu_);
        auto iter = blobs_.find(endpoint);
        if (iter != blobs_.end()) {
            return iter->second;
        }
    }
    std::shared_ptr<rtidb::client::BsClient> blob =
        std::make_shared<rtidb::client::BsClient>(endpoint);
    int code = blob->Init();
    if (code < 0) {
        *msg = "failed init blob client";
        return nullptr;
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
        return nullptr;
    }
    return iter->second;
}
BaseClient::BaseClient(
    const std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>>&
        tablets)
    : tablets_(tablets) {}

RtidbClient::RtidbClient() : client_(nullptr), empty_vector_() {}

RtidbClient::~RtidbClient() {
    if (client_ != nullptr) {
        delete client_;
    }
}

void RtidbClient::SetZkCheckInterval(int32_t interval) {
    client_->SetZkCheckInterval(interval);
}

GeneralResult RtidbClient::Init(const std::string& zk_cluster,
                                const std::string& zk_path) {
    GeneralResult result;
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

TraverseResult RtidbClient::Traverse(const std::string& name,
                                     const struct ReadOption& ro) {
    TraverseResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        result.SetError(-1, "table not found");
        return result;
    }

    uint32_t count = 0;
    std::string* raw_data = new std::string;
    bool is_finish = true;
    uint64_t snapshot_id = 0;
    std::string pk;
    bool ok =
        Traverse(name, ro, raw_data, &count, &pk, &is_finish, &snapshot_id);
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
    result.SetValue(raw_data, is_finish, pk);
    result.SetTable(name);
    result.SetBlobIdxVec(th->blobSuffix);
    return result;
}

bool RtidbClient::Traverse(const std::string& name, const struct ReadOption& ro,
                           std::string* data, uint32_t* count,
                           std::string* last_key, bool* is_finish,
                           uint64_t* snapshot_id) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        return false;
    }
    std::string err_msg;
    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
    if (tablet == nullptr) {
        return false;
    }
    ::rtidb::api::ReadOption ro_pb;
    if (!ro.index.empty()) {
        for (const auto& kv : ro.index) {
            auto iter = th->name_type_map.find(kv.first);
            if (iter == th->name_type_map.end()) {
                return false;
            }
            ::rtidb::api::Columns* index = ro_pb.add_index();
            index->add_name(kv.first);
            if (kv.second == ::rtidb::codec::NONETOKEN) {
                continue;
            }
            ::rtidb::type::DataType type = iter->second;
            std::string* val = index->mutable_value();
            if (!rtidb::codec::Convert(kv.second, type, val)) {
                return false;
            }
        }
    }
    bool ok = tablet->Traverse(th->table_info->tid(), 0, ro_pb, 200, last_key,
                               snapshot_id, data, count, is_finish, &err_msg);
    return ok;
}

UpdateResult RtidbClient::Update(
    const std::string& table_name,
    const std::map<std::string, std::string>& condition_map,
    const std::map<std::string, std::string>& value_map,
    const WriteOption& wo) {
    UpdateResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(table_name);
    if (th == nullptr) {
        result.SetError(-1, "table not found");
        return result;
    }
    if (condition_map.empty() || value_map.empty()) {
        result.SetError(-1,
                        "condition_columns_map or value_columns_map is empty");
        return result;
    }
    uint32_t tid = th->table_info->tid();
    uint32_t pid = 0;
    ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns> cd_columns;
    ::rtidb::base::ResultMsg cd_rm = ::rtidb::codec::SchemaCodec::GetCdColumns(
        *(th->columns), condition_map, &cd_columns);
    if (cd_rm.code < 0) {
        result.SetError(cd_rm.code, "GetCdColumns error, msg: " + cd_rm.msg);
        return result;
    }
    google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>
        new_value_schema;
    ::rtidb::codec::SchemaCodec::GetSchemaData(value_map, *(th->columns),
                                               new_value_schema);
    std::string data;
    ::rtidb::base::ResultMsg rm =
        ::rtidb::codec::RowCodec::EncodeRow(value_map,
                                            new_value_schema, data);
    if (rm.code < 0) {
        result.SetError(rm.code, "encode error, msg: " + rm.msg);
        return result;
    }
    if (th->table_info->compress_type() == ::rtidb::nameserver::kSnappy) {
        std::string compressed;
        ::snappy::Compress(data.c_str(), data.length(), &compressed);
        data = compressed;
    }
    std::string msg;
    auto tablet_client =
        client_->GetTabletClient(th->partition[0].leader, &msg);
    if (tablet_client == nullptr) {
        result.SetError(-1, msg);
        return result;
    }
    uint32_t count = 0;
    bool ok = tablet_client->Update(tid, pid, cd_columns, new_value_schema,
                                    data, &count, &msg);
    if (!ok) {
        result.SetError(-1, msg);
    }
    result.SetAffectedCount(count);
    return result;
}

PutResult RtidbClient::Put(
    const std::string& name,
    const std::map<std::string, std::string>& value,
    const WriteOption& wo) {
    PutResult result;

    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        result.SetError(-1, "table not found");
        return result;
    }
    std::map<std::string, std::string> val;
    if (!th->auto_gen_pk.empty()) {
        auto iter = value.find(th->auto_gen_pk);
        if (iter != value.end()) {
            result.SetError(-1, "should not input autoGenPk column");
            return result;
        } else {
            val = value;
            val.insert(
                std::make_pair(th->auto_gen_pk, ::rtidb::codec::DEFAULT_LONG));
        }
    }
    std::string buffer;
    rtidb::base::ResultMsg rm;
    if (!th->auto_gen_pk.empty()) {
        rm = ::rtidb::codec::RowCodec::EncodeRow(val, *(th->columns), buffer);
    } else {
        rm = ::rtidb::codec::RowCodec::EncodeRow(value, *(th->columns), buffer);
    }
    if (rm.code != 0) {
        result.SetError(rm.code, "encode error, msg: " + rm.msg);
        return result;
    }
    std::string err_msg;

    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
    if (tablet == nullptr) {
        result.SetError(-1, err_msg);
        return result;
    }
    int64_t auto_key = 0;
    bool ok =
        tablet->Put(th->table_info->tid(), 0, buffer, &auto_key, &err_msg);
    if (!ok) {
        result.SetError(-1, "put error, msg: " + err_msg);
        return result;
    }
    if (!th->auto_gen_pk.empty()) {
        result.SetAutoGenPk(auto_key);
    }
    return result;
}

UpdateResult RtidbClient::Delete(
    const std::string& name, const std::map<std::string, std::string>& values) {
    UpdateResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        result.SetError(-1, "table not found");
        return result;
    }
    auto tablet =
        client_->GetTabletClient(th->partition[0].leader, &result.msg);
    if (tablet == nullptr) {
        result.code = -1;
        return result;
    }
    uint32_t tid = th->table_info->tid();
    uint32_t pid = 0;
    ::google::protobuf::RepeatedPtrField<::rtidb::api::Columns> cd_columns;
    ::rtidb::base::ResultMsg cd_rm = ::rtidb::codec::SchemaCodec::GetCdColumns(
        *(th->columns), values, &cd_columns);
    if (cd_rm.code < 0) {
        result.SetError(cd_rm.code, "GetCdColumns error, msg: " + cd_rm.msg);
        return result;
    }
    uint32_t count = 0;
    bool ok = tablet->Delete(tid, pid, cd_columns, &count, &result.msg);
    if (!ok) {
        result.code = -1;
        return result;
    }
    result.SetAffectedCount(count);
    return result;
}

BatchQueryResult RtidbClient::BatchQuery(const std::string& name,
                                         const std::vector<ReadOption>& ros) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    BatchQueryResult result;
    if (th == nullptr) {
        result.SetError(-1, "table not found");
        return result;
    }
    ::google::protobuf::RepeatedPtrField<::rtidb::api::ReadOption> ros_pb;
    for (const auto& ro : ros) {
        ::rtidb::api::ReadOption* ro_ptr = ros_pb.Add();
        for (const auto& kv : ro.index) {
            auto iter = th->name_type_map.find(kv.first);
            if (iter == th->name_type_map.end()) {
                result.code_ = -1;
                result.msg_ = "col_name " + kv.first + " not exist";
                return result;
            }
            ::rtidb::api::Columns* index = ro_ptr->add_index();
            index->add_name(kv.first);
            if (kv.second == ::rtidb::codec::NONETOKEN) {
                continue;
            }
            ::rtidb::type::DataType type = iter->second;
            std::string* val = index->mutable_value();
            if (!rtidb::codec::Convert(kv.second, type, val)) {
                result.code_ = -1;
                result.msg_ = "convert str " + kv.second + " failed";
                return result;
            }
        }
    }
    std::string* data = new std::string();
    uint32_t count;
    std::string msg;
    bool ok = BatchQuery(name, ros_pb, data, &count, &msg);
    if (!ok) {
        result.SetError(-1, msg);
        return result;
    }
    result.SetRv(th);
    result.SetValue(data, count);
    result.SetTable(name);
    result.SetClient(this);
    result.SetBlobIdxVec(th->blobSuffix);
    return result;
}

bool RtidbClient::BatchQuery(
    const std::string& name,
    const ::google::protobuf::RepeatedPtrField<::rtidb::api::ReadOption>&
        ros_pb,
    std::string* data, uint32_t* count, std::string* msg) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        return false;
    }
    auto tablet = client_->GetTabletClient(th->partition[0].leader, msg);
    if (tablet == nullptr) {
        return false;
    }
    return tablet->BatchQuery(th->table_info->tid(), 0, ros_pb, data, count,
                              msg);
}

std::vector<std::string>& RtidbClient::GetBlobSchema(const std::string& name) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        return empty_vector_;
    }
    return th->blobFieldNames;
}

BlobInfoResult RtidbClient::GetBlobInfo(const std::string& name) {
    BlobInfoResult result;
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        result.SetError(-1, "table not found");
        return result;
    }
    std::string msg;
    std::shared_ptr<rtidb::client::BsClient> blob_server;
    if (th->blob_partition.empty()) {
        result.SetError(-1, "not found available blob endpoint");
        return result;
    }
    blob_server = client_->GetBlobClient(th->blob_partition[0].leader, &msg);
    if (!blob_server) {
        result.SetError(-1, "blob server is unavailable status");
        return result;
    }
    result.client_ = blob_server;
    result.tid_ = th->table_info->tid();
    return result;
}

