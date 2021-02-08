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
#include "google/protobuf/text_format.h"
#include "proto/client.pb.h"

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

bool BaseClient::CreateTable(const rtidb::nameserver::TableInfo& info) {
    std::string msg;
    return ns_client_->CreateTable(info, msg);
}

std::vector<std::string> BaseClient::ShowTable(const std::string& name) {
    std::vector<std::string> tbs;
    std::string msg;
    std::vector<::rtidb::nameserver::TableInfo> tables;
    bool ok = ns_client_->ShowTable(name, tables, msg);
    if (!ok) {
        return tbs;
    }
    tbs.resize(tables.size());
    for (const auto& tb : tables) {
        tbs.push_back(tb.name());
    }

    return std::move(tbs);
}

void BaseClient::DropTable(const std::string& name) {
    if (name.empty()) {
        return;
    }
    std::string msg;
    ns_client_->DropTable(name, msg);
}

bool BaseClient::Init(std::string* msg) {
    zk_client_ = new rtidb::zk::ZkClient(zk_cluster_, "", zk_session_timeout_,
            endpoint_, zk_root_path_);
    if (!zk_client_->Init()) {
        if (!zk_client_->Init()) {
            *msg = "zk client init failed";
            return false;
        }
    }
    std::string node_path = zk_root_path_ + "/leader";
    std::vector<std::string> children;
    if (!zk_client_->GetChildren(node_path, children) || children.empty()) {
        *msg = "get ns node list fail";
        return false;
    }
    std::string leader_path = node_path + "/" + children[0];
    std::string endpoint;
    if (!zk_client_->GetNodeValue(leader_path, endpoint)) {
        *msg = "get leader ns endpoint fail";
        return false;
    }
    const std::string name_path = zk_root_path_ + "/map/names/" + endpoint;
    if (zk_client_->IsExistNode(name_path) == 0) {
        std::string real_ep;
        if (zk_client_->GetNodeValue(name_path, real_ep)) {
            endpoint = real_ep;
        }
    }
    ns_client_ = new rtidb::client::NsClient(false, endpoint, "");
    if (ns_client_->Init() < 0) {
        *msg = "ns client init failed";
        return false;
    }

    zk_table_data_path_ = zk_root_path_ + "/table/table_data";
    RefreshNodeList();
    RefreshTable();
    bool ok = zk_client_->WatchItem(table_notify_, boost::bind(&BaseClient::DoFresh, this));
    if (!ok) {
        zk_client_->CloseZK();
        *msg = "zk watch table notify failed";
        return false;
    }
    task_thread_pool_.DelayTask(zk_keep_alive_check_, [this] { CheckZkClient(); });
    return true;
}

void BaseClient::CheckZkClient() {
    if (!zk_client_->IsConnected()) {
        // TODO(konquan): use log
        std::cout << "reconnect zk" << std::endl;
        if (zk_client_->Reconnect()) {
            std::cout << "reconnect zk ok" << std::endl;
            RefreshNodeList();
            RefreshTable();
        }
    }
    if (zk_client_session_term_ != zk_client_->GetSessionTerm()) {
        if (zk_client_->WatchItem(table_notify_,
                      boost::bind(&BaseClient::DoFresh, this))) {
            zk_client_session_term_ = zk_client_->GetSessionTerm();
        } else {
            // TODO(kongquan): print log
        }
    }
    task_thread_pool_.DelayTask(zk_keep_alive_check_,
                                [this] { CheckZkClient(); });
}

bool BaseClient::RefreshNodeList() {
    std::vector<std::string> endpoints;
    if (!zk_client_->GetNodes(endpoints)) {
        return false;
    }
    std::set<std::string> tablet_set;
    std::set<std::string> blob_set;
    for (const auto& endpoint : endpoints) {
        if (boost::starts_with(endpoint, rtidb::base::BLOB_PREFIX)) {
            blob_set.insert(endpoint.substr(rtidb::base::BLOB_PREFIX.size()));
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
        std::string real_endpoint;
        if (!GetRealEndpoint(endpoint, &real_endpoint)) {
            continue;
        }
        auto iter = old_tablets.find(endpoint);
        if (iter == old_tablets.end()) {
            auto tablet = std::make_shared<rtidb::client::TabletClient>(false, endpoint, real_endpoint);
            if (tablet->Init() != 0) {
                std::cerr << endpoint << " initial failed!" << std::endl;
                continue;
            }
            new_tablets.insert(std::make_pair(endpoint, tablet));
        } else {
            if (!real_endpoint.empty()) {
                iter->second = std::make_shared<rtidb::client::TabletClient>(false, endpoint, real_endpoint);
                if (iter->second->Init() != 0) {
                    std::cerr << endpoint << " initial failed!" << std::endl;
                    continue;
                }
            }
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
        std::string real_endpoint;
        if (!GetRealEndpoint(endpoint, &real_endpoint)) {
            continue;
        }
        auto iter = old_blobs.find(endpoint);
        if (iter == old_blobs.end()) {
            // TODO use_rdma
            auto blob = std::make_shared<rtidb::client::BsClient>(false, endpoint, real_endpoint);
            if (blob->Init() != 0) {
                std::cerr << endpoint << " initial failed!" << std::endl;
                continue;
            }
            new_blobs.insert(std::make_pair(endpoint, blob));
        } else {
            if (!real_endpoint.empty()) {
                // TODO use_rdma
                iter->second = std::make_shared<rtidb::client::BsClient>(false, endpoint, real_endpoint);
                if (iter->second->Init() != 0) {
                    std::cerr << endpoint << " initial failed!" << std::endl;
                    continue;
                }
            }
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
            std::cerr << "get table info failed! name " << table_name << " table node " << table_node << std::endl;
            continue;
        }
        std::shared_ptr<rtidb::nameserver::TableInfo> table_info = std::make_shared<rtidb::nameserver::TableInfo>();
        if (!table_info->ParseFromString(value)) {
            std::cerr << "parse table info failed! name " << table_name << std::endl;
            continue;
        }
        rtidb::type::TableType tb = table_info->table_type();
        if (tb != rtidb::type::TableType::kRelational && tb != rtidb::type::TableType::kObjectStore) {
            continue;
        }
        std::shared_ptr<TableHandler> handler = std::make_shared<TableHandler>();
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
            handler->blob_partition.resize(table_info->blob_info().blob_partition_size());
            int id = 0;
            for (const auto& part : table_info->blob_info().blob_partition()) {
                for (const auto& meta : part.partition_meta()) {
                    if (!meta.is_alive()) {
                        continue;
                    }
                    if (meta.is_leader()) {
                        handler->blob_partition[id].leader = meta.endpoint();
                    } else {
                        handler->blob_partition[id].follower.push_back(meta.endpoint());
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
        for (const auto& key : table_info->column_key()) {
            if (key.has_flag() && key.flag() == 0) {
                continue;
            }
            if (key.has_index_type() && key.index_type() == rtidb::type::IndexType::kAutoGen) {
                handler->auto_gen_pk = key.col_name(0);
            }
        }

        std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>> columns =
            std::make_shared<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>();
        int code = rtidb::codec::SchemaCodec::ConvertColumnDesc(
            table_info->column_desc_v1(), *columns, table_info->added_column_desc());
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
        std::map<uint32_t, std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>> versions;
        handler->last_schema_version = 1;
        for (const auto ver : table_info->schema_versions()) {
            int remain_size = ver.field_count() - table_info->column_desc_size();
            if (remain_size < 0)  {
                continue;
            }
            if (remain_size > table_info->added_column_desc_size()) {
                continue;
            }
            std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>> schema =
                std::make_shared<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>();
            schema->CopyFrom(table_info->column_desc_v1());
            for (int i = 0; i < remain_size; i++) {
                rtidb::common::ColumnDesc* col = schema->Add();
                col->CopyFrom(table_info->added_column_desc(i));
            }
            versions.insert(std::make_pair(ver.id(), schema));
            handler->last_schema_version = ver.id();
        }
        handler->name_type_map = std::move(map);
        handler->table_info = table_info;
        handler->columns = columns;
        handler->version_schema = versions;
        new_tables.insert(std::make_pair(table_name, handler));
    }
    std::lock_guard<std::mutex> mx(mu_);
    tables_ = new_tables;
}

void BaseClient::SetZkCheckInterval(int32_t interval) {
    zk_keep_alive_check_ = interval;
}

void BaseClient::DoFresh() {
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
    std::string real_endpoint;
    if (!GetRealEndpoint(endpoint, &real_endpoint)) {
        return std::shared_ptr<rtidb::client::TabletClient>();
    }
    auto tablet = std::make_shared<rtidb::client::TabletClient>(false, endpoint, real_endpoint);
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
    std::string real_endpoint;
    if (!GetRealEndpoint(endpoint, &real_endpoint)) {
        return std::shared_ptr<rtidb::client::BsClient>();
    }
    auto blob = std::make_shared<rtidb::client::BsClient>(false, endpoint, real_endpoint);
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

bool BaseClient::GetRealEndpoint(const std::string& endpoint,
        std::string* real_endpoint) {
    if (real_endpoint == nullptr) {
        return false;
    }
    std::string sdk_path = zk_root_path_ + "/map/sdkendpoints/" + endpoint;
    if (zk_client_->IsExistNode(sdk_path) == 0) {
        if (!zk_client_->GetNodeValue(sdk_path, *real_endpoint)) {
            std::cout << "get zk failed! : sdk_path: " << sdk_path <<
                " real_endpoint: " << *real_endpoint << std::endl;
            return false;
        }
    }
    if (real_endpoint->empty()) {
        std::string sname_path = zk_root_path_ + "/map/names/" + endpoint;
        if (zk_client_->IsExistNode(sname_path) == 0) {
            if (!zk_client_->GetNodeValue(sname_path, *real_endpoint)) {
                std::cout << "get zk failed! sname_path: " << sname_path
                    << " real_endpoint: " << *real_endpoint << std::endl;
                return false;
            }
        }
    }
    return true;
}

BaseClient::BaseClient(const std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>>& tablets)
    : tablets_(tablets), zk_session_timeout_(60*1000) {}


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
                                const std::string& zk_path, uint32_t zk_session_timeout) {
    GeneralResult result;
    std::shared_ptr<rtidb::zk::ZkClient> zk_client;
    if (zk_cluster.empty()) {
        result.SetError(-1, "initial failed! not set zk_cluster");
        return result;
    }
    if (zk_session_timeout < 1) {
        zk_session_timeout = 60;
    }
    client_ = new BaseClient(zk_cluster, zk_path, "", 1000 * zk_session_timeout, 15000);

    std::string msg;
    bool ok = client_->Init(&msg);
    if (!ok) {
        result.SetError(-2, msg);
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
        result.code_ = -2;
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
    if (condition_map.empty()) {
        result.SetError(-2, "condition columns map is empty");
        return result;
    }
    if (value_map.empty()) {
        result.SetError(-3, "value map is empty");
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
        ::rtidb::codec::RowCodec::EncodeRow(value_map, new_value_schema, th->last_schema_version, data);
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
        result.SetError(-4, msg);
        return result;
    }
    uint32_t count = 0;
    bool ok = tablet_client->Update(tid, pid, cd_columns, new_value_schema,
                                    data, &count, &msg);
    if (!ok) {
        result.SetError(-5, msg);
    }
    result.SetAffectedCount(count);
    return result;
}

PutResult RtidbClient::Put(const std::string& name, const std::map<std::string, std::string>& value,
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
            result.SetError(-2, "should not input autoGenPk column");
            return result;
        } else {
            val = value;
            val.insert(std::make_pair(th->auto_gen_pk, ::rtidb::codec::DEFAULT_LONG));
        }
    }
    std::string buffer;
    rtidb::base::ResultMsg rm;
    if (!th->auto_gen_pk.empty()) {
        rm = ::rtidb::codec::RowCodec::EncodeRow(val, *(th->columns), th->last_schema_version, buffer);
    } else {
        rm = ::rtidb::codec::RowCodec::EncodeRow(value, *(th->columns), th->last_schema_version, buffer);
    }
    if (rm.code != 0) {
        result.SetError(rm.code, "encode error, msg: " + rm.msg);
        return result;
    }
    std::string err_msg;

    auto tablet = client_->GetTabletClient(th->partition[0].leader, &err_msg);
    if (tablet == nullptr) {
        result.SetError(-3, err_msg);
        return result;
    }
    ::rtidb::api::WriteOption pb_wo;
    pb_wo.set_update_if_exist(wo.update_if_exist);
    int64_t auto_key = 0;
    std::vector<int64_t> blob_keys;
    bool ok = tablet->Put(th->table_info->tid(), 0, buffer, pb_wo, &auto_key, &blob_keys, &err_msg);
    if (!ok) {
        result.SetError(-4, "put error, msg: " + err_msg);
        return result;
    }
    if (!blob_keys.empty()) {
        if (!DeleteBlobs(name, blob_keys)) {
            result.SetError(-5, "deleteBlob of put error");
            return result;
        }
    }
    if (!th->auto_gen_pk.empty()) {
        result.SetAutoGenPk(auto_key);
    }
    return result;
}

bool RtidbClient::DeleteBlobs(const std::string& name, const std::vector<int64_t>& keys) {
    std::shared_ptr<TableHandler> th = client_->GetTableHandler(name);
    if (th == nullptr) {
        return false;
    }
    if (th->blob_partition.empty()) {
        return true;
    }
    if (th->blob_partition[0].leader.empty()) {
        return false;
    }
    std::string msg;
    auto blob = client_->GetBlobClient(th->blob_partition[0].leader, &msg);
    if (blob == nullptr) {
        return false;
    }
    uint32_t tid = th->table_info->tid();
    uint32_t pid = 0;
    for (auto& key : keys) {
        blob->Delete(tid, pid, key, &msg);
    }
    return true;
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
        result.code = -2;
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
    bool ok = false;
    std::vector<int64_t> blobs;
    if (th->blobSuffix.empty()) {
        ok = tablet->Delete(tid, pid, cd_columns, &count, &result.msg);
    } else {
        ok = tablet->Delete(tid, pid, cd_columns, &count, &result.msg, &blobs);
    }
    if (!ok) {
        result.code = -3;
        return result;
    }
    if (!blobs.empty()) {
        DeleteBlobs(name, blobs);
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
                result.code_ = -2;
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
                result.code_ = -3;
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
        result.SetError(-4, msg);
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
        result.SetError(-1, "blob partition is empty");
        return result;
    }
    if (th->blob_partition[0].leader.empty()) {
        result.SetError(-2, "not found available blob endpoint");
        return result;
    }
    blob_server = client_->GetBlobClient(th->blob_partition[0].leader, &msg);
    if (!blob_server) {
        result.SetError(-3, "blob server is unavailable status");
        return result;
    }
    result.client_ = blob_server;
    result.tid_ = th->table_info->tid();
    return result;
}

int RtidbClient::CreateTable(const std::string& table_meta) {
    rtidb::client::TableInfo table_info;
    bool ok = google::protobuf::TextFormat::ParseFromString(table_meta, &table_info);
    if (!ok) {
        return -1;
    }
    {
        std::shared_ptr<TableHandler> th = client_->GetTableHandler(table_info.name());
        if (th != nullptr) {
            return -2;
        }
    }
    rtidb::nameserver::TableInfo ns_tbinfo;
    ns_tbinfo.set_replica_num(1);
    ns_tbinfo.set_partition_num(1);

    ns_tbinfo.set_name(table_info.name());
    ns_tbinfo.set_table_type(rtidb::type::TableType::kRelational);
    std::string storage_mode = table_info.storage_mode();
    if (storage_mode == "kssd" || storage_mode == "ssd") {
        ns_tbinfo.set_storage_mode(::rtidb::common::kSSD);
    } else if (storage_mode == "khdd" || storage_mode == "hdd") {
        ns_tbinfo.set_storage_mode(::rtidb::common::kHDD);
    } else {
        return -3;
    }
    std::map<std::string, std::string> name_map;
    for (const auto& col : table_info.column_desc()) {
        const std::string& type = col.type();
        if (rtidb::codec::DATA_TYPE_MAP.count(type) < 1) {
            return -4;
        }
        const std::string& col_name = col.name();
        if (col_name.empty() || name_map.count(col_name) > 0) {
            return -5;
        }
        rtidb::common::ColumnDesc* new_col = ns_tbinfo.add_column_desc_v1();
        new_col->CopyFrom(col);
        const auto& tp_iter = rtidb::codec::DATA_TYPE_MAP.find(type);
        if (tp_iter == rtidb::codec::DATA_TYPE_MAP.end()) {
           return -21;
        }
        new_col->set_data_type(tp_iter->second);
        name_map.insert(std::make_pair(col_name, type));
    }
    std::set<std::string> key_set;
    for (const auto& idx : table_info.column_key()) {
        const std::string& key_name = idx.index_name();
        if (key_set.count(key_name) > 0) {
            return -6;
        }
        for (const auto& col : idx.col_name()) {
            const auto& iter = name_map.find(col);
            if (iter == name_map.end()) {
                return -7;
            }
            if (iter->second == "float" || iter->second == "double") {
                return -8;
            }
        }
        rtidb::common::ColumnKey* column_key = ns_tbinfo.add_column_key();
        column_key->CopyFrom(idx);
        key_set.insert(key_name);
    }
    std::set<std::string> index_set;
    std::string auto_gen_pk_name;
    for (const auto& index : table_info.index()) {
        if (index_set.count(index.index_name()) > 0) {
            return -9;
        }
        for (const auto& col : index.col_name()) {
            if (name_map.count(col) < 0) {
                return -10;
            }
        }
        rtidb::common::ColumnKey* ck = ns_tbinfo.add_column_key();
        ck->set_index_name(index.index_name());
        for (const auto& col : index.col_name()) {
            ck->add_col_name(col);
        }
        const auto& idx_iter = rtidb::codec::INDEX_TYPE_MAP.find(index.index_type());
        if (idx_iter == rtidb::codec::INDEX_TYPE_MAP.end()) {
            return -11;
        }
        if (idx_iter->second == rtidb::type::kAutoGen) {
            auto_gen_pk_name = index.col_name(0);
        }
        ck->set_index_type(idx_iter->second);
        const auto& iter = name_map.find(auto_gen_pk_name);
        if (iter != name_map.end() && iter->second != "bigint") {
            return -12;
        }
        index_set.insert(index.index_name());
    }
    if (index_set.empty() && !name_map.empty()) {
        return -13;
    }
    ok = client_->CreateTable(ns_tbinfo);
    if (!ok) {
        return -20;
    }
    return 0;
}

std::vector<std::string> RtidbClient::ShowTable(const std::string& name) {
    return std::move(client_->ShowTable(name));
}

void RtidbClient::DropTable(const std::string& name) {
    if (name.empty()) return;
    client_->DropTable(name);
}
