/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "catalog/tablet_catalog.h"

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>

#include "catalog/distribute_iterator.h"
#include "codec/list_iterator_codec.h"
#include "glog/logging.h"
#include "schema/index_util.h"
#include "schema/schema_adapter.h"

DECLARE_bool(enable_localtablet);
namespace openmldb {
namespace catalog {

TabletTableHandler::TabletTableHandler(const ::openmldb::api::TableMeta& meta,
                                       std::shared_ptr<hybridse::vm::Tablet> local_tablet)
    : partition_num_(meta.table_partition_size()),
      schema_(),
      table_st_(meta),
      tables_(std::make_shared<Tables>()),
      types_(),
      index_pos_(0),
      index_hint_vec_(),
      table_client_manager_(),
      local_tablet_(local_tablet) {}

TabletTableHandler::TabletTableHandler(const ::openmldb::nameserver::TableInfo& meta,
                                       std::shared_ptr<hybridse::vm::Tablet> local_tablet)
    : partition_num_(meta.table_partition_size()),
      schema_(),
      table_st_(meta),
      tables_(std::make_shared<Tables>()),
      types_(),
      index_pos_(0),
      index_hint_vec_(),
      table_client_manager_(),
      local_tablet_(local_tablet) {}

bool TabletTableHandler::Init(const ClientManager& client_manager) {
    if (partition_num_ == 0) {
        // some test cases not set table_partition
        partition_num_ = 1;
    }
    index_hint_vec_.resize(partition_num_);
    bool ok = schema::SchemaAdapter::ConvertSchema(table_st_.GetColumns(), &schema_);
    if (!ok) {
        LOG(WARNING) << "fail to covert schema to sql schema";
        return false;
    }
    // init types var
    for (int32_t i = 0; i < schema_.size(); i++) {
        const ::hybridse::type::ColumnDef& column = schema_.Get(i);
        ::hybridse::vm::ColInfo col_info;
        col_info.type = column.type();
        col_info.idx = i;
        col_info.name = column.name();
        types_.insert(std::make_pair(column.name(), col_info));
    }

    if (!UpdateIndex(table_st_.GetColumnKey())) {
        LOG(WARNING) << "fail to update index";
        return false;
    }
    table_client_manager_ = std::make_shared<TableClientManager>(table_st_, client_manager);
    DLOG(INFO) << "init table handler for table " << GetName() << " in db " << GetDatabase() << " done";
    return true;
}

bool TabletTableHandler::UpdateIndex(
        const ::google::protobuf::RepeatedPtrField<::openmldb::common::ColumnKey>& indexs) {
    int pos = (index_pos_.load() + 1) % partition_num_;
    index_hint_vec_[pos].clear();
    for (int32_t i = 0; i < indexs.size(); i++) {
        const auto& column_key = indexs.Get(i);
        ::hybridse::vm::IndexSt index_st;
        index_st.index = i;
        index_st.ts_pos = ::hybridse::vm::INVALID_POS;
        if (!column_key.ts_name().empty()) {
            int32_t pos = GetColumnIndex(column_key.ts_name());
            if (pos < 0) {
                LOG(WARNING) << "fail to get second key " << column_key.ts_name();
                return false;
            }
            index_st.ts_pos = pos;
        }
        index_st.name = column_key.index_name();
        for (int32_t j = 0; j < column_key.col_name_size(); j++) {
            const std::string& key = column_key.col_name(j);
            auto it = types_.find(key);
            if (it == types_.end()) {
                LOG(WARNING) << "column " << key << " does not exist in table " << GetName();
                return false;
            }
            index_st.keys.push_back(it->second);
        }
        index_hint_vec_[pos].emplace(index_st.name, index_st);
    }
    index_pos_.store(pos, std::memory_order_release);
    return true;
}

const ::hybridse::vm::IndexHint& TabletTableHandler::GetIndex() {
    return index_hint_vec_.at(index_pos_.load(std::memory_order_acquire));
}

std::unique_ptr<::hybridse::codec::RowIterator> TabletTableHandler::GetIterator() {
    return std::unique_ptr<::hybridse::codec::RowIterator>(GetRawIterator());
}

std::unique_ptr<::hybridse::codec::WindowIterator> TabletTableHandler::GetWindowIterator(const std::string& idx_name) {
    const auto& index_hint = GetIndex();
    auto iter = index_hint.find(idx_name);
    if (iter == index_hint.end()) {
        LOG(WARNING) << "index name " << idx_name << " not exist";
        return std::unique_ptr<::hybridse::codec::WindowIterator>();
    }
    DLOG(INFO) << "get window it with index " << idx_name;
    auto tables = std::atomic_load_explicit(&tables_, std::memory_order_acquire);
    if (!tables) {
        LOG(WARNING) << " tables is null";
        return {};
    }
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients;
    for (uint32_t pid = 0; pid < partition_num_; pid++) {
        if (tables->count(pid) == 0) {
            auto accessor = table_client_manager_->GetTablet(pid);
            if (accessor) {
                tablet_clients.emplace(pid, accessor->GetClient());
            }
        }
    }
    DLOG(INFO) << "table size " << tables->size() << " tablet_clients size " << tablet_clients.size();
    return std::make_unique<DistributeWindowIterator>(GetTid(), partition_num_, tables,
            iter->second.index, idx_name, tablet_clients);
}

// TODO(chenjing): optimize Get(int pos) base segment
const ::hybridse::codec::Row TabletTableHandler::Get(int32_t pos) {
    auto iter = GetIterator();
    while (pos-- > 0 && iter->Valid()) {
        iter->Next();
    }
    return iter->Valid() ? iter->GetValue() : ::hybridse::codec::Row();
}

::hybridse::codec::RowIterator* TabletTableHandler::GetRawIterator() {
    auto tables = std::atomic_load_explicit(&tables_, std::memory_order_acquire);
    std::map<uint32_t, std::shared_ptr<openmldb::client::TabletClient>> tablet_clients;
    for (uint32_t pid = 0; pid < partition_num_; pid++) {
        if (tables->count(pid) == 0) {
            auto accessor = table_client_manager_->GetTablet(pid);
            if (accessor) {
                tablet_clients.emplace(pid, accessor->GetClient());
            }
        }
    }
    DLOG(INFO) << "table size " << tables->size() << " tablet_clients size " << tablet_clients.size();
    return new catalog::FullTableIterator(GetTid(), tables, tablet_clients);
}

const uint64_t TabletTableHandler::GetCount() {
    auto iter = GetIterator();
    uint64_t cnt = 0;
    while (iter->Valid()) {
        iter->Next();
        cnt++;
    }
    return cnt;
}

::hybridse::codec::Row TabletTableHandler::At(uint64_t pos) {
    auto iter = GetIterator();
    while (pos-- > 0 && iter->Valid()) {
        iter->Next();
    }
    return iter->Valid() ? iter->GetValue() : ::hybridse::codec::Row();
}

std::shared_ptr<::hybridse::vm::PartitionHandler> TabletTableHandler::GetPartition(const std::string& index_name) {
    if (GetIndex().count(index_name) == 0) {
        LOG(WARNING) << "fail to get partition for tablet table handler, index name " << index_name;
        return std::shared_ptr<::hybridse::vm::PartitionHandler>();
    }
    return std::make_shared<TabletPartitionHandler>(shared_from_this(), index_name);
}

void TabletTableHandler::AddTable(std::shared_ptr<::openmldb::storage::Table> table) {
    std::shared_ptr<Tables> old_tables;
    std::shared_ptr<Tables> new_tables;
    do {
        old_tables = std::atomic_load_explicit(&tables_, std::memory_order_acquire);
        new_tables = std::make_shared<Tables>(*old_tables);
        new_tables->emplace(table->GetPid(), table);
    } while (!atomic_compare_exchange_weak(&tables_, &old_tables, new_tables));
}

bool TabletTableHandler::HasLocalTable() {
    return !std::atomic_load_explicit(&tables_, std::memory_order_acquire)->empty();
}

int TabletTableHandler::DeleteTable(uint32_t pid) {
    std::shared_ptr<Tables> old_tables;
    std::shared_ptr<Tables> new_tables;
    do {
        old_tables = std::atomic_load_explicit(&tables_, std::memory_order_acquire);
        new_tables = std::make_shared<Tables>(*old_tables);
        new_tables->erase(pid);
    } while (!atomic_compare_exchange_weak(&tables_, &old_tables, new_tables));
    return new_tables->size();
}

void TabletTableHandler::Update(const ::openmldb::nameserver::TableInfo& meta, const ClientManager& client_manager) {
    ::openmldb::storage::TableSt new_table_st(meta);
    for (const auto& partition_st : *(new_table_st.GetPartitions())) {
        uint32_t pid = partition_st.GetPid();
        if (!(partition_st == table_st_.GetPartition(pid))) {
            table_st_.SetPartition(partition_st);
        }
        table_client_manager_->UpdatePartitionClientManager(partition_st, client_manager);
    }
    if (meta.column_key_size() != static_cast<int>(GetIndex().size())) {
        UpdateIndex(meta.column_key());
    }
}

std::shared_ptr<::hybridse::vm::Tablet> TabletTableHandler::GetTablet(const std::string& index_name,
                                                                      const std::string& pk) {
    uint32_t pid_num = table_st_.GetPartitionNum();
    uint32_t pid = 0;
    if (pid_num > 0) {
        pid = (uint32_t)(::openmldb::base::hash64(pk) % pid_num);
    }
    DLOG(INFO) << "pid num " << pid_num << " get tablet with pid = " << pid;
    auto tables = std::atomic_load_explicit(&tables_, std::memory_order_relaxed);
    // return local tablet only when --enable_localtablet==true
    if (FLAGS_enable_localtablet && tables->find(pid) != tables->end()) {
        DLOG(INFO) << "get tablet index_name " << index_name << ", pk " << pk << ", local_tablet_";
        return local_tablet_;
    }
    auto client_tablet = table_client_manager_->GetTablet(pid);
    if (!client_tablet) {
        DLOG(INFO) << "get tablet index_name " << index_name << ", pk " << pk << ", tablet nullptr";
    } else {
        DLOG(INFO) << "get tablet index_name " << index_name << ", pk " << pk << ", tablet "
                   << client_tablet->GetName();
    }
    return client_tablet;
}

std::shared_ptr<::hybridse::vm::Tablet> TabletTableHandler::GetTablet(const std::string& index_name,
                                                                      const std::vector<std::string>& pks) {
    auto tablets_accessor = std::make_shared<TabletsAccessor>();
    for (const auto& pk : pks) {
        auto tablet_accessor = GetTablet(index_name, pk);
        if (tablet_accessor) {
            tablets_accessor->AddTabletAccessor(tablet_accessor);
        } else {
            LOG(WARNING) << "fail to get tablet: pk " << pk << " not exist";
            return std::shared_ptr<TabletsAccessor>();
        }
    }
    return tablets_accessor;
}

TabletCatalog::TabletCatalog()
    : mu_(),
      tables_(),
      db_(),
      db_sp_map_(),
      client_manager_(),
      version_(1),
      local_tablet_(),
      aggr_tables_(std::make_shared<AggrTableMap>()) {}

TabletCatalog::~TabletCatalog() {}

bool TabletCatalog::Init() { return true; }

std::shared_ptr<::hybridse::type::Database> TabletCatalog::GetDatabase(const std::string& db) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto it = db_.find(db);
    if (it == db_.end()) {
        return std::shared_ptr<::hybridse::type::Database>();
    }
    return it->second;
}

std::shared_ptr<::hybridse::vm::TableHandler> TabletCatalog::GetTable(const std::string& db,
                                                                      const std::string& table_name) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto db_it = tables_.find(db);
    if (db_it == tables_.end()) {
        return std::shared_ptr<::hybridse::vm::TableHandler>();
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        return std::shared_ptr<::hybridse::vm::TableHandler>();
    }
    return it->second;
}

bool TabletCatalog::AddTable(const ::openmldb::api::TableMeta& meta,
                             std::shared_ptr<::openmldb::storage::Table> table) {
    if (!table) {
        LOG(WARNING) << "input table is null";
        return false;
    }
    const std::string& db_name = meta.db();
    std::shared_ptr<TabletTableHandler> handler;
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
        auto result = tables_.emplace(db_name, std::map<std::string, std::shared_ptr<TabletTableHandler>>());
        db_it = result.first;
    }
    const std::string& table_name = meta.name();
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        handler = std::make_shared<TabletTableHandler>(meta, local_tablet_);
        if (!handler->Init(client_manager_)) {
            LOG(WARNING) << "tablet handler init failed";
            return false;
        }
        db_it->second.emplace(table_name, handler);
    } else {
        handler = it->second;
    }
    handler->AddTable(table);
    return true;
}

bool TabletCatalog::AddDB(const ::hybridse::type::Database& db) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    TabletDB::iterator it = db_.find(db.name());
    if (it != db_.end()) {
        return false;
    }
    tables_.insert(std::make_pair(db.name(), std::map<std::string, std::shared_ptr<TabletTableHandler>>()));
    return true;
}

bool TabletCatalog::DeleteTable(const std::string& db, const std::string& table_name, uint32_t pid) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto db_it = tables_.find(db);
    if (db_it == tables_.end()) {
        return false;
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        return false;
    }
    LOG(INFO) << "delete table from catalog. db " << db << ", name " << table_name << ", pid " << pid;
    if (it->second->DeleteTable(pid) < 1) {
        db_it->second.erase(it);
    }
    return true;
}

bool TabletCatalog::DeleteDB(const std::string& db) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    return db_.erase(db) > 0;
}

bool TabletCatalog::IndexSupport() { return true; }

bool TabletCatalog::AddProcedure(const std::string& db, const std::string& sp_name,
                                 const std::shared_ptr<hybridse::sdk::ProcedureInfo>& sp_info) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto& sp_map = db_sp_map_[db];
    if (sp_map.find(sp_name) != sp_map.end()) {
        LOG(WARNING) << "procedure " << sp_name << " already exist in db " << db;
        return false;
    }
    sp_map.insert({sp_name, sp_info});
    return true;
}

bool TabletCatalog::DropProcedure(const std::string& db, const std::string& sp_name) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto db_it = db_sp_map_.find(db);
    if (db_it == db_sp_map_.end()) {
        LOG(WARNING) << "db " << db << " not exist in sp map";
        return false;
    }
    auto& sp_map = db_it->second;
    auto it = sp_map.find(sp_name);
    if (it == sp_map.end()) {
        LOG(WARNING) << "procedure " << sp_name << " not exist in db " << db;
        return false;
    }
    sp_map.erase(it);
    return true;
}

bool TabletCatalog::UpdateTableMeta(const ::openmldb::api::TableMeta& meta) {
    const std::string& db_name = meta.db();
    const std::string& table_name = meta.name();
    std::shared_ptr<TabletTableHandler> handler;
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto db_it = tables_.find(db_name);
    if (db_it == tables_.end()) {
        LOG(WARNING) << "db " << db_name << " does not exist";
        return false;
    }
    auto it = db_it->second.find(table_name);
    if (it == db_it->second.end()) {
        LOG(WARNING) << "table " << table_name << " does not exist in db " << db_name;
        return false;
    } else {
        handler = it->second;
    }
    return handler->UpdateIndex(meta.column_key());
}

bool TabletCatalog::UpdateTableInfo(const ::openmldb::nameserver::TableInfo& table_info) {
    const std::string& db_name = table_info.db();
    const std::string& table_name = table_info.name();
    std::shared_ptr<TabletTableHandler> handler;
    {
        std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
        auto db_it = tables_.find(db_name);
        if (db_it == tables_.end()) {
            auto result = tables_.emplace(db_name, std::map<std::string, std::shared_ptr<TabletTableHandler>>());
            db_it = result.first;
        }
        auto it = db_it->second.find(table_name);
        if (it == db_it->second.end()) {
            handler = std::make_shared<TabletTableHandler>(table_info, local_tablet_);
            if (!handler->Init(client_manager_)) {
                LOG(WARNING) << "tablet handler init failed";
                return false;
            }
            db_it->second.emplace(table_name, handler);
            LOG(INFO) << "add table " << table_name << " db " << db_name;
        } else {
            handler = it->second;
        }
        handler->Update(table_info, client_manager_);
    }
    return true;
}

void TabletCatalog::Refresh(const std::vector<::openmldb::nameserver::TableInfo>& table_info_vec, uint64_t version,
                            const Procedures& db_sp_map) {
    std::map<std::string, std::set<std::string>> table_map;
    for (const auto& table_info : table_info_vec) {
        const std::string& db_name = table_info.db();
        const std::string& table_name = table_info.name();
        if (db_name.empty()) {
            continue;
        }
        if (!UpdateTableInfo(table_info)) {
            continue;
        }
        auto cur_db_it = table_map.find(db_name);
        if (cur_db_it == table_map.end()) {
            auto result = table_map.emplace(db_name, std::set<std::string>());
            cur_db_it = result.first;
        }
        cur_db_it->second.insert(table_name);
    }

    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    for (auto db_it = tables_.begin(); db_it != tables_.end();) {
        auto cur_db_it = table_map.find(db_it->first);
        if (cur_db_it == table_map.end()) {
            LOG(INFO) << "delete db from catalog. db: " << db_it->first;
            db_it = tables_.erase(db_it);
            continue;
        }
        for (auto table_it = db_it->second.begin(); table_it != db_it->second.end();) {
            if (cur_db_it->second.find(table_it->first) == cur_db_it->second.end() &&
                !table_it->second->HasLocalTable()) {
                LOG(INFO) << "delete table from catalog. db: " << db_it->first << ", table: " << table_it->first;
                table_it = db_it->second.erase(table_it);
                continue;
            }
            ++table_it;
        }
        ++db_it;
    }
    db_sp_map_ = db_sp_map;
    version_.store(version, std::memory_order_relaxed);
    LOG(INFO) << "refresh catalog. version " << version;
}

bool TabletCatalog::UpdateClient(const std::map<std::string, std::string>& real_ep_map) {
    return client_manager_.UpdateClient(real_ep_map);
}

uint64_t TabletCatalog::GetVersion() const { return version_.load(std::memory_order_relaxed); }

std::shared_ptr<::hybridse::sdk::ProcedureInfo> TabletCatalog::GetProcedureInfo(const std::string& db,
                                                                                const std::string& sp_name) {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    auto db_sp_it = db_sp_map_.find(db);
    if (db_sp_it == db_sp_map_.end()) {
        return nullptr;
    }
    auto& map = db_sp_it->second;
    auto sp_it = map.find(sp_name);
    if (sp_it == map.end()) {
        return nullptr;
    }
    return sp_it->second;
}

const Procedures& TabletCatalog::GetProcedures() {
    std::lock_guard<::openmldb::base::SpinMutex> spin_lock(mu_);
    return db_sp_map_;
}

std::vector<::hybridse::vm::AggrTableInfo> TabletCatalog::GetAggrTables(
    const std::string& base_db, const std::string& base_table, const std::string& aggr_func,
    const std::string& aggr_col, const std::string& partition_cols, const std::string& order_col,
    const std::string& filter_col) {
    AggrTableKey key{base_db, base_table, aggr_func, aggr_col, partition_cols, order_col, filter_col};
    auto aggr_tables = std::atomic_load_explicit(&aggr_tables_, std::memory_order_acquire);
    return (*aggr_tables)[key];
}

void TabletCatalog::RefreshAggrTables(const std::vector<::hybridse::vm::AggrTableInfo>& table_infos) {
    auto new_aggr_tables = std::make_shared<AggrTableMap>();
    for (const auto& table_info : table_infos) {
        // TODO(zhanghao): can use AggrTableKey *table_key = static_cast<AggrTableKey*>(&table_info);
        AggrTableKey table_key{table_info.base_db,   table_info.base_table,     table_info.aggr_func,
                               table_info.aggr_col,  table_info.partition_cols, table_info.order_by_col,
                               table_info.filter_col};
        if (new_aggr_tables->count(table_key) == 0) {
            new_aggr_tables->emplace(std::move(table_key),
                                     std::vector<::hybridse::vm::AggrTableInfo>{std::move(table_info)});
        } else {
            new_aggr_tables->at(table_key).push_back(std::move(table_info));
        }
    }

    atomic_store_explicit(&aggr_tables_, new_aggr_tables, std::memory_order_relaxed);
}

std::unique_ptr<::hybridse::vm::RowIterator> TabletSegmentHandler::GetIterator() {
    auto iter = partition_handler_->GetWindowIterator();
    if (iter) {
        DLOG(INFO) << "seek to pk " << key_;
        iter->Seek(key_);
        if (iter->Valid() && 0 == iter->GetKey().compare(hybridse::codec::Row(key_))) {
            return std::move(iter->GetValue());
        } else {
            return std::unique_ptr<::hybridse::vm::RowIterator>();
        }
    }
    return std::unique_ptr<::hybridse::vm::RowIterator>();
}

::hybridse::vm::RowIterator* TabletSegmentHandler::GetRawIterator() {
    auto iter = partition_handler_->GetWindowIterator();
    if (iter) {
        DLOG(INFO) << "seek to pk " << key_;
        iter->Seek(key_);
        if (iter->Valid() && 0 == iter->GetKey().compare(hybridse::codec::Row(key_))) {
            return iter->GetRawValue();
        } else {
            return nullptr;
        }
    }
    return nullptr;
}

const uint64_t TabletSegmentHandler::GetCount() {
    auto iter = GetIterator();
    if (!iter) return 0;
    uint64_t cnt = 0;
    while (iter->Valid()) {
        cnt++;
        iter->Next();
    }
    return cnt;
}

}  // namespace catalog
}  // namespace openmldb
