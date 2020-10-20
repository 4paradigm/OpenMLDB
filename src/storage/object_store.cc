//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-14

#include "storage/object_store.h"

#include <mutex>
#include <utility>

#include "codec/codec.h"

namespace rtidb {
namespace storage {

static std::once_flag options_initialized;

ObjectStore::ObjectStore(const ::rtidb::blobserver::TableMeta& table_meta,
                         std::string db_root_path, uint32_t flush_size, int32_t flush_period)
    : db_(nullptr),
      tid_(table_meta.tid()),
      pid_(table_meta.pid()),
      name_(table_meta.name()),
      db_root_path_(std::move(db_root_path)),
      is_leader_(false),
      storage_mode_(table_meta.storage_mode()),
      id_generator_(),
      flush_size_(flush_size),
      flush_period_(flush_period) {}

bool ObjectStore::Init() {
    std::call_once(options_initialized, settings_init);
    char* path = const_cast<char*>(db_root_path_.data());
    time_t before_time = 0;
    db_ = hs_open(path, 1, before_time, 1);
    return true;
}

ObjectStore::~ObjectStore() {
    DoFlash();
    hs_close(db_);
}

bool ObjectStore::Store(int64_t key, const std::string& value) {
    std::string skey = codec::Int64ToString(key);
    char* hs_key = const_cast<char*>(skey.data());
    char* hs_value = const_cast<char*>(value.data());
    return hs_set(db_, hs_key, hs_value, value.length(), 0, 0);
}

bool ObjectStore::Store(const std::string& value, int64_t* key) {
    *key = id_generator_.Next();
    return Store(*key, value);
}

rtidb::base::Slice ObjectStore::Get(int64_t key) {
    std::string skey = codec::Int64ToString(key);
    char* hs_key = const_cast<char*>(skey.data());
    uint32_t vlen = 0, flag;
    char* ch = hs_get(db_, hs_key, &vlen, &flag);
    if (ch == NULL) {
        return rtidb::base::Slice();
    }
    return rtidb::base::Slice(ch, vlen);
}

bool ObjectStore::Delete(int64_t key) {
    std::string skey = codec::Int64ToString(key);
    char* hs_key = const_cast<char*>(skey.data());
    return hs_delete(db_, hs_key);
}

void ObjectStore::DoFlash() {
    hs_flush(db_, flush_size_, flush_period_);
}

::rtidb::common::StorageMode ObjectStore::GetStorageMode() const {
    return storage_mode_;
}

}  // namespace storage
}  // namespace rtidb
