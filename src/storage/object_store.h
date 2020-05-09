//
// Copyright (C) 2020 4paradigm.com
// Author kongquan
// Date 2020-04-14

#pragma once
#include <string>
#ifdef __cplusplus
extern "C" {
#endif
#include "storage/beans/hstore.h"
#ifdef __cplusplus
};
#endif
#include <base/id_generator.h>
#include <base/slice.h>

#include "proto/blob_server.pb.h"
#include "proto/common.pb.h"

namespace rtidb {
namespace storage {

class ObjectStore {
 public:
    ObjectStore(const ::rtidb::blobserver::TableMeta& table_meta,
                const std::string& db_root_path);

    bool Init();

    uint32_t tid() { return tid_; }

    ObjectStore(const ObjectStore&) = delete;
    ObjectStore& operator=(const ObjectStore&) = delete;
    ~ObjectStore();

    bool Store(int64_t key, const std::string& value);

    bool Store(int64_t* key, const std::string& value);

    bool Delete(int64_t key);

    rtidb::base::Slice Get(int64_t key);

    ::rtidb::common::StorageMode GetStorageMode() const;

 private:
    void DoFlash();

    HStore* db_;
    uint32_t tid_;
    uint32_t pid_;
    std::string name_;
    std::string db_root_path_;
    bool is_leader_;
    ::rtidb::common::StorageMode storage_mode_;
    ::rtidb::base::IdGenerator id_generator_;
};

}  // namespace storage
}  // namespace rtidb
