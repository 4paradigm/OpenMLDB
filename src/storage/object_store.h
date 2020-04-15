//
// Created by kongsys on 4/13/20.
//
#pragma once
#include <string>
#ifdef __cplusplus
extern "C" {
#endif
#include "storage/beans/hstore.h"
#ifdef __cplusplus
};
#endif
#include <base/slice.h>
#include "proto/tablet.pb.h"

namespace rtidb {
namespace storage {

class ObjectStore {
public:
    ObjectStore(const ::rtidb::api::TableMeta& table_meta, const std::string& db_root_path);

    bool Init();

    uint32_t tid() { return tid_; };

    ObjectStore(const ObjectStore&) = delete;
    ObjectStore& operator=(const ObjectStore&) = delete;
    ~ObjectStore();

    bool Store(const std::string& key, const std::string& value);

    rtidb::base::Slice Get(const std::string& key);

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
};

}
}

