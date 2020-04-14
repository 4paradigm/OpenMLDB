//
// Created by kongsys on 4/13/20.
//
#pragma once
#include "storage/beans/hstore.h"
#include <string>
#include "proto/tablet.pb.h"

namespace rtidb {
namespace storage {

class ObjectStore {
public:
    ObjectStore(const ::rtidb::api::TableMeta& table_meta, const std::string& db_root_path);

    bool Init();

    ObjectStore(const ObjectStore&) = delete;
    ObjectStore& operator=(const ObjectStore&) = delete;
    ~ObjectStore() {
        if (db_ != NULL) {
            hs_close(db_);
            delete db_;
        }
    }
private:
    HStore* db_;
    uint32_t tid_;
    uint32_t pid_;
    std::string name_;
    std::string db_root_path_;
    bool is_leader_;
    char* path_;

};

}
}

