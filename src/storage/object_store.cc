#include "storage/object_store.h"

namespace rtidb {
namespace storage {
ObjectStore::ObjectStore(const ::rtidb::api::TableMeta &table_meta, const std::string &db_root_path):
    db_(NULL), tid_(table_meta.tid()), pid_(table_meta.pid()), name_(table_meta.name()),
    db_root_path_(db_root_path), is_leader_(false), path_(NULL) {
}

bool ObjectStore::Init() {
    path_ = (char*)malloc(sizeof(char)*db_root_path_.length() + 1);
    memcpy(path_, db_root_path_.c_str(), db_root_path_.length());
    db_ = hs_open(path_, 1, 0, 16);
    return true;
}

ObjectStore::~ObjectStore() {
    DoFlash();
    hs_close(db_);
    if (path_ != NULL) {
        free(path_);
    }
}

bool ObjectStore::Store(const std::string &key, const std::string &value) {
    char* hs_key = (char*) malloc(sizeof(char)*key.length());
    char* hs_value = (char*) malloc(sizeof(char)*value.length());
    bool ret = hs_set(db_, hs_key, hs_value, value.length(), 0, 0);
    free(hs_key);
    free(hs_value);
    return ret;
}

rtidb::base::Slice ObjectStore::Get(const std::string& key) {
    char* hs_key = (char*) malloc(sizeof(char)*key.length());
    memcpy(hs_key, key.c_str(), key.length());
    uint32_t vlen = 0, flag;
    char* ch = hs_get(db_, hs_key, &vlen, &flag);
    free(hs_key);
    if (ch == NULL) {
        return rtidb::base::Slice();
    }
    return rtidb::base::Slice(ch, vlen);
}

void ObjectStore::DoFlash() {
    hs_flush(db_, 1024, 60 * 10);
}

}
}

