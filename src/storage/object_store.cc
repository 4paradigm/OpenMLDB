#include "storage/object_store.h"

namespace rtidb {
namespace storage {

ObjectStore::ObjectStore(const ::rtidb::api::TableMeta &table_meta, const std::string &db_root_path):
    db_(NULL), tid_(table_meta.tid()), pid_(table_meta.pid()), name_(table_meta.name()),
    db_root_path_(db_root_path), is_leader_(false) {
}

bool ObjectStore::Init() {
    char* path = (char*)malloc(sizeof(char)*db_root_path_.length() + 1);
    memcpy(path, db_root_path_.c_str(), db_root_path_.length());
    db_ = hs_open(path, 1, 0, 16);
}

}
}

