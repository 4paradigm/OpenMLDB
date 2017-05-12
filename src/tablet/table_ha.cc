//
// table_ha.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-24
//

#include "tablet/table_ha.h"
#include "leveldb/options.h"
#include <boost/lexical_cast.hpp>
#include "logging.h"
#include "timer.h"

using ::baidu::common::INFO;
using ::baidu::common::DEBUG;
using ::baidu::common::WARNING;

namespace rtidb {
namespace tablet {

const static leveldb::Slice TABLE_META_KEY("/TABLE_META_KEY/");
const static std::string DATA_PREFIX = "/TABLE_DATA/";

TableDataHA::TableDataHA(const std::string& db_path,
        const std::string& tname):db_path_(db_path),
    db_(NULL), tname_(tname), ref_(0){}


TableDataHA::~TableDataHA() {
    delete db_;
}

bool TableDataHA::Init() {
    leveldb::Options options;
    options.create_if_missing = true;
    leveldb::Status status = leveldb::DB::Open(options,
                                              db_path_, 
                                              &db_);
    if (status.ok()) {
        LOG(INFO, "create persistence db for table %s ok", tname_.c_str());
        return true;
    }
    LOG(WARNING, "fail to create persistence db for table %s with error %s",
            tname_.c_str(), status.ToString().c_str());
    return false;
}

bool TableDataHA::SaveMeta(const TableMeta& meta) {
    std::string buffer;
    meta.SerializeToString(&buffer);
    const leveldb::Slice value(buffer);
    leveldb::WriteOptions options;
    // use sync write
    options.sync = true;
    leveldb::Status status = db_->Put(options, TABLE_META_KEY, value);
    if (status.ok()) {
        return true;
    }
    LOG(WARNING, "fail to save meta for table %s with error %s",
            tname_.c_str(),
            status.ToString().c_str());
    return false;
}

bool TableDataHA::Put(const TableRow& row) {
    std::string buffer;
    row.SerializeToString(&buffer);
    const leveldb::Slice value(buffer);
    std::string key = DATA_PREFIX + row.pk() + boost::lexical_cast<std::string>(row.time());
    leveldb::WriteOptions options;
    options.sync = false;
    leveldb::Status status = db_->Put(options, key, value);
    if (status.ok()) {
        return true;
    }
    LOG(WARNING, "fail to put data for table %s with error %s",
            tname_.c_str(),
            status.ToString().c_str());
    return false;
}

void TableDataHA::Ref() {
    ref_.fetch_add(1, boost::memory_order_relaxed);
}

void TableDataHA::UnRef() {
    ref_.fetch_sub(1, boost::memory_order_acquire);
    if (ref_.load(boost::memory_order_acquire) <= 0) {
        delete db_;
        delete this;
    }
}

bool TableDataHA::Recover(::rtidb::storage::Table** table) {
    if (table == NULL) {
        return false;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    ::leveldb::ReadOptions options;
    std::string table_meta_str;
    leveldb::Status status = db_->Get(options, TABLE_META_KEY, &table_meta_str);
    if (!status.ok()) {
        LOG(WARNING, "fail to get table meta for %s", tname_.c_str());
        return false;
    }
    TableMeta meta;
    meta.ParseFromString(table_meta_str);
    ::rtidb::storage::Table* t = new ::rtidb::storage::Table(meta.name(),
            meta.tid(), meta.pid(), meta.seg_cnt(), meta.ttl());
    t->Init();
    LOG(WARNING, "Recover tablet %s meta successfully", tname_.c_str());
    *table = t;
    leveldb::Iterator* it = db_->NewIterator(options);
    it->Seek(DATA_PREFIX);
    TableRow row;
    while (it->Valid()) {
        leveldb::Slice data = it->value();
        bool ok = row.ParseFromString(data.ToString());
        if (ok) {
            t->Put(row.pk(), row.time(), row.data().c_str(),
                    row.data().size());
        }
        it->Next();
    }
    consumed = ::baidu::common::timer::get_micros() - consumed;
    LOG(WARNING, "recover tablet %s data successfully with time %ld ms", tname_.c_str(),
            consumed/1000);
    return true;
}

}
}



