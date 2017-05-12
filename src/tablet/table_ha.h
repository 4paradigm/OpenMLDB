//
// table_ha.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-24
//


#ifndef RTIDB_TABLE_HA_H
#define RTIDB_TABLE_HA_H

#include "leveldb/db.h"
#include "proto/tablet.pb.h"
#include "storage/table.h"
#include <boost/atomic.hpp>

using ::rtidb::api::TableMeta;
using ::rtidb::api::TableRow;

namespace rtidb {
namespace tablet {

// table data ha
class TableDataHA {

public:
    TableDataHA(const std::string& db_path,
            const std::string& tname);

    bool Init();

    bool SaveMeta(const TableMeta& meta);

    bool Put(const TableRow& row);

    void Ref();

    void UnRef();

    bool Recover(::rtidb::storage::Table** table);

private:
    ~TableDataHA();
private:
    std::string db_path_;
    leveldb::DB* db_;
    std::string tname_;
    boost::atomic<uint32_t> ref_;
};

}
}
#endif /* !TABLE_HA_H */
