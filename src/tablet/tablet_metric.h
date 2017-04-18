//
// tablet_metric.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-15 
// 


#ifndef RTIDB_TABLET_METRIC_H
#define RTIDB_TABLET_METRIC_H

#include "storage/table.h"

#include "gperftools/malloc_extension.h"
#include "thread_pool.h"

using ::baidu::common::ThreadPool;
namespace rtidb {
namespace tablet {

class TabletMetric {

public:
    TabletMetric(::rtidb::storage::Table* stat);
    ~TabletMetric();

    void Init();
private:
    void GenMemoryStat();
private:
    ::rtidb::storage::Table* stat_;
    MallocExtension* extension_;
    ThreadPool bg_pool_;
};

}
}
#endif /* !TABLET_METRIC_H */
