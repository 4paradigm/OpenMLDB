//
// tablet_metric.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-15 
// 


#ifndef RTIDB_TABLET_METRIC_H
#define RTIDB_TABLET_METRIC_H

#include "storage/table.h"

#include <atomic>
#include "thread_pool.h"

using ::baidu::common::ThreadPool;
namespace rtidb {
namespace tablet {

class TabletMetric {

public:
    TabletMetric(std::shared_ptr<::rtidb::storage::Table> stat);
    ~TabletMetric();

    void Init();

    void IncrThroughput(
        const uint64_t& put_count,
        const uint64_t& put_bytes,
        const uint64_t& scan_count,
        const uint64_t& scan_bytes);
private:
    void CollectMemoryStat();
    void CollectThroughput();
    void Collect();
private:
    std::shared_ptr<::rtidb::storage::Table> stat_;
    ThreadPool bg_pool_;

    // throughput 
    // the first put qps
    // the second put bandwidth
    // the third scan qps
    // the fourth scan bandwidth
    std::atomic<uint64_t>* throughput_;
    uint64_t* last_throughput_;
};

}
}
#endif /* !TABLET_METRIC_H */
