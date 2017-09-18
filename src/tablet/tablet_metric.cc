//
// tablet_metric.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-18
//

#include "config.h"
#include "tablet/tablet_metric.h"
#ifdef TCMALLOC_ENABLE
#include "gperftools/malloc_extension.h"
#endif
#include <boost/bind.hpp>
#include "time.h"

namespace rtidb {
namespace tablet {

TabletMetric::TabletMetric(::rtidb::storage::Table* stat):stat_(stat),
    bg_pool_(), throughput_(NULL){
   throughput_ = new boost::atomic<uint64_t>[4];
   // record put counter
   throughput_[0].store(0, boost::memory_order_relaxed);
   // record put bandwidth
   throughput_[1].store(0, boost::memory_order_relaxed);
   // record scan counter
   throughput_[2].store(0, boost::memory_order_relaxed);
   // record scan bandwidth
   throughput_[3].store(0, boost::memory_order_relaxed);
   last_throughput_ = new uint64_t[4];
   last_throughput_[0] = 0;
   last_throughput_[1] = 0;
   last_throughput_[2] = 0;
   last_throughput_[3] = 0;
}


TabletMetric::~TabletMetric() {
    stat_->UnRef();
    delete[] throughput_;
    delete[] last_throughput_;
}


void TabletMetric::Init() {
    bg_pool_.DelayTask(60 * 1000, boost::bind(&TabletMetric::Collect, this));
}

void TabletMetric::IncrThroughput(const uint64_t& put_count,
                                  const uint64_t& put_bytes,
                                  const uint64_t& scan_count,
                                  const uint64_t& scan_bytes) {
    if (put_count > 0) {
        throughput_[0].fetch_add(put_count, boost::memory_order_relaxed);
    }
    if (put_bytes > 0) {
        throughput_[1].fetch_add(put_bytes, boost::memory_order_relaxed);
    }
    if (scan_count > 0) {
        throughput_[2].fetch_add(scan_count, boost::memory_order_relaxed);
    }
    if (scan_bytes > 0) {
        throughput_[3].fetch_add(scan_bytes, boost::memory_order_relaxed);
    }
}

void TabletMetric::CollectThroughput() {
    uint64_t now = ::baidu::common::timer::now_time() * 1000;
    char buffer[8];
    uint64_t current_put_count = throughput_[0].load(boost::memory_order_relaxed);
    uint64_t put_qps = (current_put_count - last_throughput_[0]) / 60;
    last_throughput_[0] = current_put_count;
    memcpy(buffer, static_cast<const void*>(&put_qps), 8);
    stat_->Put("throughput.put_qps", now, buffer, 8);

    uint64_t current_put_bytes = throughput_[1].load(boost::memory_order_relaxed);
    uint64_t put_bandwidth = (current_put_bytes - last_throughput_[1]) / 60;
    last_throughput_[1] = current_put_bytes;
    memcpy(buffer, static_cast<const void*>(&put_bandwidth), 8);
    stat_->Put("throughput.put_bandwidth", now, buffer, 8);

    uint64_t current_scan_count = throughput_[2].load(boost::memory_order_relaxed);
    uint64_t scan_qps = (current_scan_count - last_throughput_[2]) / 60;
    last_throughput_[2] = current_scan_count;
    memcpy(buffer, static_cast<const void*>(&scan_qps), 8);
    stat_->Put("throughput.scan_qps", now, buffer, 8);

    uint64_t current_scan_bytes = throughput_[3].load(boost::memory_order_relaxed);
    uint64_t scan_bandwidth = (current_scan_bytes - last_throughput_[3]) / 60;
    last_throughput_[3] = current_scan_bytes;
    memcpy(buffer, static_cast<const void*>(&scan_bandwidth), 8);
    stat_->Put("throughput.scan_bandwidth", now, buffer, 8);

}

void TabletMetric::Collect() {
    CollectThroughput();
    CollectMemoryStat();
    bg_pool_.DelayTask(60 * 1000, boost::bind(&TabletMetric::Collect, this));
}

void TabletMetric::CollectMemoryStat() {
#ifdef TCMALLOC_ENABLE
    uint64_t now = ::baidu::common::timer::now_time();
    char buffer[4];
    size_t allocated = 0;
    MallocExtension* extension_ = MallocExtension::instance();
    extension_->GetNumericProperty("generic.current_allocated_bytes", &allocated);
    memcpy(buffer, static_cast<const void*>(&allocated), 4);
    stat_->Put("mem.allocated", now, buffer, 4);
    size_t heap_size = 0;
    extension_->GetNumericProperty("generic.heap_size", &heap_size);
    memcpy(buffer, static_cast<const void*>(&heap_size), 4);
    stat_->Put("mem.heap_size", now, buffer, 4);
    size_t heap_free = 0;
    extension_->GetNumericProperty("tcmalloc.pageheap_free_bytes", &heap_free);
    memcpy(buffer, static_cast<const void*>(&heap_free), 4);
    stat_->Put("mem.heap_free", now, buffer, 4);
#endif
}

}
}



