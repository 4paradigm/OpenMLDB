//
// tablet_metric.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-18
//

#include "tablet/tablet_metric.h"

#include <boost/bind.hpp>
#include "time.h"

namespace rtidb {
namespace tablet {

TabletMetric::TabletMetric(::rtidb::storage::Table* stat):stat_(stat),
    extension_(NULL), bg_pool_(){
   extension_ = MallocExtension::instance();
}


TabletMetric::~TabletMetric() {
    stat_->UnRef();
}


void TabletMetric::Init() {
    bg_pool_.DelayTask(60 * 1000, boost::bind(&TabletMetric::GenMemoryStat, this));
}

void TabletMetric::GenMemoryStat() {
    uint64_t now = ::baidu::common::timer::now_time();
    char buffer[4];
    size_t allocated = 0;
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
    bg_pool_.DelayTask(60 * 1000, boost::bind(&TabletMetric::GenMemoryStat, this));
}

}
}



