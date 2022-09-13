/*
 *  Copyright 2021 4Paradigm
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#ifndef STREAM_SRC_EXECUTOR_DYNAMIC_INTERVAL_JOIN_H_
#define STREAM_SRC_EXECUTOR_DYNAMIC_INTERVAL_JOIN_H_

#include <gtest/gtest_prod.h>
#include <limits>
#include <memory>
#include <vector>

#include "executor/interval_join.h"
#include "structure/table.h"
#include "executor/partition_strategy.h"

namespace streaming {
namespace interval_join {

class DynamicIntervalJoiner : public IntervalJoiner {
 public:
    FRIEND_TEST(IntervalJoinTest, SnapshotTest);
    using GCEntry = std::vector<::openmldb::base::Node<uint64_t, DataBlock*>*>;

    DynamicIntervalJoiner(const JoinConfiguration* configuration, const std::shared_ptr<Buffer>& buffer,
                          DataStream* output, PartitionStrategy* strategy, int id = 0,
                          const Clock* clock = &Clock::GetClock())
        : IntervalJoiner(configuration, buffer, output, id, clock),
          gc_entries_(std::make_shared<std::vector<GCEntry>>()),
          partition_strategy_(strategy) {
        peers_.resize(id + 1);
        peers_[id] = this;
    }

    void SetPeerJoiners(const std::vector<const DynamicIntervalJoiner*>& joiners) {
        DCHECK(peers_[id()] == this);
        peers_ = joiners;
    }

    inline const SkipListTable& GetBaseTable() const {
        return processed_base_;
    }

    inline const SkipListTable& GetProbeTable() const {
        return processed_probe_;
    }

    int ProcessBaseElement(Element* ele) override;
    int ProcessProbeElement(Element* ele) override;

    std::shared_ptr<std::vector<GCEntry>> GetAndReleaseGCEntries() {
        std::shared_ptr<std::vector<GCEntry>> ret = gc_entries_;
        std::atomic_store_explicit(&gc_entries_, std::make_shared<std::vector<GCEntry>>(), std::memory_order_relaxed);
        return gc_entries_;
    }

 private:
    bool DoCleanup(size_t curr_ts) override;

    static inline bool IsVisible(int64_t sys_ts, int64_t other_sys_ts) {
        DCHECK(sys_ts != std::numeric_limits<int64_t>::max());
        DCHECK(other_sys_ts != sys_ts);
        return sys_ts >= other_sys_ts;
    }

    SkipListTable processed_base_;
    SkipListTable processed_probe_;
    std::shared_ptr<std::vector<GCEntry>> gc_entries_;
    std::vector<const DynamicIntervalJoiner*> peers_;
    PartitionStrategy* partition_strategy_ = nullptr;
};

}  // namespace interval_join
}  // namespace streaming

#endif  // STREAM_SRC_EXECUTOR_DYNAMIC_INTERVAL_JOIN_H_
