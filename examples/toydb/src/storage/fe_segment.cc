/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */


#include "fe_segment.h"
#include <mutex>  //NOLINT

namespace fesql {
namespace storage {

Segment::Segment() : entries_(NULL), mu_() {
    entries_ = new KeyEntry(KEY_ENTRY_MAX_HEIGHT, 4, scmp);
}

Segment::~Segment() { delete entries_; }

void Segment::Put(const base::Slice& key, uint64_t time, DataBlock* row) {
    void* entry = NULL;
    std::lock_guard<base::SpinMutex> lock(mu_);
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == NULL) {
        entry = reinterpret_cast<void*>(new TimeEntry(tcmp));
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        base::Slice skey(pk, key.size());
        entries_->Insert(skey, entry);
    }
    reinterpret_cast<TimeEntry*>(entry)->Insert(time, row);
}

}  // namespace storage
}  // namespace fesql
