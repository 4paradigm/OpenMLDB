//
// mem_log.cc
// Copyright 2017 elasticlog <elasticlog01@gmail.com>
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

#include "replica/mem_log.h"

namespace rtidb {
namespace replica {
const static LogOffsetComparator scmp;

MemLog::MemLog():logs_(12, 4, scmp), refs_(0), head_(0), end_(0) {}

MemLog::~MemLog() {
    Logs::Iterator* it = logs_.NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        LogEntry* entry = it->GetValue();
        delete entry;
        it->Next();
    }
    delete it;
    logs_.Clear();
}

void MemLog::AppendLog(LogEntry* entry, uint64_t offset) {
    logs_.Insert(offset, entry);
    if (head_.load(boost::memory_order_relaxed) <= 0) {
        head_.store(offset, boost::memory_order_relaxed);
    }
    end_.store(offset, boost::memory_order_relaxed);
}

void MemLog::Ref() {
    refs_.fetch_add(1, boost::memory_order_relaxed);
}

void MemLog::UnRef() {
    refs_.fetch_sub(1, boost::memory_order_acquire);
    if (refs_.load(boost::memory_order_relaxed) <= 0) {
        delete this;
    }
}

uint64_t MemLog::GetHeadOffset() {
    return head_.load(boost::memory_order_relaxed);
}

uint64_t MemLog::GetTailOffset() {
    return end_.load(boost::memory_order_relaxed);
}

Logs::Iterator* MemLog::NewIterator() {
    return logs_.NewIterator();
}

}
}

