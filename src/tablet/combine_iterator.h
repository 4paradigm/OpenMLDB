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

#pragma once
#include <memory>
#include <string>
#include <vector>

#include "storage/table.h"

namespace openmldb {
namespace tablet {

__attribute__((unused)) static bool SeekWithCount(::openmldb::storage::TableIterator* it, const uint64_t time,
                                                  const ::openmldb::api::GetType& type, uint32_t max_cnt,
                                                  uint32_t* cnt) {
    if (it == nullptr) {
        return false;
    }
    it->SeekToFirst();
    while (it->Valid() && (*cnt < max_cnt || max_cnt == 0)) {
        switch (type) {
            case ::openmldb::api::GetType::kSubKeyEq:
                if (it->GetKey() <= time) {
                    return it->GetKey() == time;
                }
                break;
            case ::openmldb::api::GetType::kSubKeyLe:
                if (it->GetKey() <= time) {
                    return true;
                }
                break;
            case ::openmldb::api::GetType::kSubKeyLt:
                if (it->GetKey() < time) {
                    return true;
                }
                break;
            case ::openmldb::api::GetType::kSubKeyGe:
                return it->GetKey() >= time;
            case ::openmldb::api::GetType::kSubKeyGt:
                return it->GetKey() > time;
            default:
                return false;
        }
        it->Next();
        *cnt += 1;
    }
    return false;
}

__attribute__((unused)) static bool Seek(::openmldb::storage::TableIterator* it, const uint64_t time,
                                         const ::openmldb::api::GetType& type) {
    if (it == nullptr) {
        return false;
    }
    switch (type) {
        case ::openmldb::api::GetType::kSubKeyEq:
            it->Seek(time);
            return it->Valid() && it->GetKey() == time;
        case ::openmldb::api::GetType::kSubKeyLe:
            it->Seek(time);
            return it->Valid();
        case ::openmldb::api::GetType::kSubKeyLt:
            it->Seek(time - 1);
            return it->Valid();
        case ::openmldb::api::GetType::kSubKeyGe:
            it->SeekToFirst();
            return it->Valid() && it->GetKey() >= time;
        case ::openmldb::api::GetType::kSubKeyGt:
            it->SeekToFirst();
            return it->Valid() && it->GetKey() > time;
        default:
            return false;
    }
    return false;
}

__attribute__((unused)) static int GetIterator(std::shared_ptr<::openmldb::storage::Table> table, const std::string& pk,
                                               int index, std::shared_ptr<::openmldb::storage::TableIterator>* it,
                                               std::shared_ptr<::openmldb::storage::Ticket>* ticket) {
    if (it == nullptr || ticket == nullptr) {
        return -1;
    }
    if (!(*ticket)) {
        *ticket = std::make_shared<::openmldb::storage::Ticket>();
    }
    ::openmldb::storage::TableIterator* cur_it = nullptr;
    cur_it = table->NewIterator(index, pk, *(ticket->get()));
    if (cur_it == nullptr) {
        return -1;
    }
    it->reset(cur_it);
    return 0;
}

struct QueryIt {
    std::shared_ptr<::openmldb::storage::Table> table;
    std::shared_ptr<::openmldb::storage::TableIterator> it;
    std::shared_ptr<::openmldb::storage::Ticket> ticket;
    uint32_t iter_pos = 0;
};

class CombineIterator {
 public:
    CombineIterator(std::vector<QueryIt> q_its, uint64_t start_time, ::openmldb::api::GetType st_type,
                    const ::openmldb::storage::TTLSt& expired_value);
    void SeekToFirst();
    void Next();
    bool Valid();
    uint64_t GetTs();
    openmldb::base::Slice GetValue();
    inline uint64_t GetExpireTime() const { return expire_time_; }
    inline ::openmldb::storage::TTLType GetTTLType() const { return ttl_type_; }

 private:
    void SelectIterator();

 private:
    std::vector<QueryIt> q_its_;
    const uint64_t st_;
    ::openmldb::api::GetType st_type_;
    ::openmldb::storage::TTLType ttl_type_;
    uint64_t expire_time_;
    const uint32_t expire_cnt_;
    QueryIt* cur_qit_;
};

}  // namespace tablet
}  // namespace openmldb
