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

namespace fedb {
namespace tablet {

__attribute__((unused)) static bool SeekWithCount(
    ::fedb::storage::TableIterator* it, const uint64_t time,
    const ::fedb::api::GetType& type, uint32_t max_cnt, uint32_t* cnt) {
    if (it == NULL) {
        return false;
    }
    it->SeekToFirst();
    while (it->Valid() && (*cnt < max_cnt || max_cnt == 0)) {
        switch (type) {
            case ::fedb::api::GetType::kSubKeyEq:
                if (it->GetKey() <= time) {
                    return it->GetKey() == time;
                }
                break;
            case ::fedb::api::GetType::kSubKeyLe:
                if (it->GetKey() <= time) {
                    return true;
                }
                break;
            case ::fedb::api::GetType::kSubKeyLt:
                if (it->GetKey() < time) {
                    return true;
                }
                break;
            case ::fedb::api::GetType::kSubKeyGe:
                return it->GetKey() >= time;
            case ::fedb::api::GetType::kSubKeyGt:
                return it->GetKey() > time;
            default:
                return false;
        }
        it->Next();
        *cnt += 1;
    }
    return false;
}

__attribute__((unused)) static bool Seek(::fedb::storage::TableIterator* it,
                                         const uint64_t time,
                                         const ::fedb::api::GetType& type) {
    if (it == NULL) {
        return false;
    }
    switch (type) {
        case ::fedb::api::GetType::kSubKeyEq:
            it->Seek(time);
            return it->Valid() && it->GetKey() == time;
        case ::fedb::api::GetType::kSubKeyLe:
            it->Seek(time);
            return it->Valid();
        case ::fedb::api::GetType::kSubKeyLt:
            it->Seek(time - 1);
            return it->Valid();
        case ::fedb::api::GetType::kSubKeyGe:
            it->SeekToFirst();
            return it->Valid() && it->GetKey() >= time;
        case ::fedb::api::GetType::kSubKeyGt:
            it->SeekToFirst();
            return it->Valid() && it->GetKey() > time;
        default:
            return false;
    }
    return false;
}

__attribute__((unused)) static int GetIterator(
    std::shared_ptr<::fedb::storage::Table> table, const std::string& pk, int index,
    std::shared_ptr<::fedb::storage::TableIterator>* it,
    std::shared_ptr<::fedb::storage::Ticket>* ticket) {
    if (it == NULL || ticket == NULL) {
        return -1;
    }
    if (!(*ticket)) {
        *ticket = std::make_shared<::fedb::storage::Ticket>();
    }
    ::fedb::storage::TableIterator* cur_it = NULL;
    cur_it = table->NewIterator(index, pk, *(ticket->get()));
    if (cur_it == NULL) {
        return -1;
    }
    it->reset(cur_it);
    return 0;
}

struct QueryIt {
    std::shared_ptr<::fedb::storage::Table> table;
    std::shared_ptr<::fedb::storage::TableIterator> it;
    std::shared_ptr<::fedb::storage::Ticket> ticket;
    uint32_t iter_pos = 0;
};

class CombineIterator {
 public:
    CombineIterator(std::vector<QueryIt> q_its, uint64_t start_time,
                    ::fedb::api::GetType st_type, const ::fedb::storage::TTLSt& expired_value);
    void SeekToFirst();
    void Next();
    bool Valid();
    uint64_t GetTs();
    fedb::base::Slice GetValue();
    inline uint64_t GetExpireTime() const { return expire_time_; }
    inline ::fedb::storage::TTLType GetTTLType() const { return ttl_type_; }

 private:
    void SelectIterator();

 private:
    std::vector<QueryIt> q_its_;
    const uint64_t st_;
    ::fedb::api::GetType st_type_;
    ::fedb::storage::TTLType ttl_type_;
    uint64_t expire_time_;
    const uint32_t expire_cnt_;
    QueryIt* cur_qit_;
};

}  // namespace tablet
}  // namespace fedb
