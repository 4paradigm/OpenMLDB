
// combine_iterator.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2020-06-04
//

#pragma once
#include <memory>
#include <string>
#include <vector>
#include "storage/table.h"

namespace rtidb {
namespace tablet {

__attribute__((unused)) static bool SeekWithCount(
    ::rtidb::storage::TableIterator* it, const uint64_t time,
    const ::rtidb::api::GetType& type, uint32_t max_cnt, uint32_t* cnt) {
    if (it == NULL) {
        return false;
    }
    it->SeekToFirst();
    while (it->Valid() && (*cnt < max_cnt || max_cnt == 0)) {
        switch (type) {
            case ::rtidb::api::GetType::kSubKeyEq:
                if (it->GetKey() <= time) {
                    return it->GetKey() == time;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyLe:
                if (it->GetKey() <= time) {
                    return true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyLt:
                if (it->GetKey() < time) {
                    return true;
                }
                break;
            case ::rtidb::api::GetType::kSubKeyGe:
                return it->GetKey() >= time;
            case ::rtidb::api::GetType::kSubKeyGt:
                return it->GetKey() > time;
            default:
                return false;
        }
        it->Next();
        *cnt += 1;
    }
    return false;
}

__attribute__((unused)) static bool Seek(::rtidb::storage::TableIterator* it,
                                         const uint64_t time,
                                         const ::rtidb::api::GetType& type) {
    if (it == NULL) {
        return false;
    }
    switch (type) {
        case ::rtidb::api::GetType::kSubKeyEq:
            it->Seek(time);
            return it->Valid() && it->GetKey() == time;
        case ::rtidb::api::GetType::kSubKeyLe:
            it->Seek(time);
            return it->Valid();
        case ::rtidb::api::GetType::kSubKeyLt:
            it->Seek(time - 1);
            return it->Valid();
        case ::rtidb::api::GetType::kSubKeyGe:
            it->SeekToFirst();
            return it->Valid() && it->GetKey() >= time;
        case ::rtidb::api::GetType::kSubKeyGt:
            it->SeekToFirst();
            return it->Valid() && it->GetKey() > time;
        default:
            return false;
    }
    return false;
}

__attribute__((unused)) static int GetIterator(
    std::shared_ptr<::rtidb::storage::Table> table, const std::string& pk,
    int index, int ts_index,
    std::shared_ptr<::rtidb::storage::TableIterator>* it,
    std::shared_ptr<::rtidb::storage::Ticket>* ticket) {
    if (it == NULL || ticket == NULL) {
        return -1;
    }
    if (!(*ticket)) {
        *ticket = std::make_shared<::rtidb::storage::Ticket>();
    }
    ::rtidb::storage::TableIterator* cur_it = NULL;
    if (ts_index >= 0) {
        cur_it = table->NewIterator(index, ts_index, pk, *(ticket->get()));
    } else {
        cur_it = table->NewIterator(index, pk, *(ticket->get()));
    }
    if (cur_it == NULL) {
        return -1;
    }
    it->reset(cur_it);
    return 0;
}

struct QueryIt {
    std::shared_ptr<::rtidb::storage::Table> table;
    std::shared_ptr<::rtidb::storage::TableIterator> it;
    std::shared_ptr<::rtidb::storage::Ticket> ticket;
    uint32_t iter_pos = 0;
};

class CombineIterator {
 public:
    CombineIterator(std::vector<QueryIt> q_its, uint64_t start_time,
                    ::rtidb::api::GetType st_type, const ::rtidb::storage::TTLSt& expired_value);
    void SeekToFirst();
    void Next();
    bool Valid();
    uint64_t GetTs();
    rtidb::base::Slice GetValue();
    inline uint64_t GetExpireTime() const { return expire_time_; }
    inline ::rtidb::storage::TTLType GetTTLType() const { return ttl_type_; }

 private:
    void SelectIterator();

 private:
    std::vector<QueryIt> q_its_;
    const uint64_t st_;
    ::rtidb::api::GetType st_type_;
    ::rtidb::storage::TTLType ttl_type_;
    uint64_t expire_time_;
    const uint32_t expire_cnt_;
    QueryIt* cur_qit_;
};

}  // namespace tablet
}  // namespace rtidb
