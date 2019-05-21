//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31
//

#include "storage/segment.h"

#include "storage/record.h"
#include "base/strings.h"
#include "logging.h"
#include "timer.h"
#include <gflags/gflags.h>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

DECLARE_uint32(skiplist_max_height);
DECLARE_uint32(gc_deleted_pk_version_delta);

namespace rtidb {
namespace storage {

const static SliceComparator scmp;
Segment::Segment():entries_(NULL),mu_(), idx_cnt_(0), idx_byte_size_(0), pk_cnt_(0), ts_cnt_(1), gc_version_(0) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    key_entry_max_height_ = (uint8_t)FLAGS_skiplist_max_height;
    entry_free_list_ = new KeyEntryNodeList(4, 4, tcmp);
    multi_entries_ = NULL;
    multi_entry_free_list_ = NULL;
}

Segment::Segment(uint8_t height):entries_(NULL),mu_(), idx_cnt_(0), idx_byte_size_(0), pk_cnt_(0), 
        key_entry_max_height_(height), ts_cnt_(1), gc_version_(0) {
    entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
    entry_free_list_ = new KeyEntryNodeList(4, 4, tcmp);
    multi_entries_ = NULL;
    multi_entry_free_list_ = NULL;
}

Segment::Segment(uint8_t height, const std::vector<uint32_t>& ts_idx_vec):
        entries_(NULL),mu_(), idx_cnt_(0), idx_byte_size_(0), pk_cnt_(0), 
        key_entry_max_height_(height), ts_cnt_(ts_idx_vec.size()), gc_version_(0) {
    if (ts_idx_vec.size() > 1) {
        entries_ = NULL;
        entry_free_list_ = NULL;
        multi_entries_ = new KeyMultiEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
        multi_entry_free_list_ = new MultiKeyEntryNodeList(4, 4, tcmp);
        for (uint32_t i = 0; i < ts_idx_vec.size(); i++) {
            ts_idx_map_[ts_idx_vec[i]] = i;
        }
    } else {
        entries_ = new KeyEntries((uint8_t)FLAGS_skiplist_max_height, 4, scmp);
        multi_entries_ = NULL;
        entry_free_list_ = new KeyEntryNodeList(4, 4, tcmp);
        multi_entry_free_list_ = NULL;
        if (!ts_idx_vec.empty()) {
            ts_idx_map_[ts_idx_vec[0]] = 0;
        }
    }
}

Segment::~Segment() {
    if (entries_) delete entries_;
    if (multi_entries_) delete multi_entries_;
    if (entry_free_list_) delete entry_free_list_;
    if (multi_entry_free_list_) delete multi_entry_free_list_;
}

uint64_t Segment::Release() {
    uint64_t cnt = 0;
    if (entries_) {
        KeyEntries::Iterator* it = entries_->NewIterator();
        it->SeekToFirst();
        while (it->Valid()) {
            if (it->GetValue() != NULL) {
                cnt += it->GetValue()->Release();
            }
            delete it->GetValue();
            it->Next();
        }
        entries_->Clear();
        delete it;
    }

    if (multi_entries_) {
        KeyMultiEntries::Iterator* it = multi_entries_->NewIterator();
        it->SeekToFirst();
        while (it->Valid()) {
            if (it->GetValue() != NULL) {
                KeyEntry** entry_arr = it->GetValue();
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    cnt += entry_arr[i]->Release();
                }
            }
            delete[] it->GetValue();
            it->Next();
        }
        multi_entries_->Clear();
        delete it;
    }

    if (entry_free_list_) {
        KeyEntryNodeList::Iterator* f_it = entry_free_list_->NewIterator();
        f_it->SeekToFirst();
        while (f_it->Valid()) {
            ::rtidb::base::Node<Slice, KeyEntry*>* node = f_it->GetValue();
            delete node->GetKey().data();
            KeyEntry* entry = node->GetValue();
            entry->Release();
            delete entry;
            delete node;
            f_it->Next();
        }
        delete f_it;
        entry_free_list_->Clear();
    }

    if (multi_entry_free_list_) {
        MultiKeyEntryNodeList::Iterator* f_it = multi_entry_free_list_->NewIterator();
        f_it->SeekToFirst();
        while (f_it->Valid()) {
            ::rtidb::base::Node<Slice, KeyEntry**>* node = f_it->GetValue();
            delete node->GetKey().data();
            KeyEntry** entry_arr = node->GetValue();
            for (uint32_t i = 0; i < ts_cnt_; i++) {
                entry_arr[i]->Release();
            }
            delete[] entry_arr;
            delete node;
            f_it->Next();
        }
        delete f_it;
        multi_entry_free_list_->Clear();

    }
    return cnt;
}

void Segment::Put(const Slice& key,
        uint64_t time,
        const char* data,
        uint32_t size) {
    if (ts_cnt_ > 1) {
        return;
    }
    DataBlock* db = new DataBlock(1, data, size);
    Put(key, time, db);
}

void Segment::Put(const Slice& key, uint64_t time, DataBlock* row) {
    if (ts_cnt_ > 1) {
        return;
    }
    KeyEntry* entry = NULL;
    uint32_t byte_size = 0; 
    std::lock_guard<std::mutex> lock(mu_);
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == NULL) {
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        // need to delete memory when free node
        Slice skey(pk, key.size());
        entry = new KeyEntry(key_entry_max_height_);
        uint8_t height = entries_->Insert(skey, entry);
        byte_size += GetRecordPkIdxSize(height, key.size(), key_entry_max_height_);
        pk_cnt_.fetch_add(1, std::memory_order_relaxed);
    }
    idx_cnt_.fetch_add(1, std::memory_order_relaxed);
    uint8_t height = entry->entries.Insert(time, row);
    entry->count_.fetch_add(1, std::memory_order_relaxed);
    byte_size += GetRecordTsIdxSize(height);
    idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
}

void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row) {
    uint32_t ts_size = ts_dimension.size();
    if (ts_size == 0) {
        return;
    }
    if (ts_cnt_ == 1) {
        if (ts_size == 1) {
            Put(key, ts_dimension.begin()->ts(), row);
        } else if (!ts_idx_map_.empty()) {
            for (const auto& cur_ts : ts_dimension) {
                auto pos = ts_idx_map_.find(cur_ts.idx());
                if (pos != ts_idx_map_.end()) {
                    Put(key, cur_ts.ts(), row);
                }
                break;
            }
        }
        return;
    }
    KeyEntry** entry_arr = NULL;
    uint32_t byte_size = 0; 
    std::lock_guard<std::mutex> lock(mu_);
    for (const auto& cur_ts : ts_dimension) {
		auto pos = ts_idx_map_.find(cur_ts.idx());
        if (pos == ts_idx_map_.end()) {
            continue;
        }
		int ret = multi_entries_->Get(key, entry_arr);
		if (ret < 0 || entry_arr == NULL) {
			char* pk = new char[key.size()];
			memcpy(pk, key.data(), key.size());
			Slice skey(pk, key.size());
			entry_arr = new KeyEntry*[ts_cnt_];
			for (uint32_t i = 0; i < ts_cnt_; i++) {
				entry_arr[i] = new KeyEntry(key_entry_max_height_);
			}
			uint8_t height = multi_entries_->Insert(skey, entry_arr);
			byte_size += GetRecordPkMultiIdxSize(height, key.size(), key_entry_max_height_, ts_cnt_);
			pk_cnt_.fetch_add(1, std::memory_order_relaxed);
		}
        uint8_t height = entry_arr[pos->second]->entries.Insert(cur_ts.ts(), row);
        entry_arr[pos->second]->count_.fetch_add(1, std::memory_order_relaxed);
        byte_size += GetRecordTsIdxSize(height);
        idx_byte_size_.fetch_add(byte_size, std::memory_order_relaxed);
    }
    idx_cnt_.fetch_add(1, std::memory_order_relaxed);
}

bool Segment::Get(const Slice& key,
                  const uint64_t time,
                  DataBlock** block) {
    if (block == NULL || ts_cnt_ > 1) {
        return false;
    }

    KeyEntry* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return false;
    }

    *block = entry->entries.Get(time);
    return true;
}

bool Segment::Get(const Slice& key, uint32_t idx, const uint64_t time, DataBlock** block) {
    if (block == NULL) {
        return false;
    }
    if (ts_cnt_ == 1) {
        return Get(key, time, block);
    }
	auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return false;
    }
    KeyEntry** entry = NULL;
    if (multi_entries_->Get(key, entry) < 0 || entry == NULL) {
        return false;
    }
    *block = entry[pos->second]->entries.Get(time);
    return true;
}

bool Segment::Delete(const Slice& key) {
    if (entries_) {
        ::rtidb::base::Node<Slice, KeyEntry*>* entry_node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            entry_node = entries_->Remove(key);
            if (entry_node == NULL) {
                return false;
            }
        }
        {
            std::lock_guard<std::mutex> lock(gc_mu_);
            entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
    } else if (multi_entries_) {
        ::rtidb::base::Node<Slice, KeyEntry**>* entry_node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            entry_node = multi_entries_->Remove(key);
            if (entry_node == NULL) {
                return false;
            }
        }
        {
            std::lock_guard<std::mutex> lock(gc_mu_);
            multi_entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
    }
    return true;
}

void Segment::FreeList(::rtidb::base::Node<uint64_t, DataBlock*>* node,
                       uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt,
                       uint64_t& gc_record_byte_size) {
    while (node != NULL) {
        gc_idx_cnt++;
        ::rtidb::base::Node<uint64_t, DataBlock*>* tmp = node;
        idx_byte_size_.fetch_sub(GetRecordTsIdxSize(tmp->Height()));
        node = node->GetNextNoBarrier(0);
        PDLOG(DEBUG, "delete key %lld with height %u", tmp->GetKey(), tmp->Height());
        if (tmp->GetValue()->dim_cnt_down > 1) {
            tmp->GetValue()->dim_cnt_down --;
        }else {
            PDLOG(DEBUG, "delele data block for key %lld", tmp->GetKey());
            gc_record_byte_size += GetRecordSize(tmp->GetValue()->size);
            delete tmp->GetValue();
            gc_record_cnt++;
        }
        delete tmp;
    }
}

void Segment::GcFreeList(uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    uint64_t cur_version = gc_version_.load(std::memory_order_relaxed);
    if (cur_version < FLAGS_gc_deleted_pk_version_delta) {
        return;
    }
    uint64_t old = gc_idx_cnt;
    uint64_t free_list_version = cur_version - FLAGS_gc_deleted_pk_version_delta;
    if (entry_free_list_) {
        ::rtidb::base::Node<uint64_t, ::rtidb::base::Node<Slice, KeyEntry*>*>* node = NULL;
        {
            std::lock_guard<std::mutex> lock(gc_mu_);
            node = entry_free_list_->Split(free_list_version);
        }
        while (node != NULL) {
            ::rtidb::base::Node<Slice, KeyEntry*>* entry_node = node->GetValue();
            uint64_t byte_size = GetRecordPkIdxSize(entry_node->Height(), entry_node->GetKey().size(), key_entry_max_height_);
            idx_byte_size_.fetch_sub(byte_size, std::memory_order_relaxed);
            pk_cnt_.fetch_sub(1, std::memory_order_relaxed);
            // free pk memory
            delete entry_node->GetKey().data();
            KeyEntry* entry = entry_node->GetValue();
            TimeEntries::Iterator* it = entry->entries.NewIterator();
            it->SeekToFirst();
            if (it->Valid()) {
                uint64_t ts = it->GetKey();
                ::rtidb::base::Node<uint64_t, DataBlock*>* data_node = entry->entries.Split(ts);
                FreeList(data_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            }
            delete it;
            delete entry;
            delete entry_node;
            ::rtidb::base::Node<uint64_t, ::rtidb::base::Node<Slice, KeyEntry*>*>* tmp = node;
            node = node->GetNextNoBarrier(0);
            delete tmp;
        }
        idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    } else if (multi_entry_free_list_) {
        ::rtidb::base::Node<uint64_t, ::rtidb::base::Node<Slice, KeyEntry**>*>* node = NULL;
        {
            std::lock_guard<std::mutex> lock(gc_mu_);
            node = multi_entry_free_list_->Split(free_list_version);
        }
        while (node != NULL) {
            ::rtidb::base::Node<Slice, KeyEntry**>* entry_node = node->GetValue();
            uint64_t byte_size = GetRecordPkIdxSize(entry_node->Height(), entry_node->GetKey().size(), key_entry_max_height_);
            idx_byte_size_.fetch_sub(byte_size, std::memory_order_relaxed);
            pk_cnt_.fetch_sub(1, std::memory_order_relaxed);
            // free pk memory
            delete entry_node->GetKey().data();
            KeyEntry** entry_arr = entry_node->GetValue();
            for (uint32_t i = 0; i < ts_cnt_; i++) {
                TimeEntries::Iterator* it = entry_arr[i]->entries.NewIterator();
                it->SeekToFirst();
                if (it->Valid()) {
                    uint64_t ts = it->GetKey();
                    ::rtidb::base::Node<uint64_t, DataBlock*>* data_node = entry_arr[i]->entries.Split(ts);
                    FreeList(data_node, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
                }
                ::rtidb::base::Node<uint64_t, ::rtidb::base::Node<Slice, KeyEntry**>*>* tmp = node;
                node = node->GetNextNoBarrier(0);
                delete it;
                delete tmp;
            }
            delete[] entry_arr;
            delete entry_node;
        }
        idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    }
}

void Segment::Gc4Head(uint64_t keep_cnt, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    if (keep_cnt == 0) {
        PDLOG(WARNING, "[Gc4Head] segment gc4head is disabled");
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt;
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                node = entry->entries.SplitByPos(keep_cnt);
            }
        }
        uint64_t entry_gc_idx_cnt = 0;
        FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
        gc_idx_cnt += entry_gc_idx_cnt;
        it->Next();
    }
    PDLOG(DEBUG, "[Gc4Head] segment gc keep cnt %lu consumed %lld, count %lld", keep_cnt,
            (::baidu::common::timer::get_micros() - consumed)/1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::Gc4Head(const std::map<uint32_t, uint64_t>& keep_cnt_map, uint64_t& gc_idx_cnt, 
        uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    if (multi_entries_ == NULL) {
        if (keep_cnt_map.size() == 1) {
            return Gc4Head(keep_cnt_map.begin()->second, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        } 
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt; 
    KeyMultiEntries::Iterator* it = multi_entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry** entry_arr = it->GetValue();
        Slice key = it->GetKey();
        it->Next();
        for (const auto& kv : keep_cnt_map) {
            auto pos = ts_idx_map_.find(kv.first);
            if (pos == ts_idx_map_.end() || pos->second >= ts_cnt_ || kv.second == 0) {
                continue;
            }
            KeyEntry* entry = entry_arr[pos->second];
            ::rtidb::base::Node<uint64_t, DataBlock*>* node = NULL;
            {
                std::lock_guard<std::mutex> lock(mu_);
                if (entry->refs_.load(std::memory_order_acquire) <= 0) {
                    node = entry->entries.SplitByPos(kv.second);
                }
            }
            uint64_t entry_gc_idx_cnt = 0;
            FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
            gc_idx_cnt += entry_gc_idx_cnt;
        }
    }
    PDLOG(DEBUG, "[Gc4Head] segment gc consumed %lld, count %lld",
            (::baidu::common::timer::get_micros() - consumed) / 1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::SplitList(KeyEntry* entry, uint64_t ts, ::rtidb::base::Node<uint64_t, DataBlock*>** node) {
    // skip entry that ocupied by reader
    if (entry->refs_.load(std::memory_order_acquire) <= 0) {
        *node = entry->entries.Split(ts);
    }
}

// fast gc with no global pause
void Segment::Gc4TTL(const uint64_t time, uint64_t& gc_idx_cnt, uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt; 
    KeyEntries::Iterator* it = entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry* entry = it->GetValue();
        Slice key = it->GetKey();
        it->Next();
        ::rtidb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
        if (node == NULL || node->GetKey() > time) {
            PDLOG(DEBUG, "[Gc4TTL] segment gc with key %lu need not ttl, last node key %lu", time, node->GetKey());
            continue;
        }
        node = NULL;
        ::rtidb::base::Node<Slice, KeyEntry*>* entry_node = NULL;
        {
            std::lock_guard<std::mutex> lock(mu_);
            SplitList(entry, time, &node);
            if (entry->entries.IsEmpty()) {
                entry_node = entries_->Remove(key);
            }
        }
        if (entry_node != NULL) {
            std::lock_guard<std::mutex> lock(gc_mu_);
            entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
        }
        uint64_t entry_gc_idx_cnt = 0;
        FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
        gc_idx_cnt += entry_gc_idx_cnt;
    }
    PDLOG(DEBUG, "[Gc4TTL] segment gc with key %lld ,consumed %lld, count %lld", time,
            (::baidu::common::timer::get_micros() - consumed)/1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

void Segment::Gc4TTL(const std::map<uint32_t, uint64_t>& time_map, uint64_t& gc_idx_cnt, 
            uint64_t& gc_record_cnt, uint64_t& gc_record_byte_size) {
    if (multi_entries_ == NULL) {
        if (time_map.size() == 1) {
            return Gc4TTL(time_map.begin()->second, gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
        } 
        return;
    }
    uint64_t consumed = ::baidu::common::timer::get_micros();
    uint64_t old = gc_idx_cnt; 
    KeyMultiEntries::Iterator* it = multi_entries_->NewIterator();
    it->SeekToFirst();
    while (it->Valid()) {
        KeyEntry** entry_arr = it->GetValue();
        Slice key = it->GetKey();
        it->Next();
        uint32_t empty_cnt = 0;
        for (const auto& kv : time_map) {
            auto pos = ts_idx_map_.find(kv.first);
            if (pos == ts_idx_map_.end() || pos->second >= ts_cnt_) {
                continue;
            }
            KeyEntry* entry = entry_arr[pos->second];
            ::rtidb::base::Node<uint64_t, DataBlock*>* node = entry->entries.GetLast();
            if (node == NULL || node->GetKey() > kv.second) {
                PDLOG(DEBUG, "[Gc4TTL] segment gc with key %lu ts_idx %lu need not ttl, node key %lu", 
                             kv.second, pos->second, node->GetKey());
                continue;
            }
            node = NULL;
            {
                std::lock_guard<std::mutex> lock(mu_);
                SplitList(entry, kv.second, &node);
                if (entry->entries.IsEmpty()) {
                    empty_cnt++;
                }
            }
            uint64_t entry_gc_idx_cnt = 0;
            FreeList(node, entry_gc_idx_cnt, gc_record_cnt, gc_record_byte_size);
            entry->count_.fetch_sub(entry_gc_idx_cnt, std::memory_order_relaxed);
            gc_idx_cnt += entry_gc_idx_cnt;
        }
        if (empty_cnt == ts_cnt_) {
            bool is_empty = true;
            ::rtidb::base::Node<Slice, KeyEntry**>* entry_node = NULL;
            {
                std::lock_guard<std::mutex> lock(mu_);
                for (uint32_t i = 0; i < ts_cnt_; i++) {
                    if (!entry_arr[i]->entries.IsEmpty()) {
                        is_empty = false;
                        break;
                    }
                }
                if (is_empty) {
                    entry_node = multi_entries_->Remove(key);
                }
            }
            if (entry_node != NULL) {
                std::lock_guard<std::mutex> lock(gc_mu_);
                multi_entry_free_list_->Insert(gc_version_.load(std::memory_order_relaxed), entry_node);
            }
        }
    }
    PDLOG(DEBUG, "[Gc4TTL] segment gc with key %lld, consumed %lld, count %lld", time,
            (::baidu::common::timer::get_micros() - consumed) / 1000, gc_idx_cnt - old);
    idx_cnt_.fetch_sub(gc_idx_cnt - old, std::memory_order_relaxed);
    delete it;
}

int Segment::GetCount(const Slice& key, uint64_t& count) {
    if (ts_cnt_ > 1) {
        return -1;
    }
    KeyEntry* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return -1;
    }
    count = entry->count_.load(std::memory_order_relaxed);
    return 0;
}

int Segment::GetCount(const Slice& key, uint32_t idx, uint64_t& count) {
    if (ts_cnt_ == 1) {
        return GetCount(key, count);
    }
	auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return -1;
    }
    KeyEntry** entry_arr = NULL;
    if (multi_entries_->Get(key, entry_arr) < 0 || entry_arr == NULL) {
        return -1;
    }
    count = entry_arr[pos->second]->count_.load(std::memory_order_relaxed);
    return 0;
}

// Iterator
Iterator* Segment::NewIterator(const Slice& key, Ticket& ticket) {
    if (entries_ == NULL) {
        return new Iterator(NULL);
    }
    KeyEntry* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return new Iterator(NULL);
    }
    ticket.Push(entry);
    return new Iterator(entry->entries.NewIterator());
}

Iterator* Segment::NewIterator(const Slice& key, uint32_t idx, Ticket& ticket) {
    if (ts_cnt_ == 1) {
        return NewIterator(key, ticket);
    }
	auto pos = ts_idx_map_.find(idx);
    if (pos == ts_idx_map_.end()) {
        return new Iterator(NULL);
    }
    KeyEntry** entry_arr = NULL;
    if (multi_entries_->Get(key, entry_arr) < 0 || entry_arr == NULL) {
        return new Iterator(NULL);
    }
    ticket.Push(entry_arr[idx]);
    return new Iterator(entry_arr[idx]->entries.NewIterator());
}

Iterator::Iterator(TimeEntries::Iterator* it): it_(it) {}

Iterator::~Iterator() {
    if (it_ != NULL) {
        delete it_;
    }
}

void Iterator::Seek(const uint64_t& time) {
    if (it_ == NULL) {
        return;
    }
    it_->Seek(time);
}

bool Iterator::Valid() const {
    if (it_ == NULL) {
        return false;
    }
    return it_->Valid();
}

void Iterator::Next() {
    if (it_ == NULL) {
        return;
    }
    it_->Next();
}

DataBlock* Iterator::GetValue() const {
    return it_->GetValue();
}

uint64_t Iterator::GetKey() const {
    return it_->GetKey();
}

void Iterator::SeekToFirst() {
    if (it_ == NULL) {
        return;
    }
    it_->SeekToFirst();
}

void Iterator::SeekToLast() {
    if (it_ == NULL) {
        return;
    }
    it_->SeekToLast();
}

uint32_t Iterator::GetSize() {
    if (it_ == NULL) {
        return 0; 
    }
    return it_->GetSize();
}

}
}



