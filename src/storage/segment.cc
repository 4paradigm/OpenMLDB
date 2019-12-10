//
// segment.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-11-01
//

#include "storage/segment.h"
#include <mutex>  //NOLINT
#include "storage/codec.h"

namespace fesql {
namespace storage {

Segment::Segment() : entries_(NULL), mu_() {
    entries_ = new KeyEntry(KEY_ENTRY_MAX_HEIGHT, 4, scmp);
}

Segment::~Segment() { delete entries_; }

void Segment::Put(const Slice& key, uint64_t time, DataBlock* row) {
    void* entry = NULL;
    std::lock_guard<base::SpinMutex> lock(mu_);
    int ret = entries_->Get(key, entry);
    if (ret < 0 || entry == NULL) {
        entry = reinterpret_cast<void*>(new TimeEntry(tcmp));
        char* pk = new char[key.size()];
        memcpy(pk, key.data(), key.size());
        Slice skey(pk, key.size());
        entries_->Insert(skey, entry);
    }
    reinterpret_cast<TimeEntry*>(entry)->Insert(time, row);
}

// Iterator
std::unique_ptr<TableIterator> Segment::NewIterator(const Slice& key, const uint64_t ts) {
    if (entries_ == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    void* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    return std::unique_ptr<TableIterator>(new TableIterator(
        NULL, (reinterpret_cast<TimeEntry*>(entry))->NewIterator(ts)));
}

std::unique_ptr<TableIterator> Segment::NewIterator(const Slice& key) {
    if (entries_ == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    void* entry = NULL;
    if (entries_->Get(key, entry) < 0 || entry == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    return std::unique_ptr<TableIterator>(new TableIterator(
        NULL, (reinterpret_cast<TimeEntry*>(entry))->NewIterator()));
}

std::unique_ptr<TableIterator> Segment::NewIterator() {
    if (entries_ == NULL) {
        return std::unique_ptr<TableIterator>(new TableIterator());
    }
    Iterator<Slice, void*>* it = entries_->NewIterator();
    return std::unique_ptr<TableIterator>(new TableIterator(it, NULL));
}

TableIterator::TableIterator(Iterator<Slice, void*>* pk_it,
                             Iterator<uint64_t, DataBlock*>* ts_it)
    : pk_it_(pk_it), ts_it_(ts_it) {}

TableIterator::~TableIterator() {
    delete ts_it_;
    delete pk_it_;
}

void TableIterator::Seek(uint64_t time) {
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->Seek(time);
}

void TableIterator::Seek(const std::string& key, uint64_t ts) {
    if (pk_it_ == NULL) {
        return;
    }
    Slice spk(key);
    pk_it_->Seek(spk);
    if (pk_it_->Valid()) {
        delete ts_it_;
        ts_it_ = NULL;
        while (pk_it_->Valid()) {
            ts_it_ = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                         ->NewIterator();
            ts_it_->SeekToFirst();
            if (ts_it_->Valid()) break;
            delete ts_it_;
            ts_it_ = NULL;
            pk_it_->Next();
        }
    }
}

bool TableIterator::Valid() {
    if (ts_it_ == NULL) {
        return false;
    }
    if (pk_it_ == NULL) {
        return ts_it_->Valid();
    }
    return pk_it_->Valid() && ts_it_->Valid();
}

void TableIterator::Next() {
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->Next();
    if (!ts_it_->Valid() && pk_it_ != NULL) {
        delete ts_it_;
        ts_it_ = NULL;
        pk_it_->Next();
        while (pk_it_->Valid()) {
            ts_it_ = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                         ->NewIterator();
            ts_it_->SeekToFirst();
            if (ts_it_->Valid()) break;
            delete ts_it_;
            ts_it_ = NULL;
            pk_it_->Next();
        }
    }
}

Slice TableIterator::GetValue() const {
    return Slice(
        ts_it_->GetValue()->data,
        RowView::GetSize(reinterpret_cast<int8_t*>(ts_it_->GetValue()->data)));
}

uint64_t TableIterator::GetKey() const { return ts_it_->GetKey(); }

std::string TableIterator::GetPK() const {
    if (pk_it_ == NULL) {
        return std::string();
    }
    return pk_it_->GetKey().ToString();
}

void TableIterator::SeekToFirst() {
    if (pk_it_ != NULL) {
        delete ts_it_;
        ts_it_ = NULL;
        pk_it_->SeekToFirst();
        if (pk_it_->Valid()) {
            while (pk_it_->Valid()) {
                ts_it_ = (reinterpret_cast<TimeEntry*>(pk_it_->GetValue()))
                             ->NewIterator();
                ts_it_->SeekToFirst();
                if (ts_it_->Valid()) return;
                delete ts_it_;
                ts_it_ = NULL;
                pk_it_->Next();
            }
        } else {
            ts_it_ = NULL;
        }
    }
    if (ts_it_ == NULL) {
        return;
    }
    ts_it_->SeekToFirst();
}

}  // namespace storage
}  // namespace fesql
