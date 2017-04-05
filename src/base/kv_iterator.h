#ifndef RTIDB_BASE_KV_LIST
#define RTIDB_BASE_KV_LIST

#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "base/slice.h"
#include "proto/tablet.pb.h"

namespace rtidb {
namespace base {

class KvIterator {

public:
    KvIterator(::rtidb::api::ScanResponse* response):response_(response),buffer_(NULL),
    tsize_(0),
    offset_(0), c_size_(0),
    tmp_(NULL){
        buffer_ = reinterpret_cast<char*>(&((*response->mutable_pairs())[0]));
        tsize_ = response->pairs().size();
    }

    ~KvIterator() {
        delete response_;
    }

    bool Valid() {
        if (offset_ >= tsize_ - 12) {
            return false;
        }
        return true;
    }

    void Next() {
        assert(Valid());
        uint32_t block_size = 0;
        memcpy(static_cast<void*>(&block_size), buffer_, 4);
        buffer_ += 4;
        memcpy(static_cast<void*>(&time_), buffer_, 8);
        buffer_ += 8;
        tmp_ = new Slice(buffer_, block_size - 8);
        buffer_ += (block_size - 8);
        offset_ += (4 + block_size);
    }

    uint64_t GetKey() const {
        return time_; 
    }

    Slice& GetValue() const {
        return *tmp_;
    }

private:
    ::rtidb::api::ScanResponse* response_;
    char* buffer_;
    uint32_t tsize_;
    uint32_t offset_;
    uint32_t c_size_;
    uint64_t time_;
    Slice* tmp_;
};

}
}
#endif
