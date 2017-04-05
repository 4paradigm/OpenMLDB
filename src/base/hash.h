//
// hash.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-03-31 
// 


#ifndef BASE_HASH_H
#define BASE_HASH_H

namespace rtidb {
namespace base {


static inline uint32_t hash(const void* key, uint32_t len, uint32_t seed) {

    const uint32_t m = 0x5bd1e995;
    const uint32_t r = 24;
    uint32_t h = seed ^ len;
    const unsigned char* data = (const unsigned char*) key;
    while (len >= 4) {
        uint32_t k = *(uint32_t*)data;
        k *= m;
        k ^= k >> r;
        k *= m;
        h *= m;
        h ^= k;
        data += 4;
        len -= 4;
    }

    switch(len) {
        case 3: h ^= data[2] << 16;
        case 2: h ^= data[1] << 8;
        case 1: h ^= data[0];
                h *=m;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

}
}

#endif /* !HASH_H */
