//
// hash.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize
// Date 2017-03-31
//
#pragma once
namespace fesql {
namespace storage {

static inline uint32_t hash(const void* key, uint32_t len, uint32_t seed) {
    const uint32_t m = 0x5bd1e995;
    const uint32_t r = 24;
    uint32_t h = seed ^ len;
    const unsigned char* data = (const unsigned char*)key;
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

    switch (len) {
        case 3:
            h ^= data[2] << 16;
        case 2:
            h ^= data[1] << 8;
        case 1:
            h ^= data[0];
            h *= m;
    }
    h ^= h >> 13;
    h *= m;
    h ^= h >> 15;
    return h;
}

static uint64_t MurmurHash64A(const void* key, int len, unsigned int seed) {
    const uint64_t m = 0xc6a4a7935bd1e995;
    const int r = 47;
    uint64_t h = seed ^ (len * m);
    const uint8_t* data = (const uint8_t*)key;
    const uint8_t* end = data + (len - (len & 7));

    while (data != end) {
        uint64_t k;

        k = (uint64_t)data[0];
        k |= (uint64_t)data[1] << 8;
        k |= (uint64_t)data[2] << 16;
        k |= (uint64_t)data[3] << 24;
        k |= (uint64_t)data[4] << 32;
        k |= (uint64_t)data[5] << 40;
        k |= (uint64_t)data[6] << 48;
        k |= (uint64_t)data[7] << 56;

        k *= m;
        k ^= k >> r;
        k *= m;
        h ^= k;
        h *= m;
        data += 8;
    }

    switch (len & 7) {
        case 7:
            h ^= (uint64_t)data[6] << 48;
        case 6:
            h ^= (uint64_t)data[5] << 40;
        case 5:
            h ^= (uint64_t)data[4] << 32;
        case 4:
            h ^= (uint64_t)data[3] << 24;
        case 3:
            h ^= (uint64_t)data[2] << 16;
        case 2:
            h ^= (uint64_t)data[1] << 8;
        case 1:
            h ^= (uint64_t)data[0];
            h *= m;
    };

    h ^= h >> r;
    h *= m;
    h ^= h >> r;
    return h;
}

static inline int64_t hash64(const std::string& key) {
    uint64_t raw_value = MurmurHash64A(key.c_str(), key.length(), 0xe17a1465);
    int64_t cur_value = (int64_t)raw_value;
    // convert to signed integer as same as java client
    if (cur_value < 0) {
        cur_value *= -1;
    }
    return cur_value;
}

}  // namespace storage
}  // namespace fesql
