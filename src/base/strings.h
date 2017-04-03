//
// strings.h
// Copyright (C) 2017 4paradigm.com
// Author wangtaize 
// Date 2017-04-02 
// 


#ifndef RTIDB_BASE_STRINGS_H
#define RTIDB_BASE_STRINGS_H

#include <string>
#include <vector>

namespace rtidb {
namespace base {

static inline void SplitString(const std::string& full,
                               const std::string& delim,
                               std::vector<std::string>* result) {
    result->clear();
    if (full.empty()) {
        return;
    }

    std::string tmp;
    std::string::size_type pos_begin = full.find_first_not_of(delim);
    std::string::size_type comma_pos = 0;

    while (pos_begin != std::string::npos) {
        comma_pos = full.find(delim, pos_begin);
        if (comma_pos != std::string::npos) {
            tmp = full.substr(pos_begin, comma_pos - pos_begin);
            pos_begin = comma_pos + delim.length();
        } else {
            tmp = full.substr(pos_begin);
            pos_begin = comma_pos;
        }

        if (!tmp.empty()) {
            result->push_back(tmp);
            tmp.clear();
        }
    }
}

}
}
#endif /* !STRINGS_H */
