//
// file_util.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-06-07
//


#ifndef RTIDB_FILE_UTIL_H
#define RTIDB_FILE_UTIL_H

#include <sys/types.h>
#include <sys/stat.h>
#include <unistd.h>
#include <fcntl.h>
#include <dirent.h>
#include <errno.h>
#include "logging.h"

using ::baidu::common::INFO;
using ::baidu::common::WARNING;

namespace rtidb {
namespace base {

inline static bool Mkdir(const std::string& path) {

    const int dir_mode = 0777;
    int ret = ::mkdir(path.c_str(), dir_mode); 
    if (ret == 0 || errno == EEXIST) {
        return true; 
    }
    LOG(WARNING, "mkdir %s failed err[%d: %s]", 
            path.c_str(), errno, strerror(errno));
    return false;
}

inline static bool MkdirRecur(const std::string& dir_path) {

    size_t beg = 0;
    size_t seg = dir_path.find('/', beg);
    while (seg != std::string::npos) {
        if (seg + 1 >= dir_path.size()) {
            break; 
        }
        if (!Mkdir(dir_path.substr(0, seg + 1))) {
            return false; 
        }
        beg = seg + 1;
        seg = dir_path.find('/', beg);
    }
    return Mkdir(dir_path);
}

}
}

#endif /* !FILE_UTIL_H */
