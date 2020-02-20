/*
 * fs_util.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "base/fs_util.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include "glog/logging.h"

namespace fesql {
namespace base {

bool ListDir(const std::string& path, std::vector<std::string>& files) {
    DIR* dir = ::opendir(path.c_str());
    if (dir == NULL) {
        LOG(WARNING) << "opendir " << path << " failed err " << strerror(errno);
        return false;
    }
    struct dirent* entry;
    while ((entry = ::readdir(dir)) != NULL) {
        if (strcmp(entry->d_name, ".") == 0 || strcmp(entry->d_name, "..") == 0)
            continue;
        files.push_back(entry->d_name);
    }
    ::closedir(dir);
    return true;
}

}  // namespace base
}  // namespace fesql
