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

#include <gflags/gflags.h>

#include <fstream>
#include <iostream>
#include <string>

#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/id_generator.h"

namespace openmldb {
namespace base {

bool WriteTxt(const std::string& full_path, std::string* name) {
    if (name == nullptr) {
        return false;
    }
    std::ofstream my_cout(full_path);
    if (my_cout.fail()) {
        PDLOG(WARNING, "init ofstream failed, path %s", full_path.data());
        return false;
    }
    IdGenerator ig;
    int64_t id = ig.Next();
    *name = std::to_string(id);
    my_cout << *name << std::endl;
    my_cout.close();
    return true;
}

bool ReadTxt(const std::string& full_path, std::string* name) {
    if (name == nullptr) {
        return false;
    }
    std::ifstream infile(full_path);
    if (infile.fail()) {
        PDLOG(WARNING, "init ifstream failed, path %s", full_path.data());
        return false;
    }
    getline(infile, *name);
    infile.close();
    return true;
}

bool GetNameFromTxt(const std::string& data_dir, std::string* name) {
    if (name == nullptr) {
        return false;
    }
    if (!IsExists(data_dir)) {
        if (!MkdirRecur(data_dir)) {
            PDLOG(WARNING, "make dir failed, path %s", data_dir.data());
            return false;
        }
    }
    std::string full_path = data_dir + "/name.txt";
    if (!IsExists(full_path)) {
        return WriteTxt(full_path, name);
    }
    return ReadTxt(full_path, name);
}

}  // namespace base
}  // namespace openmldb
