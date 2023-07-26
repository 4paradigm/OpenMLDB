/*
 * Copyright 2021 4Paradigm
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

#ifndef HYBRIDSE_SRC_UDF_DYNAMIC_LIB_MANAGER_H_
#define HYBRIDSE_SRC_UDF_DYNAMIC_LIB_MANAGER_H_

#include <map>
#include <memory>
#include <mutex>
#include <string>
#include <vector>
#include "base/fe_status.h"

namespace hybridse {
namespace udf {

/*struct DynamicLibHandle {
    explicit DynamicLibHandle(void* ptr) {
        handle = ptr;
        ref_cnt = 1;
    }
    void* handle = nullptr;
    uint32_t ref_cnt = 0;
};*/

class DynamicLibManager {
 public:
    DynamicLibManager() = default;
    ~DynamicLibManager();
    DynamicLibManager(const DynamicLibManager&) = delete;
    DynamicLibManager& operator=(const DynamicLibManager&) = delete;

    base::Status ExtractFunction(const std::string& name, bool is_aggregate,
            const std::string& file, std::vector<void*>* funs);

    base::Status RemoveHandler(const std::string& file);

 private:
    std::mutex mu_;
    // std::map<std::string, std::shared_ptr<DynamicLibHandle>> handle_map_;
    std::map<std::string, void*> handle_map_;
};

}  // namespace udf
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_UDF_DYNAMIC_LIB_MANAGER_H_
