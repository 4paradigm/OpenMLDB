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

#include <dlfcn.h>
#include <string>
#include <vector>
#include <map>
#include <memory>
#include <mutex>
#include "base/fe_status.h"

namespace hybridse {
namespace udf {

struct DynamicLibHandle {
    explicit DynamicLibHandle(void* ptr) {
        handle = ptr;
        ref_cnt = 1;
    }
    void* handle = nullptr;
    uint32_t ref_cnt = 0;
};

class DynamicLibManager {
 public:
    DynamicLibManager() = default;

    base::Status ExtractFunction(const std::string& name, bool is_aggregate,
            const std::string& file, std::vector<void*>* funs) {
        CHECK_TRUE(funs != nullptr, common::kExternalUDFError, "funs is nullptr")
        std::shared_ptr<DynamicLibHandle> so_handle;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto iter = handle_map_.find(file);
            if (iter != handle_map_.end()) {
                so_handle = iter->second;
                so_handle->ref_cnt++;
            }
        }
        if (!so_handle) {
            void* handle = dlopen(file.c_str(), RTLD_LAZY);
            CHECK_TRUE(handle != nullptr, common::kExternalUDFError, "can not open the dynamic library: " + file)
            so_handle = std::make_shared<DynamicLibHandle>(handle);
            std::lock_guard<std::mutex> lock(mu_);
            handle_map_.emplace(file, so_handle);
        }
        if (is_aggregate) {
            auto init_fun = dlsym(so_handle->handle, std::string(name + "_init").c_str());
            if (init_fun == nullptr) {
                RemoveHandler(file);
                return {common::kExternalUDFError, "can not find the init function: " + name};
            }
            funs->emplace_back(init_fun);
            auto update_fun = dlsym(so_handle->handle, std::string(name + "_update").c_str());
            if (update_fun == nullptr) {
                RemoveHandler(file);
                return {common::kExternalUDFError, "can not find the update function: " + name};
            }
            funs->emplace_back(update_fun);
            auto output_fun = dlsym(so_handle->handle, std::string(name + "_output").c_str());
            if (output_fun == nullptr) {
                RemoveHandler(file);
                return {common::kExternalUDFError, "can not find the output function: " + name};
            }
            funs->emplace_back(output_fun);
        } else {
            auto fun = dlsym(so_handle->handle, name.c_str());
            if (fun == nullptr) {
                RemoveHandler(file);
                return {common::kExternalUDFError, "can not find the function: " + name};
            }
            funs->emplace_back(fun);
        }
        return base::Status::OK();
    }

    base::Status RemoveHandler(const std::string& file) {
        std::shared_ptr<DynamicLibHandle> so_handle;
        {
            std::lock_guard<std::mutex> lock(mu_);
            auto iter = handle_map_.find(file);
            if (iter != handle_map_.end()) {
                iter->second->ref_cnt--;
                if (iter->second->ref_cnt == 0) {
                    so_handle = iter->second;
                    handle_map_.erase(iter);
                }
            }
        }
        if (so_handle) {
            if (dlclose(so_handle->handle) != 0) {
                return {common::kExternalUDFError, "dlclose run error. file is " + file};
            }
        }
        return {};
    }

 private:
    std::mutex mu_;
    std::map<std::string, std::shared_ptr<DynamicLibHandle>> handle_map_;
};

}  // namespace udf
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_UDF_DYNAMIC_LIB_MANAGER_H_
