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

#include "udf/dynamic_lib_manager.h"

#include <dlfcn.h>

namespace hybridse {
namespace udf {

DynamicLibManager::~DynamicLibManager() {
    for (const auto& kv : handle_map_) {
        auto so_handle = kv.second;
        if(so_handle != nullptr) {
            dlclose(so_handle);
        }
    }
    handle_map_.clear();
}

base::Status DynamicLibManager::ExtractFunction(const std::string& name, bool is_aggregate, const std::string& file,
                                                std::vector<void*>* funs) {
    CHECK_TRUE(funs != nullptr, common::kExternalUDFError, "funs is nullptr")
    void* handle = dlopen(file.c_str(), RTLD_LAZY);
    CHECK_TRUE(handle != nullptr, common::kExternalUDFError,
               "can not open the dynamic library: " + file + ", error: " + dlerror())
    {
        std::lock_guard<std::mutex> lock(mu_);
        handle_map_.[file] = so_handle;
    }
    if (is_aggregate) {
        std::string init_fun_name = name + "_init";
        auto init_fun = dlsym(so_handle, init_fun_name.c_str());
        if (init_fun == nullptr) {
            RemoveHandler(file);
            return {common::kExternalUDFError, "can not find the init function: " + init_fun_name};
        }
        funs->emplace_back(init_fun);
        std::string update_fun_name = name + "_update";
        auto update_fun = dlsym(so_handle, update_fun_name.c_str());
        if (update_fun == nullptr) {
            RemoveHandler(file);
            return {common::kExternalUDFError, "can not find the update function: " + update_fun_name};
        }
        funs->emplace_back(update_fun);
        std::string output_fun_name = name + "_output";
        auto output_fun = dlsym(so_handle, output_fun_name.c_str());
        if (output_fun == nullptr) {
            RemoveHandler(file);
            return {common::kExternalUDFError, "can not find the output function: " + output_fun_name};
        }
        funs->emplace_back(output_fun);
    } else {
        auto fun = dlsym(so_handle, name.c_str());
        if (fun == nullptr) {
            RemoveHandler(file);
            return {common::kExternalUDFError, "can not find the function: " + name};
        }
        funs->emplace_back(fun);
    }
    return base::Status::OK();
}

base::Status DynamicLibManager::RemoveHandler(const std::string& file) {
    void* so_handle;
    {
        std::lock_guard<std::mutex> lock(mu_);
        if (auto iter = handle_map_.find(file); iter != handle_map_.end()) {
            if (iter->second != nullptr) {
                CHECK_TRUE(handle != nullptr, common::kExternalUDFError,
                           "can not close the dynamic library: " + file + ", error: " + dlerror())
            }
        }
    }
    return {};
}

}  // namespace udf
}  // namespace hybridse
