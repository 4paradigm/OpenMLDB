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

#include <string>
#include <map>
#include <memory>
#include <utility>
#include "base/spinlock.h"
#include "sdk/base.h"
#include "vm/engine_context.h"

namespace openmldb {
namespace tablet {

struct SQLProcedureCacheEntry {
    std::shared_ptr<hybridse::sdk::ProcedureInfo> procedure_info;
    std::shared_ptr<hybridse::vm::CompileInfo> request_info;
    std::shared_ptr<hybridse::vm::CompileInfo> batch_request_info;

    SQLProcedureCacheEntry(const std::shared_ptr<hybridse::sdk::ProcedureInfo> pinfo,
                           std::shared_ptr<hybridse::vm::CompileInfo> rinfo,
                           std::shared_ptr<hybridse::vm::CompileInfo> brinfo)
        : procedure_info(pinfo), request_info(rinfo), batch_request_info(brinfo) {}
};

class SpCache : public hybridse::vm::CompileInfoCache {
 public:
    SpCache() : db_sp_map_() {}
    ~SpCache() {}
    void InsertSQLProcedureCacheEntry(const std::string& db, const std::string& sp_name,
                                      std::shared_ptr<hybridse::sdk::ProcedureInfo> procedure_info,
                                      std::shared_ptr<hybridse::vm::CompileInfo> request_info,
                                      std::shared_ptr<hybridse::vm::CompileInfo> batch_request_info) {
        std::lock_guard<::openmldb::base::SpinMutex> spin_lock(spin_mutex_);
        auto& sp_map_of_db = db_sp_map_[db];
        sp_map_of_db.insert(
            std::make_pair(sp_name, SQLProcedureCacheEntry(procedure_info, request_info, batch_request_info)));
    }

    void DropSQLProcedureCacheEntry(const std::string& db, const std::string& sp_name) {
        std::lock_guard<::openmldb::base::SpinMutex> spin_lock(spin_mutex_);
        db_sp_map_[db].erase(sp_name);
        return;
    }
    const bool ProcedureExist(const std::string& db, const std::string& sp_name) {
        std::lock_guard<::openmldb::base::SpinMutex> spin_lock(spin_mutex_);
        auto& sp_map_of_db = db_sp_map_[db];
        auto sp_it = sp_map_of_db.find(sp_name);
        return sp_it != sp_map_of_db.end();
    }
    std::shared_ptr<hybridse::vm::CompileInfo> GetRequestInfo(const std::string& db, const std::string& sp_name,
                                                              hybridse::base::Status& status) override {  // NOLINT
        std::lock_guard<::openmldb::base::SpinMutex> spin_lock(spin_mutex_);
        auto db_it = db_sp_map_.find(db);
        if (db_it == db_sp_map_.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        auto sp_it = db_it->second.find(sp_name);
        if (sp_it == db_it->second.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }

        if (!sp_it->second.request_info) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        return sp_it->second.request_info;
    }
    std::shared_ptr<hybridse::vm::CompileInfo> GetBatchRequestInfo(const std::string& db, const std::string& sp_name,
                                                                   hybridse::base::Status& status) override {  // NOLINT
        std::lock_guard<::openmldb::base::SpinMutex> spin_lock(spin_mutex_);
        auto db_it = db_sp_map_.find(db);
        if (db_it == db_sp_map_.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        auto sp_it = db_it->second.find(sp_name);
        if (sp_it == db_it->second.end()) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        if (!sp_it->second.batch_request_info) {
            status = hybridse::base::Status(hybridse::common::kProcedureNotFound,
                                            "store procedure[" + sp_name + "] not found in db[" + db + "]");
            return std::shared_ptr<hybridse::vm::CompileInfo>();
        }
        return sp_it->second.batch_request_info;
    }

 private:
    std::map<std::string, std::map<std::string, SQLProcedureCacheEntry>> db_sp_map_;
    ::openmldb::base::SpinMutex spin_mutex_;
};

}  // namespace tablet
}  // namespace openmldb
