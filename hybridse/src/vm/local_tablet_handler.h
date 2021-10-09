/*
 * local_tablet.h
 * Copyright (C) 4paradigm 2021 chenjing <chenjing@4paradigm.com>
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
#ifndef HYBRIDSE_SRC_VM_LOCAL_TABLET_HANDLER_H_
#define HYBRIDSE_SRC_VM_LOCAL_TABLET_HANDLER_H_
#include <memory>
#include <string>
#include <vector>
#include "vm/engine.h"
#include "vm/mem_catalog.h"
namespace hybridse {
namespace vm {

class LocalTabletRowHandler : public RowHandler {
 public:
    LocalTabletRowHandler(uint32_t task_id, const RequestRunSession& session,
                          const Row& request)
        : RowHandler(),
          status_(base::Status::Running()),
          table_name_(""),
          db_(""),
          schema_(nullptr),
          task_id_(task_id),
          session_(session),
          request_(request),
          value_() {}
    virtual ~LocalTabletRowHandler() {}
    const Row& GetValue() override {
        if (!status_.isRunning()) {
            return value_;
        }
        status_ = SyncValue();
        return value_;
    }
    base::Status SyncValue() {
        DLOG(INFO) << "Sync Value ... local tablet SubQuery request: task id "
                   << task_id_;
        if (0 != session_.Run(task_id_, request_, &value_)) {
            return base::Status(common::kCallRpcMethodError,
                                "sub query fail: session run fail");
        }
        return base::Status::OK();
    }
    const Schema* GetSchema() override { return schema_; }
    const std::string& GetName() override { return table_name_; }
    const std::string& GetDatabase() override { return db_; }
    base::Status status_;
    std::string table_name_;
    std::string db_;
    const Schema* schema_;
    uint32_t task_id_;
    RequestRunSession session_;
    Row request_;
    Row value_;
};
class LocalTabletTableHandler : public MemTableHandler {
 public:
    LocalTabletTableHandler(uint32_t task_id,
                            const BatchRequestRunSession session,
                            const std::vector<Row> requests,
                            const bool request_is_common)
        : status_(base::Status::Running()),
          task_id_(task_id),
          session_(session),
          requests_(requests),
          request_is_common_(request_is_common) {}
    ~LocalTabletTableHandler() {}
    Row At(uint64_t pos) override {
        if (!status_.isRunning()) {
            return MemTableHandler::At(pos);
        }
        status_ = SyncValue();
        return MemTableHandler::At(pos);
    }
    std::unique_ptr<RowIterator> GetIterator() {
        if (status_.isRunning()) {
            status_ = SyncValue();
        }
        return MemTableHandler::GetIterator();
    }
    RowIterator* GetRawIterator() {
        if (status_.isRunning()) {
            status_ = SyncValue();
        }
        return MemTableHandler::GetRawIterator();
    }
    virtual const uint64_t GetCount() {
        if (status_.isRunning()) {
            status_ = SyncValue();
        }
        return MemTableHandler::GetCount();
    }

 private:
    base::Status SyncValue() {
        DLOG(INFO) << "Local tablet SubQuery batch request: task id "
                   << task_id_;
        if (0 != session_.Run(task_id_, requests_, table_)) {
            return base::Status(common::kCallRpcMethodError,
                                "sub query fail: session run fail");
        }
        return base::Status::OK();
    }
    base::Status status_;
    uint32_t task_id_;
    BatchRequestRunSession session_;
    const std::vector<Row> requests_;
    const bool request_is_common_;
};
}  // namespace vm
}  // namespace hybridse
#endif  // HYBRIDSE_SRC_VM_LOCAL_TABLET_HANDLER_H_
