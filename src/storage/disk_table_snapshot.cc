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

#include "storage/disk_table_snapshot.h"
#include <limits.h>
#include <stdlib.h>
#include <gflags/gflags.h>
#include <memory>
#include <string>
#include "absl/strings/str_cat.h"
#include "absl/strings/match.h"
#include "base/file_util.h"
#include "base/glog_wrapper.h"
#include "base/strings.h"

namespace openmldb {
namespace storage {

const std::string MANIFEST = "MANIFEST";  // NOLINT

DiskTableSnapshot::DiskTableSnapshot(uint32_t tid, uint32_t pid, const std::string& db_root_path)
    : Snapshot(tid, pid), db_root_path_(db_root_path) {}

bool DiskTableSnapshot::Init() {
    snapshot_path_ = absl::StrCat(db_root_path_, "/", tid_, "_" , pid_, "/snapshot/");
    if (!::openmldb::base::MkdirRecur(snapshot_path_)) {
        PDLOG(WARNING, "fail to create db meta path %s", snapshot_path_.c_str());
        return false;
    }
    char abs_path_buff[PATH_MAX];
    if (realpath(snapshot_path_.c_str(), abs_path_buff) == NULL) {
        PDLOG(WARNING, "fail to get realpath. source path %s", snapshot_path_.c_str());
        return false;
    }
    snapshot_path_.assign(abs_path_buff);
    if (!absl::EndsWith(snapshot_path_, "/")) {
        snapshot_path_.append("/");
    }
    return true;
}

int DiskTableSnapshot::MakeSnapshot(std::shared_ptr<Table> table, uint64_t& out_offset, uint64_t end_offset,
                                    uint64_t term) {
    if (making_snapshot_.load(std::memory_order_acquire)) {
        PDLOG(INFO, "snapshot is doing now! tid %u pid %u", tid_, pid_);
        return 0;
    }
    if (end_offset > 0) {
        PDLOG(ERROR, "disk table snapshot doesn't support end_offset > 0");
        return -1;
    }

    making_snapshot_.store(true, std::memory_order_release);
    int ret = 0;
    do {
        std::string now_time = ::openmldb::base::GetNowTime();
        std::string snapshot_dir_name = now_time.substr(0, now_time.length() - 2);
        std::string snapshot_dir = snapshot_path_ + snapshot_dir_name;
        std::string snapshot_dir_tmp = snapshot_dir + ".tmp";
        if (::openmldb::base::IsExists(snapshot_dir_tmp)) {
            PDLOG(WARNING, "checkpoint dir[%s] is exist", snapshot_dir_tmp.c_str());
            if (!::openmldb::base::RemoveDir(snapshot_dir_tmp)) {
                PDLOG(WARNING, "delete checkpoint failed. checkpoint dir[%s]", snapshot_dir_tmp.c_str());
                break;
            }
        }
        DiskTable* disk_table = dynamic_cast<DiskTable*>(table.get());
        if (disk_table == nullptr) {
            break;
        }
        uint64_t record_count = disk_table->GetRecordCnt();
        uint64_t cur_offset = disk_table->GetOffset();
        if (disk_table->CreateCheckPoint(snapshot_dir_tmp) < 0) {
            PDLOG(WARNING, "create checkpoint failed. checkpoint dir[%s]", snapshot_dir_tmp.c_str());
            break;
        }
        std::string snapshot_dir_bak;
        if (::openmldb::base::IsExists(snapshot_dir)) {
            snapshot_dir_bak = snapshot_dir + ".bak";
            if (::openmldb::base::IsExists(snapshot_dir_bak)) {
                if (!::openmldb::base::RemoveDir(snapshot_dir_bak)) {
                    PDLOG(WARNING, "delete checkpoint bak failed. checkpoint bak dir[%s]", snapshot_dir_bak.c_str());
                    break;
                }
            }
            if (!::openmldb::base::Rename(snapshot_dir, snapshot_dir_bak)) {
                PDLOG(WARNING, "rename checkpoint failed. checkpoint bak dir[%s]", snapshot_dir_bak.c_str());
                break;
            }
        }
        if (!::openmldb::base::Rename(snapshot_dir_tmp, snapshot_dir)) {
            PDLOG(WARNING, "rename checkpoint failed. checkpoint dir[%s]", snapshot_dir.c_str());
            break;
        }
        ::openmldb::api::Manifest manifest;
        GetLocalManifest(snapshot_path_ + MANIFEST, manifest);
        if (GenManifest(snapshot_dir_name, record_count, cur_offset, term) == 0) {
            if (manifest.has_name() && manifest.name() != snapshot_dir_name) {
                DEBUGLOG("delete old checkpoint[%s]", manifest.name().c_str());
                if (!::openmldb::base::RemoveDir(snapshot_path_ + manifest.name())) {
                    PDLOG(WARNING, "delete checkpoint failed. checkpoint dir[%s]", snapshot_dir.c_str());
                }
            }
            if (!snapshot_dir_bak.empty() && ::openmldb::base::IsExists(snapshot_dir_bak)) {
                if (!::openmldb::base::RemoveDir(snapshot_dir_bak)) {
                    PDLOG(WARNING, "delete checkpoint backup failed. backup dir[%s]", snapshot_dir_bak.c_str());
                }
            }
            offset_ = cur_offset;
            out_offset = cur_offset;
            ret = 0;
        } else {
            PDLOG(WARNING, "GenManifest failed. delete checkpoint[%s]", snapshot_dir.c_str());
            ::openmldb::base::RemoveDir(snapshot_dir);
        }
    } while (false);
    making_snapshot_.store(false, std::memory_order_release);
    return ret;
}

bool DiskTableSnapshot::Recover(std::shared_ptr<Table> table, uint64_t& latest_offset) { return true; }

base::Status DiskTableSnapshot::ExtractIndexData(const std::shared_ptr<Table>& table,
            const std::vector<::openmldb::common::ColumnKey>& add_indexs,
            const std::vector<std::shared_ptr<::openmldb::log::WriteHandle>>& whs,
            uint64_t offset, bool dump_data) {
    auto disk_table = std::dynamic_pointer_cast<DiskTable>(table);
    return {};
}

}  // namespace storage
}  // namespace openmldb
