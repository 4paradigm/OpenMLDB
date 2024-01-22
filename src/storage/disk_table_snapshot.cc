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
#include "base/hash.h"
#include "base/strings.h"
#include "schema/index_util.h"

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
    uint32_t pid = table->GetPid();
    schema::TableIndexInfo table_index_info(*(table->GetTableMeta()), add_indexs);
    if (!table_index_info.Init()) {
        return {-1, "parse TableIndexInfo failed"};
    }
    std::unique_ptr<TraverseIterator> it(table->NewTraverseIterator(0));
    if (it == nullptr) {
        PDLOG(WARNING, "fail to get iterator");
        return {-1, "fail to get iterator"};
    }
    it->SeekToFirst();
    while (it->Valid()) {
        auto data = it->GetValue();
        uint64_t ts = it->GetKey();
        std::vector<std::string> index_row;
        auto staus = DecodeData(table, data, table_index_info.GetAllIndexCols(), &index_row);
        std::map<uint32_t, std::vector<::openmldb::api::Dimension>> dimension_map;
        for (auto idx : table_index_info.GetAddIndexIdx()) {
            std::string index_key;
            for (auto pos : table_index_info.GetRealIndexCols(idx)) {
                if (index_key.empty()) {
                    index_key = index_row.at(pos);
                } else {
                    absl::StrAppend(&index_key, "|", index_row.at(pos));
                }
            }
            ::openmldb::api::Dimension dim;
            dim.set_idx(idx);
            dim.set_key(index_key);
            uint32_t index_pid = ::openmldb::base::hash64(index_key) % whs.size();
            auto iter = dimension_map.emplace(index_pid, std::vector<::openmldb::api::Dimension>()).first;
            iter->second.push_back(dim);
        }
        api::LogEntry entry;
        entry.set_value(data.ToString());
        entry.set_ts(ts);
        std::string tmp_buf;
        auto iter = dimension_map.find(pid);
        if (iter != dimension_map.end()) {
            entry.clear_dimensions();
            for (const auto& dim : iter->second) {
                entry.add_dimensions()->CopyFrom(dim);
            }
            DLOG(INFO) << "extract: dim size " << entry.dimensions_size() << " key " << entry.dimensions(0).key()
                << " pid " << pid;
            table->Put(entry);
        }
        if (dump_data) {
            for (const auto& kv : dimension_map) {
                if (kv.first == pid) {
                    continue;
                }
                entry.clear_dimensions();
                for (const auto& dim : kv.second) {
                    entry.add_dimensions()->CopyFrom(dim);
                }
                tmp_buf.clear();
                entry.SerializeToString(&tmp_buf);
                if (!whs[kv.first]->Write(::openmldb::base::Slice(tmp_buf)).ok()) {
                    return  {-1, "fail to dump index entry"};
                }
                DLOG(INFO) << "dump " << pid << " dim size " << entry.dimensions_size()
                    << " key " << entry.dimensions(0).key() << " des pid " << kv.first; ;
            }
        }
        it->Next();
    }
    return {};
}

}  // namespace storage
}  // namespace openmldb
