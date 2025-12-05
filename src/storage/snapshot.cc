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

#include "storage/snapshot.h"

#include <fcntl.h>
#ifdef DISALLOW_COPY_AND_ASSIGN
#undef DISALLOW_COPY_AND_ASSIGN
#endif
#include <snappy.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include "absl/strings/str_cat.h"
#include "base/slice.h"
#include "base/glog_wrapper.h"
#include "codec/row_codec.h"
#include "google/protobuf/io/zero_copy_stream_impl.h"
#include "google/protobuf/text_format.h"

namespace openmldb {
namespace storage {

constexpr const char*  MANIFEST = "MANIFEST";

int Snapshot::GenManifest(const SnapshotMeta& snapshot_meta) {
    return GenManifest(snapshot_meta.snapshot_name, snapshot_meta.count,
            snapshot_meta.offset, snapshot_meta.term);
}

int Snapshot::GenManifest(const std::string& snapshot_name, uint64_t key_count, uint64_t offset, uint64_t term) {
    DEBUGLOG("record offset[%lu]. add snapshot[%s] key_count[%lu]", offset, snapshot_name.c_str(), key_count);
    std::string full_path = absl::StrCat(snapshot_path_, MANIFEST);
    std::string tmp_file = absl::StrCat(snapshot_path_, MANIFEST, ".tmp");
    ::openmldb::api::Manifest manifest;
    std::string manifest_info;
    manifest.set_offset(offset);
    manifest.set_name(snapshot_name);
    manifest.set_count(key_count);
    manifest.set_term(term);
    manifest_info.clear();
    google::protobuf::TextFormat::PrintToString(manifest, &manifest_info);
    FILE* fd_write = fopen(tmp_file.c_str(), "w");
    if (fd_write == nullptr) {
        PDLOG(WARNING, "fail to open file %s", tmp_file.c_str());
        return -1;
    }
    bool io_error = false;
    if (fputs(manifest_info.c_str(), fd_write) == EOF) {
        PDLOG(WARNING, "write error. path[%s]", tmp_file.c_str());
        io_error = true;
    }
    if (!io_error && ((fflush(fd_write) == EOF) || fsync(fileno(fd_write)) == -1)) {
        PDLOG(WARNING, "flush error. path[%s]", tmp_file.c_str());
        io_error = true;
    }
    fclose(fd_write);
    if (!io_error && rename(tmp_file.c_str(), full_path.c_str()) == 0) {
        DEBUGLOG("%s generate success. path[%s]", MANIFEST, full_path.c_str());
        return 0;
    }
    unlink(tmp_file.c_str());
    return -1;
}

int Snapshot::GetLocalManifest(const std::string& full_path, ::openmldb::api::Manifest& manifest) {
    int fd = open(full_path.c_str(), O_RDONLY);
    if (fd < 0) {
        PDLOG(INFO, "[%s] does not exist", MANIFEST);
        return 1;
    } else {
        google::protobuf::io::FileInputStream fileInput(fd);
        fileInput.SetCloseOnDelete(true);
        if (!google::protobuf::TextFormat::Parse(&fileInput, &manifest)) {
            PDLOG(WARNING, "parse manifest failed");
            return -1;
        }
    }
    return 0;
}

::openmldb::base::Status Snapshot::DecodeData(const std::shared_ptr<Table>& table,
        openmldb::base::Slice raw_data,
        const std::vector<uint32_t>& cols, std::vector<std::string>* row) {
    if (row == nullptr) {
        return {-1, "row is nullptr"};
    }
    row->clear();
    std::string buff;
    openmldb::base::Slice data;
    if (table->GetCompressType() == openmldb::type::kSnappy) {
        snappy::Uncompress(raw_data.data(), raw_data.size(), &buff);
        data.reset(buff.data(), buff.size());
    } else {
        data = raw_data;
    }
    const int8_t* raw = reinterpret_cast<const int8_t*>(data.data());
    uint8_t version = openmldb::codec::RowView::GetSchemaVersion(raw);
    auto decoder = table->GetVersionDecoder(version);
    if (decoder == nullptr) {
        return ::openmldb::base::Status(-1, "get decoder failed. version is " + std::to_string(version));
    }
    if (!openmldb::codec::RowCodec::DecodeRow(*decoder, raw, cols, true, row)) {
        return ::openmldb::base::Status(-1, "decode failed");
    }
    return {};
}

}  // namespace storage
}  // namespace openmldb
