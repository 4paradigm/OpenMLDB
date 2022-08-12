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

#include "tools/log_exporter.h"

#include <dirent.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <algorithm>
#include <fstream>
#include <iostream>

#include "base/file_util.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/common.pb.h"
#include "proto/tablet.pb.h"
#include "storage/snapshot.h"
#include "tools/tablemeta_reader.h"

using ::openmldb::base::ParseFileNameFromPath;
using ::openmldb::base::Slice;
using ::openmldb::log::NewSeqFile;
using ::openmldb::log::Reader;
using ::openmldb::log::SequentialFile;
using ::openmldb::log::Status;

namespace openmldb {
namespace tools {

void Exporter::ReadManifest() {
    printf("--------start readManifest--------\n");
    std::string manifest_path = table_dir_path_ + "/snapshot/MANIFEST";
    ::openmldb::api::Manifest manifest;
    if (::openmldb::storage::Snapshot::GetLocalManifest(manifest_path, manifest)) {
        printf("Failed to read manifest\n");
        printf("---------end readManifest---------\n");
        return;
    }
    std::string snapshot_name = manifest.name();
    snapshot_path_ = table_dir_path_ + "/snapshot/" + snapshot_name;
    offset_ = manifest.offset();
    printf("--------snapshot offset: %lu\n", offset_);
    printf("--------snapshot path: %s\n", snapshot_path_.c_str());
    printf("---------end readManifest---------\n");
}

void Exporter::ExportTable() {
    std::vector<std::string> file_path;
    // Add all binlog files to file_path lists
    struct dirent *ptr;
    DIR *dir;
    std::string log_dir = table_dir_path_ + "/binlog/";
    dir = opendir(log_dir.c_str());
    while ((ptr = readdir(dir)) != NULL) {
        if (ptr->d_name[0] == '.')
            continue;
        std::string log = log_dir + ptr->d_name;
        file_path.emplace_back(log);
    }
    std::ofstream my_cout(table_dir_path_.substr(table_dir_path_.find_last_of("/") + 1) + "_result.csv");
    for (int i = 0; i < schema_.size(); ++i) {
        my_cout << schema_.Get(i).name() << ",";
    }
    my_cout << "\n";
    if (snapshot_path_.length()) {
        ReadSnapshot(my_cout);
    }

    // Sorts binlog files and performs binary search
    std::sort(file_path.begin(), file_path.end());
    int left = 0, right = file_path.size() - 1;
    while (left < right) {
        int mid = (left + right) / 2;
        if (GetLogStartOffset(file_path[mid]) >= offset_)
            right = mid;
        else
            left = mid + 1;
    }
    int start_index;
    if (right == 0)
        start_index = right;
    else
        start_index = right - 1;

    // Read binlog files
    for (uint64_t i = start_index; i < file_path.size(); ++i) {
        printf("--------start ReadLog %s--------\n", file_path[i].c_str());
        std::string log_path = file_path[i];
        ReadLog(log_path, my_cout);
        printf("---------end ReadLog %s---------\n", file_path[i].c_str());
    }
    my_cout.close();
}

uint64_t Exporter::GetLogStartOffset(std::string &log_path) {
    FILE* fd_r = fopen(log_path.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", log_path.c_str());
        return 0;
    }
    SequentialFile* rf = NewSeqFile(log_path, fd_r);
    std::string scratch;
    bool is_compress = false;

    Reader reader(rf, NULL, true, 0, is_compress);
    Status status;

    Slice first_value;
    status = reader.ReadRecord(&first_value, &scratch);
    ::openmldb::api::LogEntry first_entry;
    first_entry.ParseFromString(first_value.ToString());
    printf("The start offset of binlog file %s is %lu, ", log_path.c_str(), first_entry.log_index());
    if (first_entry.log_index() < offset_) {
        printf("which is less than snapshot's offset.\n");
    } else {
        printf("which is greater than snapshot's offset.\n");
    }
    return first_entry.log_index();
}

void Exporter::ReadLog(const std::string &log_path, std::ofstream& my_cout) {
    FILE* fd_r = fopen(log_path.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", log_path.c_str());
        return;
    }

    SequentialFile* rf = NewSeqFile(log_path, fd_r);
    std::string scratch;
    bool is_compress = false;

    Reader reader(rf, NULL, true, 0, is_compress);
    Status status;
    uint64_t success_cnt = 0;

    do {
        Slice value;
        status = reader.ReadRecord(&value, &scratch);
        if (!status.ok()) {
            // Finish reading the file.
            break;
        }
        ::openmldb::api::LogEntry entry;
        entry.ParseFromString(value.ToString());

        // Determine if there is a dimension with an idx of 0 in the dimensions.
        // If so, parse the value, else skip it
        if (entry.dimensions_size() != 0) {
            for (int i = 0; i < entry.dimensions_size(); i++) {
                if (entry.dimensions(i).idx() == 0) {
                    if (entry.log_index() > offset_) {
                        std::string row;
                        row = entry.value();
                        RowView view(schema_);
                        view.Reset(reinterpret_cast<int8_t*>(&(row[0])), row.size());
                        WriteToFile(view, my_cout);
                        success_cnt++;
                    }
                    break;
                }
            }
        }
    } while (status.ok());
    printf("--------success_cnt: %lu--------\n", success_cnt);
    offset_ += success_cnt;
}

void Exporter::ReadSnapshot(std::ofstream& my_cout) {
    printf("--------start ReadSnapshot--------\n");
    FILE* fd_r = fopen(snapshot_path_.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", snapshot_path_.c_str());
        return;
    }
    SequentialFile* rf = NewSeqFile(snapshot_path_, fd_r);
    std::string scratch;
    bool is_compress = false;
    if (snapshot_path_.find(openmldb::log::ZLIB_COMPRESS_SUFFIX) != std::string::npos ||
        snapshot_path_.find(openmldb::log::SNAPPY_COMPRESS_SUFFIX) != std::string::npos) {
        is_compress = true;
    }
    Reader reader(rf, NULL, true, 0, is_compress);
    Status status;
    do {
        Slice value;
        status = reader.ReadRecord(&value, &scratch);
        if (!status.ok()) {
            // Finish reading the file.
            break;
        }
        ::openmldb::api::LogEntry entry;
        entry.ParseFromString(value.ToString());

        // Determine if there is a dimension with an idx of 0 in the dimensions.
        // If so, parse the value, else skip it
        if (entry.dimensions_size() != 0) {
            for (int i = 0; i < entry.dimensions_size(); i++) {
                if (entry.dimensions(i).idx() == 0) {
                    std::string row;
                    row = entry.value();
                    RowView view(schema_);
                    view.Reset(reinterpret_cast<int8_t*>(&(row[0])), row.size());
                    WriteToFile(view, my_cout);
                    break;
                }
            }
        }
    } while (status.ok());
    printf("----------end ReadSnapshot----------\n");
}

void Exporter::WriteToFile(::openmldb::codec::RowView& view, std::ofstream& my_cout) {
    // Gets the values for each column, then writes the row to the csv file.
    for (int i = 0; i< schema_.size(); ++i) {
        std::string col;
        view.GetStrValue(i, &col);
        my_cout << col << ",";
    }
    my_cout << "\n";
}

}  // namespace tools
}  // namespace openmldb
