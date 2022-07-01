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

#include <gflags/gflags.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <dirent.h>

#include <algorithm>
#include <fstream>
#include <iostream>

#include "base/file_util.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "proto/common.pb.h"
#include "storage/snapshot.h"

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
    std::string manifest_path = table_dir_path + "/snapshot/MANIFEST";
    ::openmldb::api::Manifest manifest;
    ::openmldb::storage::Snapshot::GetLocalManifest(manifest_path, manifest);
    std::string snapshot_name = manifest.name();
    snapshot_path = table_dir_path + "/snapshot/" +snapshot_name;
    offset = manifest.offset();
    printf("--------snapshot offset: %lu\n", offset);
    printf("--------snapshot path: %s\n", snapshot_path.c_str());
    printf("--------end readManifest--------\n");
}

void Exporter::ExportTable() {
    std::vector<std::string> file_path;
    // Add all binlog files to file_path lists
    struct dirent *ptr;
    DIR *dir;
    std::string log_dir = table_dir_path + "/binlog/";
    dir=opendir(log_dir.c_str());
    while ((ptr = readdir(dir)) != NULL) {
        if (ptr->d_name[0] == '.')
            continue;
        std::string log = log_dir + ptr->d_name;
        file_path.emplace_back(log);
    }

    std::ofstream my_cout(table_dir_path + "_result.csv");
    for (int i = 0; i < schema.size(); ++i) {
        my_cout << schema.Get(i).name() << ",";
    }
    my_cout << "\n";
    ReadSnapshot(my_cout);

    // Sorts binlog files and performs binary search
    std::sort(file_path.begin(), file_path.end());
    int left = 0, right = file_path.size() - 1;
    while (left < right) {
        int mid = (left + right) / 2;
        if (GetLogStartOffset(file_path[mid]) >= offset)
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
        ReadLog(my_cout, log_path);
        printf("--------end ReadLog %s--------\n", file_path[i].c_str());
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
    bool for_snapshot = false;

    Reader reader(rf, NULL, true, 0, for_snapshot);
    Status status;

    Slice first_value;
    status = reader.ReadRecord(&first_value, &scratch);
    ::openmldb::api::LogEntry first_entry;
    first_entry.ParseFromString(first_value.ToString());
    printf("The start offset of binlog file %s is %lu, \n", log_path.c_str(), first_entry.log_index());
    if (first_entry.log_index() < offset) {
        printf("which is less than snapshot's offset.\n");
    } else {
        printf("which is greater than snapshot's offset.\n");
    }
    return first_entry.log_index();
}

void Exporter::ReadLog(std::ofstream& my_cout, std::string &log_path) {
    FILE* fd_r = fopen(log_path.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", log_path.c_str());
        return;
    }

    SequentialFile* rf = NewSeqFile(log_path, fd_r);
    std::string scratch;
    bool for_snapshot = false;

    Reader reader(rf, NULL, true, 0, for_snapshot);
    Status status;
    uint64_t success_cnt = 0;

    do {
        Slice value;
        status = reader.ReadRecord(&value, &scratch);
        if (!status.ok()) {
            break;
        }
        ::openmldb::api::LogEntry entry;
        entry.ParseFromString(value.ToString());

        // Determine if there is a dimension with an idx of 0 in the dimensions.
        // If so, parse the value, else skip it
        if (entry.dimensions_size() != 0) {
            for (int i = 0; i < entry.dimensions_size(); i++) {
                if (entry.dimensions(i).idx() == 0) {
                    if (entry.log_index() > offset) {
                        std::string row;
                        row = entry.value();
                        RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), row.size());
                        WriteToFile(my_cout, view);
                        success_cnt++;
                        break;
                    }
                }
            }
        }
    } while (status.ok());
    printf("--------success_cnt: %lu\n", success_cnt);
    offset += success_cnt;
}

void Exporter::ReadSnapshot(std::ofstream& my_cout) {
    printf("--------start ReadSnapshot--------\n");
    FILE* fd_r = fopen(snapshot_path.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", snapshot_path.c_str());
        return;
    }
    SequentialFile* rf = NewSeqFile(snapshot_path, fd_r);
    std::string scratch;
    bool for_snapshot = false;
    if (snapshot_path.find(openmldb::log::ZLIB_COMPRESS_SUFFIX) != std::string::npos ||
        snapshot_path.find(openmldb::log::SNAPPY_COMPRESS_SUFFIX) != std::string::npos) {
        for_snapshot = true;
    }
    Reader reader(rf, NULL, true, 0, for_snapshot);
    Status status;
    do {
        Slice value;
        status = reader.ReadRecord(&value, &scratch);
        if (!status.ok()) {
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
                    RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), row.size());
                    WriteToFile(my_cout, view);
                    break;
                }
            }
        }
    } while (status.ok());
    printf("--------end ReadSnapshot--------\n");
}

void Exporter::WriteToFile(std::ofstream &my_cout, ::openmldb::codec::RowView& view) {
    // Gets the values for each column, then writes the row to the csv file.
    for (int i = 0; i< schema.size(); ++i) {
        ::openmldb::type::DataType type = schema.Get(i).data_type();
        switch (type) {
            case openmldb::type::kBool: {
                bool val;
                view.GetBool(i, &val);
                my_cout << std::to_string(val) << ",";
            }
                break;
            case openmldb::type::kSmallInt: {
                int16_t val2;
                view.GetInt16(i, &val2);
                my_cout << std::to_string(val2) << ",";
            }
                break;
            case openmldb::type::kInt: {
                int32_t val3;
                view.GetInt32(i, &val3);
                my_cout << std::to_string(val3) << ",";
            }
                break;
            case openmldb::type::kBigInt: {
                int64_t val4;
                view.GetInt64(i, &val4);
                my_cout << std::to_string(val4) << ",";
            }
                break;
            case openmldb::type::kFloat: {
                float val5;
                view.GetFloat(i, &val5);
                my_cout << std::to_string(val5) << ",";
            }
                break;
            case openmldb::type::kDouble: {
                double val6;
                view.GetDouble(i, &val6);
                my_cout << std::to_string(val6) << ",";
            }
                break;
            case openmldb::type::kTimestamp: {
                int64_t val7;
                view.GetTimestamp(i, &val7);
                my_cout << std::to_string(val7) << ",";
            }
                break;
            case openmldb::type::kVarchar:
            case openmldb::type::kString: {
                char *ch;
                uint32_t len = 0;
                view.GetString(i, &ch, &len);
                std::string val8(ch, len);
                my_cout << val8 << ",";
            }
                break;
            case openmldb::type::kDate: {
                uint32_t year, month, day;
                view.GetDate(i, &year, &month, &day);
                std::string val9 =
                        std::to_string(year) + "-" + std::to_string(month) + "-" + std::to_string(day);
                my_cout << val9 << ",";
            }
            default:
                break;
        }
    }
    my_cout << "\n";
}

}  // namespace tools
}  // namespace openmldb


int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::tools::Exporter exporter = ::openmldb::tools::Exporter(argv[1]);

    exporter.ReadManifest();

    ::openmldb::codec::Schema schema;
    // TODO(xiaopanz): Create schema for common cases
    // Currently only works on the data from the quickstart.
    ::openmldb::common::ColumnDesc* col = schema.Add();
    col->set_name("c1");
    col->set_data_type(::openmldb::type::kString);
    col = schema.Add();
    col->set_name("c2");
    col->set_data_type(::openmldb::type::kInt);
    col = schema.Add();
    col->set_name("c3");
    col->set_data_type(::openmldb::type::kBigInt);
    col = schema.Add();
    col->set_name("c4");
    col->set_data_type(::openmldb::type::kFloat);
    col = schema.Add();
    col->set_name("c5");
    col->set_data_type(::openmldb::type::kDouble);
    col = schema.Add();
    col->set_name("c6");
    col->set_data_type(::openmldb::type::kTimestamp);
    col = schema.Add();
    col->set_name("c7");
    col->set_data_type(::openmldb::type::kDate);
    exporter.SetSchema(schema);

    printf("--------start ExportData--------\n");
    exporter.ExportTable();
    printf("--------end ExportData--------\n");
    return 0;
}
