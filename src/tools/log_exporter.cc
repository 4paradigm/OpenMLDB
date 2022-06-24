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

#include <gflags/gflags.h>
#include <sched.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>

#include <fstream>
#include <iostream>

#include "base/file_util.h"
#include "codec/codec.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"
#include "proto/common.pb.h"
#include "tools/log_exporter.h"

using ::openmldb::base::ParseFileNameFromPath;
using ::openmldb::base::Slice;
using ::openmldb::log::NewSeqFile;
using ::openmldb::log::Reader;
using ::openmldb::log::SequentialFile;
using ::openmldb::log::Status;

namespace openmldb {
namespace tools {

void Exporter::ReadLog() {
    std::string fname = ParseFileNameFromPath(log_path);
    std::ofstream my_cout(fname + "_result.csv");
    FILE* fd_r = fopen(log_path.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", log_path.c_str());
        return;
    }
    SequentialFile* rf = NewSeqFile(log_path, fd_r);
    std::string scratch;
    bool for_snapshot = false;
    if (log_path.find(openmldb::log::ZLIB_COMPRESS_SUFFIX) != std::string::npos ||
        log_path.find(openmldb::log::SNAPPY_COMPRESS_SUFFIX) != std::string::npos) {
        for_snapshot = true;
    }
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

                    std::string row;
                    row = entry.value();

                    ::openmldb::codec::RowView view(schema, reinterpret_cast<int8_t*>(&(row[0])), row.size());

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
                    success_cnt++;
                    break;
                }
            }
        }
    } while (status.ok());
    my_cout.close();
    printf("--------success_cnt: %lu\n", success_cnt);
}
}  // namespace tools
}  // namespace openmldb

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::openmldb::tools::Exporter exporter = ::openmldb::tools::Exporter(argv[1]);
    printf("--------start readlog--------\n");
    printf("--------log_path: %s\n", argv[1]);
    exporter.ReadLog();
    printf("--------end readlog--------\n");
    return 0;
}
