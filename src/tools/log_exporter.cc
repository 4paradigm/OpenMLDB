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
#include "log_exporter.h"

using ::openmldb::base::ParseFileNameFromPath;
using ::openmldb::base::Slice;
using ::openmldb::log::NewSeqFile;
using ::openmldb::log::Reader;
using ::openmldb::log::SequentialFile;
using ::openmldb::log::Status;
using ::openmldb::codec::Schema;
using ::openmldb::codec::RowView;

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
                            // TODO: Create schema for common cases
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

//                            for (int i = 0; i< schema.size(); ++i) {
//                                view.GetValue(reinterpret_cast<int8_t *>(&(row[0])), i, &ch, &length);
//                                std::string ret(ch, length);
//                                //printf("The value is %s\n", ret);
//                                my_cout << ret << std::endl;
//                            }

                            // Gets the values for each column.
                            char* ch = NULL;
                            uint32_t length = 0;
                            view.GetString(0, &ch, &length);
                            std::string c1_val(ch, length);
                            int32_t c2_val;
                            view.GetInt32(1, &c2_val);
                            int64_t c3_val;
                            view.GetInt64(2, &c3_val);
                            float c4_val;
                            view.GetFloat(3, &c4_val);
                            double c5_val;
                            view.GetDouble(4, &c5_val);
                            int64_t c6_val;
                            view.GetTimestamp(5, &c6_val);
                            uint32_t year, month, day;
                            view.GetDate(6, &year, &month, &day);
                            std::string c7_val;

                            // Writes the row to the csv file.
                            c7_val = std::to_string(year) + "-" + std::to_string(month) +"-" + std::to_string(day);
                            my_cout << c1_val << "," << c2_val << "," << c3_val << "," << c4_val << "," <<
                                        c5_val << "," << c6_val << "," << c7_val << "\n";

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
