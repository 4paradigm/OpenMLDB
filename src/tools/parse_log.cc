//
// tools/parse_log.cc
// Copyright (C) 2020 4paradigm.com
// Author wangbao
// Date 2020-07-06
//

#include <gflags/gflags.h>
#include <sched.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <iostream>
#include <fstream>
#include "base/file_util.h"
#include "log/log_reader.h"
#include "log/log_writer.h"
#include "proto/tablet.pb.h"

using ::rtidb::log::SequentialFile;
using ::rtidb::log::Reader;
using ::rtidb::base::Slice;
using ::rtidb::base::Status;
using ::rtidb::log::NewSeqFile;
using ::rtidb::base::ParseFileNameFromPath;

namespace rtidb {
namespace tools {

void ReadLog(const std::string& full_path) {
    std::string fname = ParseFileNameFromPath(full_path);
    std::ofstream my_cout(fname + "_result.txt");
    FILE* fd_r = fopen(full_path.c_str(), "rb");
    if (fd_r == NULL) {
        printf("fopen failed: %s\n", full_path.c_str());
        return;
    }
    SequentialFile* rf = NewSeqFile(full_path, fd_r);
    std::string scratch;
    bool for_snapshot = false;
    if (full_path.find(rtidb::log::ZLIB_COMPRESS_SUFFIX) != std::string::npos
            || full_path.find(rtidb::log::SNAPPY_COMPRESS_SUFFIX) != std::string::npos) {
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
        ::rtidb::api::LogEntry entry;
        entry.ParseFromString(value.ToString());
        if (entry.ts_dimensions_size() == 0) {
            my_cout << entry.ts() << std::endl;
        } else {
            for (int i = 0; i < entry.ts_dimensions_size(); i++) {
                my_cout << entry.ts_dimensions(i).idx() << "\t" <<
                    entry.ts_dimensions(i).ts() << std::endl;
            }
        }
        if (entry.dimensions_size() == 0) {
            my_cout << entry.pk() << std::endl;
        } else {
            for (int i = 0; i < entry.dimensions_size(); i++) {
                my_cout << entry.dimensions(i).idx() << "\t"
                    << entry.dimensions(i).key().c_str() << std::endl;
            }
        }
        success_cnt++;
    } while (status.ok());
    my_cout.close();
    printf("--------success_cnt: %lu\n", success_cnt);
}

}  // namespace tools
}  // namespace rtidb

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    printf("--------start readlog--------\n");
    printf("--------full_path: %s\n", argv[1]);
    rtidb::tools::ReadLog(argv[1]);
    printf("--------end readlog--------\n");
    return 0;
}
