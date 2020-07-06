//
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

using ::rtidb::base::Slice;
using ::rtidb::base::Status;

namespace rtidb {
namespace log {

    void ReadLog(const std::string& log_dir,
            const std::string& fname) {
        std::ofstream my_count_1("ts.txt");
        std::ofstream my_count_2("key.txt");
        std::string full_path = log_dir + "/" + fname;
        printf("--------full_path: %s\n", full_path.c_str());
        FILE* fd_r = fopen(full_path.c_str(), "rb");
        if (fd_r == NULL) {
            printf("fopen failed: %s\n", full_path.c_str());
            return;
        }
        SequentialFile* rf = NewSeqFile(fname, fd_r);
        std::string scratch2;
        Reader reader(rf, NULL, true, 0);
        Status status;
        uint64_t success_cnt = 0;
        do {
            Slice value2;
            status = reader.ReadRecord(&value2, &scratch2);
            if (!status.ok()) {
                break;
            }
            ::rtidb::api::LogEntry entry2;
            entry2.ParseFromString(value2.ToString());
            my_count_1 << entry2.ts() << std::endl;
            my_count_2 << entry2.dimensions(0).key().c_str() << std::endl;
            success_cnt++;
        } while (status.ok());
        my_count_1.close();
        my_count_2.close();
        printf("--------success_cnt: %lu\n", success_cnt);
    }

}  // namespace log
}  // namespace rtidb

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    printf("--------start readlog--------\n");
    rtidb::log::ReadLog(argv[1], argv[2]);
    printf("--------end readlog--------\n");
    return 0;
}
