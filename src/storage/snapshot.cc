//
// snapshot.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-07
//

#include "storage/snapshot.h"
#include "logging.h"
#include <google/protobuf/text_format.h>
#include <google/protobuf/io/zero_copy_stream_impl.h>
#include <unistd.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

using ::baidu::common::DEBUG;
using ::baidu::common::INFO;
using ::baidu::common::WARNING;

namespace rtidb {
namespace storage {

const std::string MANIFEST = "MANIFEST";    

int Snapshot::GenManifest(const std::string& snapshot_name, uint64_t key_count, uint64_t offset, uint64_t term) {
    PDLOG(DEBUG, "record offset[%lu]. add snapshot[%s] key_count[%lu]",
                offset, snapshot_name.c_str(), key_count);
    std::string full_path = snapshot_path_ + MANIFEST;
    std::string tmp_file = snapshot_path_ + MANIFEST + ".tmp";
    ::rtidb::api::Manifest manifest;
    std::string manifest_info;
    manifest.set_offset(offset);
    manifest.set_name(snapshot_name);
    manifest.set_count(key_count);
    manifest.set_term(term);
    manifest_info.clear();
    google::protobuf::TextFormat::PrintToString(manifest, &manifest_info);
    FILE* fd_write = fopen(tmp_file.c_str(), "w");
    if (fd_write == NULL) {
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
        PDLOG(DEBUG, "%s generate success. path[%s]", MANIFEST.c_str(), full_path.c_str());
        return 0;
    }
    unlink(tmp_file.c_str());
    return -1;
}

int Snapshot::GetLocalManifest(::rtidb::api::Manifest& manifest) {
    std::string full_path = snapshot_path_ + MANIFEST;
    int fd = open(full_path.c_str(), O_RDONLY);
    if (fd < 0) {
        PDLOG(INFO, "[%s] is not exist", MANIFEST.c_str());
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
   
}
}
