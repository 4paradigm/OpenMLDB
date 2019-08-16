
// file_receiver.cc
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-15
//

#include "tablet/file_receiver.h"
#include "base/file_util.h"
#include "base/strings.h"
#include "logging.h"
#include <boost/algorithm/string/predicate.hpp>

using ::baidu::common::INFO;
using ::baidu::common::WARNING;
using ::baidu::common::DEBUG;

namespace rtidb {
namespace tablet {

FileReceiver::FileReceiver(const std::string& file_name, const std::string& path):
        file_name_(file_name), path_(path), size_(0), block_id_(0), file_(NULL) {
}

FileReceiver::~FileReceiver() {
    if (file_) fclose(file_);
}

int FileReceiver::Init() {
    if (file_) {
        fclose(file_);
        file_ = NULL;
    }
    if (!boost::ends_with(path_, "/")) {
        path_.append("/");
    }
    if (!::rtidb::base::MkdirRecur(path_)) {
        PDLOG(WARNING, "mkdir failed! path[%s]", path_.c_str());
        return -1;
    }
    std::string full_path = path_ + file_name_ + ".tmp";
    FILE* file = fopen(full_path.c_str(), "wb");
    if (file == NULL) {
        PDLOG(WARNING, "fail to open file %s", full_path.c_str());
        return -1;
    }
    file_ = file;
    return 0;
}

uint64_t FileReceiver::GetBlockId() {
	return block_id_;
}

int FileReceiver::WriteData(const std::string& data, uint64_t block_id) {
    if (file_ == NULL) {
        PDLOG(WARNING, "file is NULL");
		return -1;
    }
    if (block_id <= block_id_) {
        PDLOG(DEBUG, "block id %lu has been received", block_id);
        return 0;
    }
	size_t r = fwrite_unlocked(data.c_str(), 1, data.size(), file_);
	if (r < data.size()) {
		PDLOG(WARNING, "write error. name %s%s", path_.c_str(), file_name_.c_str());
		return -1;
	}
	size_ += r;
	block_id_ = block_id;
    return 0;
}

void FileReceiver::SaveFile() {
    std::string full_path = path_ + file_name_;
    std::string tmp_file_path = full_path + ".tmp";
    if (::rtidb::base::IsExists(full_path)) {
        std::string backup_file = full_path + "." + ::rtidb::base::GetNowTime();
        rename(full_path.c_str(), backup_file.c_str());
    }
    rename(tmp_file_path.c_str(), full_path.c_str());
    PDLOG(INFO, "file %s received. size %lu", full_path.c_str(), size_);
}

}
}
