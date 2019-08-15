// file_reciver.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2019-08-14
//

#pragma once

#include <string>

namespace rtidb {
namespace tablet {

class FileReceiver {
public:
    FileReceiver(const std::string& file_name, const std::string& path);
    ~FileReceiver();
    FileReceiver(const FileReceiver&) = delete;
    FileReceiver& operator = (const FileReceiver&) = delete;
    int Init();
    int WriteData(const std::string& data, uint64_t block_id);
    void SaveFile();
    uint64_t GetBlockId();

private:
    std::string file_name_;
    std::string path_;
    uint64_t size_;
    uint64_t block_id_;
    FILE* file_;
};

}
}
