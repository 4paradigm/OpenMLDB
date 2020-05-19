//
// Copyright 2020 4paradigm
//

#include "client/interclient_tools.h"

bool PutBlob(BlobInfoResult& blobInfo, char* ch, int64_t len) { // NOLINT
    return blobInfo.client_->Put(blobInfo.tid_, 0, ch, len, &blobInfo.key_,
                                    &blobInfo.msg_);
}

void GetBlob(BlobInfoResult& blobInfo, char** packet, int64_t* sz) { // NOLINT
    butil::IOBuf buf;
    bool ok = blobInfo.client_->Get(blobInfo.tid_, 0, blobInfo.key_,
                                    &blobInfo.msg_, &buf);
    if (!ok) {
        blobInfo.code_ = -1;
        return;
    }
    *packet = reinterpret_cast<char*>(malloc(buf.size()));
    *sz = buf.size();
    char* ch = &(*packet)[0];
    int64_t remain_size = buf.size();
    int64_t start_pos = 0;
    while (remain_size > 0) {
        int64_t n = buf.copy_to(static_cast<void*>(ch), buf.size(), start_pos);
        remain_size -= n;
        start_pos += n;
        ch += n;
    }

    return;
}
