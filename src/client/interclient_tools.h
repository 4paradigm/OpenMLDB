//
// Copyright 2020 4paradigm
//

#pragma once
#include <memory>
#include "client/client_type.h"

bool PutBlob(BlobInfoResult& blobInfo, BlobOPResult& result, // NOLINT
             char* ch, int64_t len);

void GetBlob(BlobInfoResult& blobInfo, BlobOPResult& result, // NOLINT
             char** packet, int64_t* sz);
