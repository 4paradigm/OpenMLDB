//
// Copyright 2020 4paradigm
//

#pragma once
#include <memory>
#include "client/client_type.h"

bool PutBlob(BlobInfoResult& blobInfo, char* ch, int64_t len); // NOLINT

void GetBlob(BlobInfoResult& blobInfo, char** packet, int64_t* sz); // NOLINT
