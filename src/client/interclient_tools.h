#include <memory>
#include "client/client_type.h"

bool PutBlob(BlobInfoResult& blobInfo, char* ch, int64_t len);

void GetBlob(BlobInfoResult& blobInfo, char** packet, int64_t* sz);
