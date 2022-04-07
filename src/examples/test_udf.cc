#include "udf/openmldb_udf.h"

extern "C"
int myfun(UDFContext* ctx, int input) {
    return input + 2;
}

extern "C"
void cut2(UDFContext* ctx, StringRef* input, StringRef* output, bool* is_null) {
    if (input == nullptr || output == nullptr) {
        *is_null = true;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    char *buffer = ctx->pool->Alloc(size);
    memcpy(buffer, input->data_, size);
    output->size_ = size;
    output->data_ = buffer;
    *is_null = false;
}
