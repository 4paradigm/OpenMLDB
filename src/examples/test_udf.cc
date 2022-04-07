#include "udf/openmldb_udf.h"

extern "C"
void cut2(UDFContext* ctx, StringRef* input, StringRef* output) {
    if (input == nullptr || output == nullptr) {
        return;
    }
    uint32_t size = input->size_ <= 2 ? input->size_ : 2;
    char *buffer = ctx->pool->Alloc(size);
    memcpy(buffer, input->data_, size);
    output->size_ = size;
    output->data_ = buffer;
}

extern "C"
int strlength(UDFContext* ctx, StringRef* input) {
    if (input == nullptr) {
        return 0;
    }
    return input->size_;
}

extern "C"
void int2str(UDFContext* ctx, int input, StringRef* output) {
    std::string tmp = std::to_string(input);
    char *buffer = ctx->pool->Alloc(tmp.length());
    memcpy(buffer, tmp.data(), tmp.length());
    output->size_ = tmp.length();
    output->data_ = buffer;
}
