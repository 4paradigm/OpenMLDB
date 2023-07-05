#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_library.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

void combin_3id(::openmldb::base::StringRef* str1, ::openmldb::base::StringRef* str2, ::openmldb::base::StringRef* str3,
                ::openmldb::base::StringRef* output) {
    if (str1 == nullptr || str2 == nullptr || str3 == nullptr || output == nullptr) {
        return;
    }

    std::string delimiter = "|";
    std::string pair_delimiter = ":";
    std::string result_delimiter = ",";
    std::string str1_std(str1->data_, str1->size_);
    std::string str2_std(str2->data_, str2->size_);
    std::string str3_std(str3->data_, str3->size_);

    // Generate the output string
    std::ostringstream oss;
    std::istringstream token_stream(str3_std);
    std::string token, key;
    while (std::getline(token_stream, token, delimiter[0])) {
        std::istringstream pair_stream(token);
        std::getline(pair_stream, key, pair_delimiter[0]);
        oss << str1_std << "^" << str2_std << "^" << key << result_delimiter;
    }

    // Remove the trailing delimiter
    std::string result_str = oss.str();
    result_str = result_str.substr(0, result_str.size() - result_delimiter.size());

    // Allocate the memory in the UDFContext's memory pool and fill in the result
    char* buffer = v1::AllocManagedStringBuf(result_str.size());
    memcpy(buffer, result_str.data(), result_str.size());
    output->size_ = result_str.size();
    output->data_ = buffer;
}

void DefaultUdfLibrary::InitThirdDefs() {
    RegisterExternal("combin_3id")
        .args<openmldb::base::StringRef, openmldb::base::StringRef, openmldb::base::StringRef>(combin_3id)
        .doc(R"(combine three id)");
}

}  // namespace udf
}  // namespace hybridse
