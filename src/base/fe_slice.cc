// Copyright (C) 2019, 4paradigm
#include "base/fe_slice.h"

namespace fesql {
namespace base {

SharedSliceRef Slice::CreateManaged(const int8_t* buf, size_t size) {
    return std::shared_ptr<Slice>(
        new Slice(reinterpret_cast<const char*>(buf), size, true));
}

SharedSliceRef Slice::Create(const int8_t* buf, size_t size) {
    return std::shared_ptr<Slice>(
        new Slice(reinterpret_cast<const char*>(buf), size, false));
}

SharedSliceRef Slice::CreateFromCStr(const char* str) {
    return std::shared_ptr<Slice>(new Slice(str));
}

SharedSliceRef Slice::CreateFromCStr(const std::string& str) {
    return std::shared_ptr<Slice>(new Slice(str));
}

SharedSliceRef Slice::CreateEmpty() {
    return std::shared_ptr<Slice>(new Slice());
}


}  // namespace base
}  // namespace fesql
