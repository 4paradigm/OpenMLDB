/*-------------------------------------------------------------------------
 * Copyright (C) 2019, 4paradigm
 * window_functions_def.cc
 *--------------------------------------------------------------------------
 **/
#include <algorithm>
#include <string>
#include <tuple>
#include <unordered_set>
#include <utility>
#include <vector>

#include "boost/algorithm/string.hpp"
#include "boost/algorithm/string/join.hpp"
#include "boost/algorithm/string/regex.hpp"

#include "codec/list_iterator_codec.h"
#include "codec/type_codec.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"
#include "vm/jit_runtime.h"

namespace fesql {
namespace udf {

using fesql::codec::ListRef;
using fesql::codec::StringRef;

/**
 * A mutable string ArrayListV
 */
class MutableStringListV : public codec::ListV<StringRef> {
 public:
    MutableStringListV() {}
    ~MutableStringListV() {}

    std::unique_ptr<base::ConstIterator<uint64_t, StringRef>> GetIterator()
        const override;
    base::ConstIterator<uint64_t, StringRef>* GetIterator(
        int8_t* addr) const override;

    const uint64_t GetCount() override { return buffer_.size(); }

    StringRef At(uint64_t pos) override { return StringRef(buffer_[pos]); }

    void Add(const std::string& str) {
        if (total_len_ + str.size() > MAXIMUM_STRING_LENGTH) {
            return;
        }
        buffer_.push_back(str);
        total_len_ += str.size();
    }

 protected:
    static const size_t MAXIMUM_STRING_LENGTH = 4096;
    std::vector<std::string> buffer_;
    size_t total_len_ = 0;
};

class MutableStringListVIterator
    : public base::ConstIterator<uint64_t, StringRef> {
 public:
    explicit MutableStringListVIterator(const std::vector<std::string>* buffer)
        : buffer_(buffer), iter_(buffer->cbegin()), key_(0) {
        if (Valid()) {
            tmp_ = StringRef(*iter_);
        }
    }

    ~MutableStringListVIterator() {}

    void Seek(const uint64_t& key) override {
        iter_ = (buffer_->cbegin() + key) >= buffer_->cend()
                    ? buffer_->cend()
                    : buffer_->cbegin() + key;
    }

    bool Valid() const override { return buffer_->cend() != iter_; }

    void Next() override {
        ++iter_;
        if (Valid()) {
            tmp_ = StringRef(*iter_);
        }
    }

    const StringRef& GetValue() override { return tmp_; }

    const uint64_t& GetKey() const override { return key_; }

    void SeekToFirst() {
        iter_ = buffer_->cbegin();
        if (Valid()) {
            tmp_ = StringRef(*iter_);
        }
    }

    bool IsSeekable() const override { return true; }

 protected:
    const std::vector<std::string>* buffer_;
    typename std::vector<std::string>::const_iterator iter_;
    StringRef tmp_;
    uint64_t key_;
};

std::unique_ptr<base::ConstIterator<uint64_t, StringRef>>
MutableStringListV::GetIterator() const {
    return std::unique_ptr<MutableStringListVIterator>(
        new MutableStringListVIterator(&buffer_));
}
base::ConstIterator<uint64_t, StringRef>* MutableStringListV::GetIterator(
    int8_t* addr) const {
    if (nullptr == addr) {
        return new MutableStringListVIterator(&buffer_);
    } else {
        return new (addr) MutableStringListVIterator(&buffer_);
    }
}

/**
 * ListV && ListRef Wrapper whose lifetime is managed by jit runtime.
 */
class StringListWrapper : public base::FeBaseObject {
 public:
    StringListWrapper() { list_ref_.list = reinterpret_cast<int8_t*>(&list_); }

    ListRef<StringRef>* GetListRef() { return &list_ref_; }

    MutableStringListV* GetListV() { return &list_; }

 private:
    MutableStringListV list_;
    ListRef<StringRef> list_ref_;
};

struct FZStringOpsDef {
    static StringListWrapper* InitList() {
        auto list = new StringListWrapper();
        vm::JITRuntime::get()->AddManagedObject(list);
        return list;
    }

    static void OutputList(StringListWrapper* ptr, ListRef<StringRef>* output) {
        *output = *ptr->GetListRef();
    }

    static StringListWrapper* UpdateSplit(StringListWrapper* ptr,
                                          StringRef* str, bool is_null,
                                          StringRef* delimeter) {
        if (is_null) {
            return ptr;
        }
        auto list = ptr->GetListV();
        std::vector<std::string> parts;
        boost::split_regex(parts, str->ToString(),
                           boost::regex(delimeter->ToString()));
        for (auto& part : parts) {
            list->Add(part);
        }
        return ptr;
    }

    static StringListWrapper* UpdateSplitByKey(StringListWrapper* ptr,
                                               StringRef* str, bool is_null,
                                               StringRef* delimeter) {
        if (is_null) {
            return ptr;
        }
        auto list = ptr->GetListV();
        std::vector<std::string> parts;
        boost::split_regex(parts, str->ToString(),
                           boost::regex(delimeter->ToString()));
        for (auto& part : parts) {
            std::vector<std::string> sub_parts;
            boost::split(sub_parts, part, boost::is_any_of(":"));
            if (sub_parts.size() >= 2) {
                list->Add(sub_parts[0]);
            }
        }
        return ptr;
    }

    static StringListWrapper* UpdateSplitByValue(StringListWrapper* ptr,
                                                 StringRef* str, bool is_null,
                                                 StringRef* delimeter) {
        if (is_null) {
            return ptr;
        }
        auto list = ptr->GetListV();
        std::vector<std::string> parts;
        boost::split_regex(parts, str->ToString(),
                           boost::regex(delimeter->ToString()));
        for (auto& part : parts) {
            std::vector<std::string> sub_parts;
            boost::split(sub_parts, part, boost::is_any_of(":"));
            if (sub_parts.size() >= 2) {
                list->Add(sub_parts[1]);
            }
        }
        return ptr;
    }

    static void StringJoin(ListRef<StringRef>* list_ref, StringRef* delimeter,
                           StringRef* output) {
        auto list = reinterpret_cast<codec::ListV<StringRef>*>(list_ref->list);
        auto iter = list->GetIterator();
        std::string delim = delimeter->ToString();

        size_t bytes = 0;
        while (iter->Valid()) {
            auto& next = iter->GetValue();
            bytes += next.size_;
            iter->Next();
            if (iter->Valid()) {
                bytes += delim.size();
            }
        }
        char* buf = v1::AllocManagedStringBuf(bytes + 1);
        buf[bytes] = 0;

        size_t offset = 0;
        iter->SeekToFirst();
        while (iter->Valid()) {
            auto& next = iter->GetValue();
            std::copy_n(next.data_, next.size_, buf + offset);
            offset += next.size_;
            iter->Next();
            if (iter->Valid()) {
                std::copy_n(delim.c_str(), delim.size(), buf + offset);
                offset += delim.size();
            }
        }
        output->size_ = bytes;
        output->data_ = buf;
    }
};

void DefaultUDFLibrary::InitFeatureZero() {
    RegisterUDAF("fz_split")
        .templates<ListRef<StringRef>, Opaque<StringListWrapper>,
                   Nullable<StringRef>, StringRef>()
        .init("fz_split_init", FZStringOpsDef::InitList)
        .update("fz_split_update", FZStringOpsDef::UpdateSplit)
        .output("fz_split_output", FZStringOpsDef::OutputList);

    RegisterUDAF("fz_split_by_key")
        .templates<ListRef<StringRef>, Opaque<StringListWrapper>,
                   Nullable<StringRef>, StringRef>()
        .init("fz_split_by_key_init", FZStringOpsDef::InitList)
        .update("fz_split_by_key_update", FZStringOpsDef::UpdateSplitByKey)
        .output("fz_split_by_key_output", FZStringOpsDef::OutputList);

    RegisterUDAF("fz_split_by_value")
        .templates<ListRef<StringRef>, Opaque<StringListWrapper>,
                   Nullable<StringRef>, StringRef>()
        .init("fz_split_by_value_init", FZStringOpsDef::InitList)
        .update("fz_split_by_value_update", FZStringOpsDef::UpdateSplitByValue)
        .output("fz_split_by_value_output", FZStringOpsDef::OutputList);

    RegisterExternal("fz_join")
        .list_argument_at(0)
        .args<ListRef<StringRef>, StringRef>(FZStringOpsDef::StringJoin);
}

}  // namespace udf
}  // namespace fesql
