/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <algorithm>
#include <queue>
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
#include "udf/containers.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_registry.h"
#include "vm/jit_runtime.h"

namespace hybridse {
namespace udf {

using hybridse::codec::ListRef;
using hybridse::codec::StringRef;
using openmldb::base::Date;
using openmldb::base::Timestamp;

/**
 * A mutable string ArrayListV
 */
class MutableStringListV : public codec::ListV<StringRef> {
 public:
    MutableStringListV() {}
    ~MutableStringListV() {}

    std::unique_ptr<base::ConstIterator<uint64_t, StringRef>> GetIterator()
        override;
    base::ConstIterator<uint64_t, StringRef>* GetRawIterator() override;

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

    void SeekToFirst() override {
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
MutableStringListV::GetIterator() {
    return std::unique_ptr<MutableStringListVIterator>(
        new MutableStringListVIterator(&buffer_));
}
base::ConstIterator<uint64_t, StringRef>* MutableStringListV::GetRawIterator() {
    return new MutableStringListVIterator(&buffer_);
}

/**
 * ListV && ListRef Wrapper whose lifetime is managed by jit runtime.
 */
class StringSplitState : public base::FeBaseObject {
 public:
    StringSplitState() { list_ref_.list = reinterpret_cast<int8_t*>(&list_); }

    ListRef<StringRef>* GetListRef() { return &list_ref_; }

    MutableStringListV* GetListV() { return &list_; }

    bool IsDelimeterInitialized() const { return delims_compiled_; }

    void SetDelimeterInitialized() { delims_compiled_ = true; }

    void InitDelimeter(const std::string& delim) {
        delims_[0] = boost::regex(delim);
    }

    void InitKVDelimeter(const std::string& delim) {
        delims_[1] = boost::regex(delim);
    }

    boost::regex& GetDelimeter() { return delims_[0]; }

    boost::regex& GetKVDelimeter() { return delims_[1]; }

 private:
    MutableStringListV list_;
    ListRef<StringRef> list_ref_;

    boost::regex delims_[2];
    bool delims_compiled_ = false;
};

struct FZStringOpsDef {
    static StringSplitState* InitList() {
        auto list = new StringSplitState();
        vm::JitRuntime::get()->AddManagedObject(list);
        return list;
    }

    static void OutputList(StringSplitState* state,
                           ListRef<StringRef>* output) {
        *output = *state->GetListRef();
    }

    static StringSplitState* UpdateSplit(StringSplitState* state,
                                         StringRef* str, bool is_null,
                                         StringRef* delimeter) {
        if (is_null || delimeter->size_ == 0) {
            return state;
        }
        auto list = state->GetListV();
        if (delimeter->size_ == 1) {
            char d = delimeter->data_[0];
            const char* begin = str->data_;
            const char* end = str->data_ + str->size_;
            const char* cur = begin;
            while (cur < end) {
                if (*cur == d) {
                    list->Add(std::string(begin, cur - begin));
                    begin = cur + 1;
                }
                ++cur;
            }
            if (begin == cur) {
                list->Add("");
            } else {
                list->Add(std::string(begin, cur - begin));
            }
        } else {
            // fallback impl with boost regex
            if (!state->IsDelimeterInitialized()) {
                state->InitDelimeter(delimeter->ToString());
                state->SetDelimeterInitialized();
            }
            std::vector<std::string> parts;
            boost::split_regex(parts, str->ToString(), state->GetDelimeter());
            for (auto& part : parts) {
                list->Add(part);
            }
        }
        return state;
    }

    static void SingleSplit(StringRef* str, bool is_null, StringRef* delimeter,
                            ListRef<StringRef>* output) {
        auto list = InitList();
        UpdateSplit(list, str, is_null, delimeter);
        output->list = reinterpret_cast<int8_t*>(list->GetListV());
    }

    static StringSplitState* UpdateSplitByKey(StringSplitState* state,
                                              StringRef* str, bool is_null,
                                              StringRef* delimeter,
                                              StringRef* kv_delimeter) {
        if (is_null || delimeter->size_ == 0 || kv_delimeter->size_ == 0) {
            return state;
        }
        auto list = state->GetListV();
        if (delimeter->size_ == 1 && kv_delimeter->size_ == 1) {
            char d1 = delimeter->data_[0];
            char d2 = kv_delimeter->data_[0];
            const char* begin = str->data_;
            const char* end = str->data_ + str->size_;
            const char* cur = begin;
            bool part_found = false;
            while (cur < end) {
                if (*cur == d1) {
                    part_found = false;
                    begin = cur + 1;
                } else if (*cur == d2 && !part_found) {
                    list->Add(std::string(begin, cur - begin));
                    part_found = true;
                }
                ++cur;
            }
        } else {
            // fallback impl with boost regex
            if (!state->IsDelimeterInitialized()) {
                state->InitDelimeter(delimeter->ToString());
                state->InitKVDelimeter(kv_delimeter->ToString());
                state->SetDelimeterInitialized();
            }
            std::vector<std::string> parts;
            boost::split_regex(parts, str->ToString(), state->GetDelimeter());
            for (auto& part : parts) {
                std::vector<std::string> sub_parts;
                boost::split_regex(sub_parts, part, state->GetKVDelimeter());
                if (sub_parts.size() >= 2) {
                    list->Add(sub_parts[0]);
                }
            }
        }
        return state;
    }

    static void SingleSplitByKey(StringRef* str, bool is_null,
                                 StringRef* delimeter, StringRef* kv_delimeter,
                                 ListRef<StringRef>* output) {
        auto list = InitList();
        UpdateSplitByKey(list, str, is_null, delimeter, kv_delimeter);
        output->list = reinterpret_cast<int8_t*>(list->GetListV());
    }

    static StringSplitState* UpdateSplitByValue(StringSplitState* state,
                                                StringRef* str, bool is_null,
                                                StringRef* delimeter,
                                                StringRef* kv_delimeter) {
        if (is_null || delimeter->size_ == 0 || kv_delimeter->size_ == 0) {
            return state;
        }
        auto list = state->GetListV();
        if (delimeter->size_ == 1 && kv_delimeter->size_ == 1) {
            char d1 = delimeter->data_[0];
            char d2 = kv_delimeter->data_[0];
            const char* begin = str->data_;
            const char* end = str->data_ + str->size_;
            const char* cur = begin;
            int cur_parts = 0;
            while (cur < end) {
                if (*cur == d1) {
                    if (cur_parts == 1) {
                        list->Add(std::string(begin, cur - begin));
                    }
                    cur_parts = 0;
                } else if (*cur == d2) {
                    ++cur_parts;
                    if (cur_parts == 1) {
                        begin = cur + 1;
                    } else if (cur_parts == 2) {
                        list->Add(std::string(begin, cur - begin));
                    }
                }
                ++cur;
            }
            if (cur_parts == 1) {
                if (cur == begin) {
                    list->Add("");
                } else {
                    list->Add(std::string(begin, cur - begin));
                }
            }
        } else {
            // fallback impl with boost regex
            if (!state->IsDelimeterInitialized()) {
                state->InitDelimeter(delimeter->ToString());
                state->InitKVDelimeter(kv_delimeter->ToString());
                state->SetDelimeterInitialized();
            }
            std::vector<std::string> parts;
            boost::split_regex(parts, str->ToString(), state->GetDelimeter());
            for (auto& part : parts) {
                std::vector<std::string> sub_parts;
                boost::split_regex(sub_parts, part, state->GetKVDelimeter());
                if (sub_parts.size() >= 2) {
                    list->Add(sub_parts[1]);
                }
            }
        }
        return state;
    }

    static void SingleSplitByValue(StringRef* str, bool is_null,
                                   StringRef* delimeter,
                                   StringRef* kv_delimeter,
                                   ListRef<StringRef>* output) {
        auto list = InitList();
        UpdateSplitByValue(list, str, is_null, delimeter, kv_delimeter);
        output->list = reinterpret_cast<int8_t*>(list->GetListV());
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
        if (buf == nullptr) {
            output->size_ = 0;
            output->data_ = "";
            return;
        }
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

    static int32_t ListSize(hybridse::codec::ListRef<StringRef>* list) {
        auto list_v = reinterpret_cast<hybridse::codec::ListV<StringRef> *>(list->list);
        return list_v->GetCount();
    }
};

template <typename K>
struct FZTop1Ratio {
    using ContainerT = udf::container::BoundedGroupByDict<K, int64_t, int64_t>;
    using InputK = typename ContainerT::InputK;

    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        std::string suffix =
            ".opaque_dict_" + DataTypeTrait<K>::to_string() + "_";
        helper.doc(helper.GetDoc())
            .templates<double, Opaque<ContainerT>, Nullable<K>>()
            .init("top1_ratio_init" + suffix, ContainerT::Init)
            .update("top1_ratio_update" + suffix, Update)
            .output("top1_ratio_output" + suffix, Output);
    }

    static ContainerT* Update(ContainerT* ptr, InputK key, bool is_key_null) {
        if (is_key_null) {
            return ptr;
        }
        auto& map = ptr->map();
        auto stored_key = ContainerT::to_stored_key(key);
        auto iter = map.find(stored_key);
        if (iter == map.end()) {
            map.insert(iter, {stored_key, 1});
        } else {
            auto& single = iter->second;
            single += 1;
        }
        return ptr;
    }

    static double Output(ContainerT* ptr) {
        auto& map = ptr->map();
        if (map.empty()) {
            return 0;
        }
        int max = 0;
        int size = 0;
        for (auto iter = map.begin(); iter != map.end(); ++iter) {
            size += iter->second;
            if (iter->second > max) {
                max = iter->second;
            }
        }
        double maxRatio = static_cast<double>(max) / size;
        ContainerT::Destroy(ptr);
        return maxRatio;
    }
};

template <typename K>
struct FZTopNFrequency {
    static const size_t MAXIMUM_TOPN = 1024;

    // we need store top_n config in state
    class TopNContainer
        : public udf::container::BoundedGroupByDict<K, int64_t> {
     public:
        static void Init(TopNContainer* addr) { new (addr) TopNContainer(); }
        static void Destroy(TopNContainer* ptr) { ptr->~TopNContainer(); }
        // record top n config
        size_t top_n_ = 0;
    };

    using InputK = typename TopNContainer::InputK;

    void operator()(UdafRegistryHelper& helper) {  // NOLINT
        std::string suffix =
            ".opaque_dict_" + DataTypeTrait<K>::to_string() + "_";
        helper.doc(helper.GetDoc())
            .templates<StringRef, Opaque<TopNContainer>, Nullable<K>, int32_t>()
            .init("topn_frequency_init" + suffix, TopNContainer::Init)
            .update("topn_frequency_update" + suffix, Update)
            .output("topn_frequency_output" + suffix, Output);
    }

    static TopNContainer* Update(TopNContainer* ptr, InputK key,
                                 bool is_key_null, int32_t top_n) {
        auto& map = ptr->map();
        ptr->top_n_ = top_n;
        if (is_key_null) {
            return ptr;
        }
        auto stored_key = TopNContainer::to_stored_key(key);
        auto iter = map.find(stored_key);
        if (iter == map.end()) {
            map.insert(iter, {stored_key, 1});
        } else {
            auto& single = iter->second;
            single += 1;
        }
        return ptr;
    }

    static void Output(TopNContainer* ptr, codec::StringRef* output) {
        if (ptr->top_n_ == 0) {
            output->data_ = "";
            output->size_ = 0;
            return;
        }
        size_t top_n = ptr->top_n_ < MAXIMUM_TOPN ? ptr->top_n_ : MAXIMUM_TOPN;
        auto& map = ptr->map();
        using StorageK = typename container::ContainerStorageTypeTrait<K>::type;
        using Entry = std::pair<StorageK, size_t>;
        auto cmp = [](Entry x, Entry y) {
            if (x.second < y.second) {
                return true;
            } else if (x.second == y.second) {
                return x.first > y.first;
            } else {
                return false;
            }
        };
        std::priority_queue<Entry, std::vector<Entry>, decltype(cmp)> queue(
            cmp);
        for (auto iter = map.begin(); iter != map.end(); ++iter) {
            queue.push({iter->first, iter->second});
        }
        std::vector<StorageK> keys;
        for (size_t i = 0; i < top_n; ++i) {
            if (queue.empty()) {
                break;
            }
            keys.emplace_back(queue.top().first);
            queue.pop();
        }

        // estimate output length
        uint32_t str_len = 0;
        for (size_t i = 0; i < top_n; ++i) {
            if (i < keys.size()) {
                str_len += v1::to_string_len(keys[i]) + 1;  // "k,"
            } else {
                str_len += 5;
            }
        }

        // allocate string buffer
        char* buffer = udf::v1::AllocManagedStringBuf(str_len);
        if (buffer == nullptr) {
            output->size_ = 0;
            output->data_ = "";
            return;
        }

        // fill string buffer
        char* cur = buffer;
        uint32_t remain_space = str_len;
        for (size_t i = 0; i < top_n; ++i) {
            uint32_t key_len;
            if (i < keys.size()) {
                key_len = v1::format_string(keys[i], cur, remain_space);
            } else {
                key_len = 4;
                snprintf(cur, 5, "NULL");  // NOLINT
            }
            cur += key_len;
            *(cur++) = ',';
            remain_space -= key_len + 1;
        }
        // must leave one '\0' for string format impl
        *(buffer + str_len - 1) = '\0';
        output->data_ = buffer;
        output->size_ = str_len - 1;
        TopNContainer::Destroy(ptr);
    }
};

void DefaultUdfLibrary::InitFeatureZero() {
    RegisterUdaf("window_split")
        .templates<ListRef<StringRef>, Opaque<StringSplitState>,
                   Nullable<StringRef>, StringRef>()
        .init("window_split_init", FZStringOpsDef::InitList)
        .update("window_split_update", FZStringOpsDef::UpdateSplit)
        .output("window_split_output", FZStringOpsDef::OutputList)
        .doc(R"(
            @brief For each string value from specified
            column of window, split by delimeter and add segment
            to output list. Null values are skipped.

            @since 0.1.0)");

    RegisterExternal("split")
        .returns<ListRef<StringRef>>()
        .return_by_arg(true)
        .args<Nullable<StringRef>, StringRef>(
            reinterpret_cast<void*>(&FZStringOpsDef::SingleSplit))
        .doc(R"(
            @brief Split string to list by delimeter. Null values are skipped.

            @param input Input string
            @param delimeter Delimeter of string

            Example:

            @code{.sql}
            select `join`(split("k1:1, k2:2", ",", ":"), " ") as out;
            -- output "k1:1 k2:2"
            @endcode

            @since 0.1.0)");

    RegisterUdaf("window_split_by_key")
        .templates<ListRef<StringRef>, Opaque<StringSplitState>,
                   Nullable<StringRef>, StringRef, StringRef>()
        .init("window_split_by_key_init", FZStringOpsDef::InitList)
        .update("window_split_by_key_update",
                FZStringOpsDef::UpdateSplitByKey)
        .output("window_split_by_key_output", FZStringOpsDef::OutputList)
        .doc(R"(
            @brief For each string value from specified
            column of window, split by delimeter and then split each segment 
            as kv pair, then add each key to output list. Null and 
            illegal segments are skipped.

            @since 0.1.0)");

    // single line version
    RegisterExternal("split_by_key")
        .returns<ListRef<StringRef>>()
        .return_by_arg(true)
        .args<Nullable<StringRef>, StringRef, StringRef>(
            reinterpret_cast<void*>(FZStringOpsDef::SingleSplitByKey))
        .doc(R"(
            @brief Split string by delimeter and split each segment as kv pair, then add each 
            key to output list. Null or illegal segments are skipped.

            @param input Input string
            @param delimeter Delimeter of string
            @param kv_delimeter Delimeter of kv pair

            Example:

            @code{.sql}
            select `join`(split_by_key("k1:1, k2:2", ",", ":"), " ") as out;
            -- output "k1 k2"
            @endcode

            @since 0.1.0)");

    RegisterUdaf("window_split_by_value")
        .templates<ListRef<StringRef>, Opaque<StringSplitState>,
                   Nullable<StringRef>, StringRef, StringRef>()
        .init("window_split_by_value_init", FZStringOpsDef::InitList)
        .update("window_split_by_value_update",
                FZStringOpsDef::UpdateSplitByValue)
        .output("window_split_by_value_output", FZStringOpsDef::OutputList)
        .doc(R"(
            @brief For each string value from specified
            column of window, split by delimeter and then split each segment 
            as kv pair, then add each value to output list. Null and 
            illegal segments are skipped.

            @since 0.1.0)");

    // single line version
    RegisterExternal("split_by_value")
        .returns<ListRef<StringRef>>()
        .return_by_arg(true)
        .args<Nullable<StringRef>, StringRef, StringRef>(
            reinterpret_cast<void*>(FZStringOpsDef::SingleSplitByValue))
        .doc(R"(
            @brief Split string by delimeter and split each segment as kv pair, then add each
            value to output list. Null or illegal segments are skipped.

            @param input Input string
            @param delimeter Delimeter of string
            @param kv_delimeter Delimeter of kv pair

            Example:

            @code{.sql}
            select `join`(split_by_value("k1:1, k2:2", ",", ":"), " ") as out;
            -- output "1 2"
            @endcode

            @since 0.1.0)");

    RegisterExternal("join")
        .doc(R"(
            @brief For each string value from specified
            column of window, join by delimeter. Null values are skipped.

            @param input String expression to join
            @param delimeter Join delimeter

            Example:

            @code{.sql}
                select fz_join(fz_split("k1:v1,k2:v2", ","), " ");
                --  "k1:v1 k2:v2"
            @endcode
            @since 0.1.0
        )")
        .list_argument_at(0)
        .args<ListRef<StringRef>, StringRef>(FZStringOpsDef::StringJoin);

    RegisterUdafTemplate<FZTop1Ratio>("top1_ratio")
        .doc(R"(@brief Compute the top1 key's ratio

        @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp,
                 StringRef>();

    RegisterUdafTemplate<FZTopNFrequency>("topn_frequency")
        .doc(R"(@brief Return the topN keys sorted by their frequency

        @since 0.1.0)")
        .args_in<int16_t, int32_t, int64_t, float, double, Date, Timestamp,
                 StringRef>();

    RegisterExternal("size")
        .list_argument_at(0)
        .args<ListRef<StringRef>>(reinterpret_cast<int32_t (*)(ListRef<StringRef>*)>(FZStringOpsDef::ListSize))
        .returns<int32_t>()
        .doc(R"(
            @brief Get the size of a List (e.g., result of split)

            Example:

            @code{.sql}
                select size(split("a b c", " "));
                -- output 3

            @endcode
            @since 0.7.0)");

    RegisterAlias("fz_window_split", "window_split");
    RegisterAlias("fz_split", "split");
    RegisterAlias("fz_split_by_key", "split_by_key");
    RegisterAlias("fz_split_by_value", "split_by_value");
    RegisterAlias("fz_window_split_by_key", "window_split_by_key");
    RegisterAlias("fz_window_split_by_value", "window_split_by_value");
    RegisterAlias("fz_join", "join");
    RegisterAlias("fz_top1_ratio", "top1_ratio");
    RegisterAlias("fz_topn_frequency", "topn_frequency");
}

}  // namespace udf
}  // namespace hybridse
