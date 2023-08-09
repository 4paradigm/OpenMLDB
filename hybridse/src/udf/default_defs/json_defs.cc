/**
 * Copyright (c) 2023 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "simdjson.h"
#include "udf/default_udf_library.h"
#include "udf/udf.h"
#include "udf/udf_library.h"
#include "udf/udf_registry.h"

namespace hybridse {
namespace udf {

void json_array_length(openmldb::base::StringRef* in, int32_t* sz, bool* is_null) noexcept {
    *is_null = true;

    simdjson::ondemand::parser parser;
    simdjson::padded_string json(in->data_, in->size_);
    simdjson::ondemand::document doc;
    auto err = parser.iterate(json).get(doc);
    if (err) {
        return;
    }
    simdjson::ondemand::array arr;
    err = doc.get_array().get(arr);
    if (err) {
        return;
    }
    size_t arr_sz;
    arr.count_elements().tie(arr_sz, err);
    if (err) {
        return;
    }

    *is_null = false;
    *sz = static_cast<int32_t>(arr_sz);
}

void get_json_object(openmldb::base::StringRef* in, openmldb::base::StringRef* json_path,
                     openmldb::base::StringRef* out, bool* is_null) noexcept {
    *is_null = true;

    simdjson::ondemand::parser parser;
    simdjson::padded_string json(in->data_, in->size_);
    simdjson::ondemand::document doc;
    simdjson::error_code err = simdjson::error_code::SUCCESS;

    if (parser.iterate(json).tie(doc, err); err) {
        return;
    }

    simdjson::ondemand::value val;
    std::string_view path(json_path->data_, json_path->size_);
    if (doc.at_pointer(path).tie(val, err); err) {
        return;
    }

    simdjson::ondemand::json_type type;
    if (val.type().tie(type, err); err) {
        return;
    }
    std::string_view raw_str;
    switch (type) {
        // NOTE: JSON validation skipped for null/bool/number/array/object simplify for performance,
        // string value always checked.
        // Recheck here if u think more accurate syntax check is necessary.
        case simdjson::ondemand::json_type::null:
        case simdjson::ondemand::json_type::boolean:
        case simdjson::ondemand::json_type::number:
        case simdjson::ondemand::json_type::array:
        case simdjson::ondemand::json_type::object: {
            if (simdjson::to_json_string(val).tie(raw_str, err); err) {
                return;
            }
            break;
        }
        case simdjson::ondemand::json_type::string: {
            if (val.get_string().tie(raw_str, err); err) {
                return;
            }
            break;
        }
        default:
            return;
    }

    size_t sz = raw_str.size();
    char* buf = v1::AllocManagedStringBuf(sz);
    memcpy(buf, raw_str.data(), sz);

    out->size_ = sz;
    out->data_ = buf;
    *is_null = false;
}

void DefaultUdfLibrary::InitJsonUdfs() {
    RegisterExternal("json_array_length")
    .args<openmldb::base::StringRef>(json_array_length)
    .doc(R"(
         @brief Returns the number of elements in the outermost JSON array.

         Null returned if input is not valid JSON array string.

         @param jsonArray JSON arry in string

         Example:

         @code{.sql}
           select json_array_length('[1, 2]')
           -- 2

           SELECT json_array_length('[1,2,3,{"f1":1,"f2":[5,6]},4]');
           -- 5

           select json_array_length('[1, 2')
           -- NULL
         @endcode

         @since 0.9.0)");

    RegisterExternal("get_json_object")
    .args<openmldb::base::StringRef, openmldb::base::StringRef>(get_json_object)
    .doc(R"(
         @brief Extracts a JSON object from [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901)

         NOTE JSON string is not fully validated. Which means that the function may still returns values even though returned string does not valid for JSON.
         It's your responsibility to make sure input string is valid JSON

         @param expr A string expression contains well formed JSON
         @param path A string expression of JSON string representation from [JSON Pointer](https://datatracker.ietf.org/doc/html/rfc6901)

         Example:

         @code{.sql}
           select get_json_object('{"boo": "baz"}', "/boo")
           -- baz

           select get_json_object('{"boo": [1, 2]}', "/boo/0")
           -- 1

           select get_json_object('{"m~n": 1}', "/m~0n")
           -- 1

           select get_json_object('{"m/n": 1}', "/m~1n")
           -- 1

           select get_json_object('{"foo": {"bar": bz}}', "/foo")
           -- {"bar": bz}
           --- returns value even input JSON is not a valid JSON
         @endcode

         @since 0.9.0)");
}

}  // namespace udf
}  // namespace hybridse
