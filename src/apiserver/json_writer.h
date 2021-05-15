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

#ifndef SRC_APISERVER_JSON_WRITER_H_
#define SRC_APISERVER_JSON_WRITER_H_

#include <cstddef>
#include <string>

namespace fedb {
namespace http {

class JsonWriter {
 public:
    JsonWriter();
    ~JsonWriter();

    /// Obtains the serialized JSON string.
    const char* GetString() const;

    // Archive concept

    operator bool() const { return true; }

    JsonWriter& StartObject();
    JsonWriter& Member(const char* name);
    bool HasMember(const char* name) const;
    JsonWriter& EndObject();

    JsonWriter& StartArray(size_t* size = nullptr);  // JsonReader needs size
    JsonWriter& EndArray();

    JsonWriter& operator&(const bool& b);
    JsonWriter& operator&(const unsigned& u);
    JsonWriter& operator&(const int& i);
    JsonWriter& operator&(const int64_t& i);
    JsonWriter& operator&(const double& d);
    JsonWriter& operator&(const std::string& s);
    JsonWriter& SetNull();

    static const bool IsReader = false;
    static const bool IsWriter = !IsReader;

    JsonWriter(const JsonWriter&) = delete;
    JsonWriter& operator=(const JsonWriter&) = delete;

 private:
    // PIMPL idiom
    void* mWriter;  ///< JSON writer.
    void* mStream;  ///< Stream buffer.
};

}  // namespace http
}  // namespace fedb

#endif  // SRC_APISERVER_JSON_WRITER_H_
