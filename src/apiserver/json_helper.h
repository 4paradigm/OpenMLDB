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

#ifndef SRC_APISERVER_JSON_HELPER_H_
#define SRC_APISERVER_JSON_HELPER_H_

#include <cstddef>
#include <string>

#include "rapidjson/document.h"  // rapidjson's DOM-style API
#include "rapidjson/writer.h"

namespace openmldb {
namespace apiserver {

using rapidjson::Document;
using rapidjson::SizeType;
using rapidjson::StringBuffer;
using rapidjson::Value;
using rapidjson::Writer;

/**
\class Archiver
\brief Archiver concept
Archiver can be a reader or writer for serialization or deserialization respectively.
Usage:
    You can use "JsonReader >> obj" to read from json string, and use "JsonWriter << obj" to write to buffer which the
json writer holds.
    When you want to support read/write a new struct, implement this function below. Then you can use `>>` & `<<`.
    `template <typename Archiver>
    Archiver& operator&(Archiver& ar, NewStruct& s) {}`

Ref
https://github.com/Tencent/rapidjson/blob/915218878afb3a60f343a80451723d023273081c/example/archiver/archiver.h

 **/

/// Represents a JSON reader which implements Archiver concept.
class JsonReader {
 public:
    /// Constructor.
    /**
        \param json A non-const source json string for in-situ parsing.
        \note in-situ means the source JSON string will be modified after parsing.
        just pass document for template read flags
    */
    JsonReader(const char* json);

    /// Destructor.
    ~JsonReader();

    // Archive concept

    operator bool() const { return !error_; }

    JsonReader& StartObject();
    JsonReader& Member(const char* name);
    bool HasMember(const char* name) const;
    JsonReader& EndObject();

    JsonReader& StartArray(size_t* size = nullptr);
    JsonReader& EndArray();

    JsonReader& operator&(bool& b);         // NOLINT
    JsonReader& operator&(unsigned& u);     // NOLINT
    JsonReader& operator&(int16_t& i);      // NOLINT
    JsonReader& operator&(int& i);          // NOLINT
    JsonReader& operator&(int64_t& i);      // NOLINT
    JsonReader& operator&(float& f);        // NOLINT
    JsonReader& operator&(double& d);       // NOLINT
    JsonReader& operator&(std::string& s);  // NOLINT

    JsonReader& SetNull();

    void Next();

    static const bool IsReader = true;
    static const bool IsWriter = !IsReader;

    JsonReader& operator=(const JsonReader&) = delete;
    JsonReader(const JsonReader&) = delete;

 private:
    // PIMPL
    void* document_;  ///< DOM result of parsing.
    void* stack_;     ///< Stack for iterating the DOM
    bool error_;      ///< Whether an error has occurred.
};

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
    JsonWriter& operator&(uint64_t i);
    JsonWriter& operator&(const double& d);
    JsonWriter& operator&(const std::string& s);
    JsonWriter& SetNull();

    static const bool IsReader = false;
    static const bool IsWriter = !IsReader;

    JsonWriter(const JsonWriter&) = delete;
    JsonWriter& operator=(const JsonWriter&) = delete;

 private:
    // PIMPL idiom
    void* writer_;  ///< JSON writer.
    void* stream_;  ///< Stream buffer.
};

template <typename T>
JsonReader& operator>>(JsonReader& ar, T& s) {
    return ar & s;
}
template <typename T>
JsonWriter& operator<<(JsonWriter& ar, T& s) {
    return ar & s;
}

}  // namespace apiserver
}  // namespace openmldb

#endif  // SRC_APISERVER_JSON_HELPER_H_
