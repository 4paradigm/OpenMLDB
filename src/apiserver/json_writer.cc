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

#include "apiserver/json_writer.h"

#include "json2pb/rapidjson.h"  // rapidjson's DOM-style API

namespace fedb {
namespace http {

using butil::rapidjson::Document;
using butil::rapidjson::PrettyWriter;
using butil::rapidjson::SizeType;
using butil::rapidjson::StringBuffer;
using butil::rapidjson::Writer;

// TODO(hw): PrettyWriter is easy to read, but for transport, we should use Writer. How about template?
#define WRITER reinterpret_cast<PrettyWriter<StringBuffer>*>(mWriter)
#define STREAM reinterpret_cast<StringBuffer*>(mStream)

JsonWriter::JsonWriter() {  // : mWriter(), mStream()
    mStream = new StringBuffer;
    mWriter = new PrettyWriter<StringBuffer>(*STREAM);
}

JsonWriter::~JsonWriter() {
    delete WRITER;
    delete STREAM;
}

const char* JsonWriter::GetString() const { return STREAM->GetString(); }

JsonWriter& JsonWriter::StartObject() {
    WRITER->StartObject();
    return *this;
}

JsonWriter& JsonWriter::EndObject() {
    WRITER->EndObject();
    return *this;
}

JsonWriter& JsonWriter::Member(const char* name) {
    WRITER->String(name, static_cast<SizeType>(strlen(name)));
    return *this;
}

bool JsonWriter::HasMember(const char*) const {
    // This function is for JsonReader only.
    assert(false);
    return false;
}

JsonWriter& JsonWriter::StartArray(size_t*) {
    WRITER->StartArray();
    return *this;
}

JsonWriter& JsonWriter::EndArray() {
    WRITER->EndArray();
    return *this;
}

JsonWriter& JsonWriter::operator&(const bool& b) {
    WRITER->Bool(b);
    return *this;
}

JsonWriter& JsonWriter::operator&(const unsigned& u) {
    WRITER->AddUint(u);
    return *this;
}

JsonWriter& JsonWriter::operator&(const int& i) {
    WRITER->AddInt(i);
    return *this;
}

JsonWriter& JsonWriter::operator&(const int64_t& i) {
    WRITER->AddInt64(i);
    return *this;
}

JsonWriter& JsonWriter::operator&(const double& d) {
    WRITER->Double(d);
    return *this;
}

JsonWriter& JsonWriter::operator&(const std::string& s) {
    WRITER->String(s.c_str(), static_cast<SizeType>(s.size()));
    return *this;
}

JsonWriter& JsonWriter::SetNull() {
    WRITER->Null();
    return *this;
}
}  // namespace http
}  // namespace fedb
