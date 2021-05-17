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

#include "apiserver/json_helper.h"

#include <stack>

#include "json2pb/rapidjson.h"  // rapidjson's DOM-style API

namespace fedb {
namespace http {

using butil::rapidjson::Document;
using butil::rapidjson::SizeType;
using butil::rapidjson::StringBuffer;
using butil::rapidjson::Value;
using butil::rapidjson::Writer;

struct JsonReaderStackItem {
    enum State {
        BeforeStart,  //!< An object/array is in the stack but it is not yet called by StartObject()/StartArray().
        Started,      //!< An object/array is called by StartObject()/StartArray().
        Closed        //!< An array is closed after read all element, but before EndArray().
    };

    JsonReaderStackItem(const Value* value, State state) : value(value), state(state), index() {}

    const Value* value;
    State state;
    SizeType index;  // For array iteration
};

typedef std::stack<JsonReaderStackItem> JsonReaderStack;

#define DOCUMENT reinterpret_cast<Document*>(mDocument_)
#define STACK (reinterpret_cast<JsonReaderStack*>(mStack_))
#define TOP (STACK->top())
#define CURRENT (*TOP.value)

JsonReader::JsonReader(const char* json) : mDocument_(), mStack_(), mError_(false) {
    mDocument_ = new Document;
    DOCUMENT->Parse(json);
    if (DOCUMENT->HasParseError()) {
        mError_ = true;
    } else {
        mStack_ = new JsonReaderStack;
        STACK->push(JsonReaderStackItem(DOCUMENT, JsonReaderStackItem::BeforeStart));
    }
}

JsonReader::~JsonReader() {
    delete DOCUMENT;
    delete STACK;
}

// Archive concept
JsonReader& JsonReader::StartObject() {
    if (!mError_) {
        if (CURRENT.IsObject() && TOP.state == JsonReaderStackItem::BeforeStart) {
            TOP.state = JsonReaderStackItem::Started;
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::EndObject() {
    if (!mError_) {
        if (CURRENT.IsObject() && TOP.state == JsonReaderStackItem::Started) {
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::Member(const char* name) {
    if (!mError_) {
        if (CURRENT.IsObject() && TOP.state == JsonReaderStackItem::Started) {
            Value::ConstMemberIterator memberItr = CURRENT.FindMember(name);
            if (memberItr != CURRENT.MemberEnd()) {
                STACK->push(JsonReaderStackItem(&memberItr->value, JsonReaderStackItem::BeforeStart));
            } else {
                mError_ = true;
            }
        } else {
            mError_ = true;
        }
    }
    return *this;
}

bool JsonReader::HasMember(const char* name) const {
    if (!mError_ && CURRENT.IsObject() && TOP.state == JsonReaderStackItem::Started) return CURRENT.HasMember(name);
    return false;
}

JsonReader& JsonReader::StartArray(size_t* size) {
    if (!mError_) {
        if (CURRENT.IsArray() && TOP.state == JsonReaderStackItem::BeforeStart) {
            TOP.state = JsonReaderStackItem::Started;
            if (size) *size = CURRENT.Size();

            if (!CURRENT.Empty()) {
                const Value* value = &CURRENT[TOP.index];
                STACK->push(JsonReaderStackItem(value, JsonReaderStackItem::BeforeStart));
            } else {
                TOP.state = JsonReaderStackItem::Closed;
            }
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::EndArray() {
    if (!mError_) {
        if (CURRENT.IsArray() && TOP.state == JsonReaderStackItem::Closed) {
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::operator&(bool& b) {  // NOLINT
    if (!mError_) {
        if (CURRENT.IsBool()) {
            b = CURRENT.GetBool();
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::operator&(unsigned& u) {  // NOLINT
    if (!mError_) {
        if (CURRENT.IsUint()) {
            u = CURRENT.GetUint();
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::operator&(int& i) {  // NOLINT
    if (!mError_) {
        if (CURRENT.IsInt()) {
            i = CURRENT.GetInt();
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::operator&(double& d) {  // NOLINT
    if (!mError_) {
        if (CURRENT.IsNumber()) {
            d = CURRENT.GetDouble();
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::operator&(std::string& s) {  // NOLINT
    if (!mError_) {
        if (CURRENT.IsString()) {
            s = CURRENT.GetString();
            Next();
        } else {
            mError_ = true;
        }
    }
    return *this;
}

JsonReader& JsonReader::SetNull() {
    // This function is for JsonWriter only.
    mError_ = true;
    return *this;
}

void JsonReader::Next() {
    if (!mError_) {
        assert(!STACK->empty());
        STACK->pop();

        if (!STACK->empty() && CURRENT.IsArray()) {
            if (TOP.state == JsonReaderStackItem::Started) {  // Otherwise means reading array item pass end
                if (TOP.index < CURRENT.Size() - 1) {
                    const Value* value = &CURRENT[++TOP.index];
                    STACK->push(JsonReaderStackItem(value, JsonReaderStackItem::BeforeStart));
                } else {
                    TOP.state = JsonReaderStackItem::Closed;
                }
            } else {
                mError_ = true;
            }
        }
    }
}

#undef DOCUMENT
#undef STACK
#undef TOP
#undef CURRENT

////////////////////////////////////////////////////////////////////////////////
// JsonWriter
// We use Writer instead of PrettyWriter for performance reasons
#define WRITER (reinterpret_cast<Writer<StringBuffer>*>(mWriter_))
#define STREAM (reinterpret_cast<StringBuffer*>(mStream_))

JsonWriter::JsonWriter() {  // : mWriter_(), mStream_()
    mStream_ = new StringBuffer;
    mWriter_ = new Writer<StringBuffer>(*STREAM);
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
