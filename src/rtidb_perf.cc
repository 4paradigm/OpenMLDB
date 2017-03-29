/*
 * rtidb.cc
 * Copyright 2017 elasticlog <elasticlog01@gmail.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <vector>

#include "flatbuffers/flatbuffers.h"
#include "proto/kvpair_generated.h"
#include "proto/kvpair.pb.h"
#include "timer.h"
#include <iostream>

void HandleFbsPerf() {
    flatbuffers::FlatBufferBuilder builder(1024);
    std::vector<flatbuffers::Offset<rtidb::fbs::KvPair>> kv_vector;
    for (int i = 0; i < 1000; i++) {
        flatbuffers::Offset<flatbuffers::String> key = builder.CreateString("000011111");
        flatbuffers::Offset<flatbuffers::String> value = builder.CreateString("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
        flatbuffers::Offset<rtidb::fbs::KvPair> kvpair = rtidb::fbs::CreateKvPair(builder, key, value);
        kv_vector.push_back(kvpair);
    }
    flatbuffers::Offset<rtidb::fbs::KvPairList> kv_list = rtidb::fbs::CreateKvPairListDirect(builder, &kv_vector);
    rtidb::fbs::FinishKvPairListBuffer(builder, kv_list);
    builder.GetBufferPointer();
}

void HandlePbPerf() {
    rtidb::pb::KvPairList list;
    for (int i = 0; i < 1000; i++) {
        rtidb::pb::KvPair* pair = list.add_pairs();
        pair->set_pk("000011111");
        pair->set_value("xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx");
    }
    std::string buffer;
    list.SerializeToString(&buffer);
}

void runCase(const std::string& name, void(*func)(), int times ) {
    int64_t consumed = ::baidu::common::timer::get_micros();
    for (int i = 0; i < times; i++) {
        func();
    } 
    consumed = ::baidu::common::timer::get_micros() - consumed;
    std::cout << "run " << name << " with " << times << " consumed:"
        << consumed / 1000 << " ms" << std::endl;
}

int main(int argc, char* args[]) {
    runCase("flatbuffer", &HandleFbsPerf, 10);
    runCase("protobuf", &HandlePbPerf, 10);
    runCase("flatbuffer", &HandleFbsPerf, 10000);
    runCase("protobuf", &HandlePbPerf, 10000);
    return 0;
} 
