//
// tablet_client_wrapper.cc
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-04-25
//

#include "client/tablet_client.h"

#include "base/codec.h"

using namespace rtidb::client;
using namespace rtidb::base;

extern "C" {

TabletClient* NewClient(const char* endpoint) {
    std::string ep(endpoint);
    TabletClient* client = new TabletClient(ep);
    client->Init();
    return client;
}

void FreeClient(TabletClient* client) {
    delete client;

}

bool CreateTable(TabletClient* client, 
                 const char* name, int tid,
                 int pid, int ttl) {
    std::string n(name);
    return client->CreateTable(n, tid, pid, ttl, true, std::vector<std::string>(), 
                    ::rtidb::api::TTLType::kAbsoluteTime, 16);
}

bool Put(TabletClient* client,
        uint32_t tid, uint32_t pid,
        const char* pk,
        uint64_t time,
        const char* value) {
    return client->Put(tid, pid, pk, time, value);
}

KvIterator* Scan(TabletClient* client,
        uint32_t tid, uint32_t pid,
        const char* pk,
        uint64_t st,
        uint64_t et) {
    return client->Scan(tid, pid, pk, st, et, false);
}

bool DropTable(TabletClient* client, uint32_t tid, uint32_t pid) {
    return client->DropTable(tid, pid);
}

bool IteratorValid(KvIterator* it) {
    return it->Valid();
}

void IteratorNext(KvIterator* it) {
    it->Next();
}

void IteratorGetValue(KvIterator* it, uint32_t* len, char** val) {
    Slice& value = it->GetValue();
    char* buf = static_cast<char*>(malloc(value.size()));
    memcpy(buf, value.data(), value.size());
    *val = buf;
    *len = value.size();
}

void IteratorGetKey(KvIterator* it, uint64_t* key) {
    *key = it->GetKey();
}

void IteratorFree(KvIterator* it) {
    delete it;
}

void FreeString(const char* str) {
    delete str;
}

}


