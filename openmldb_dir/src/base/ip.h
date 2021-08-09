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

#pragma once

#include <arpa/inet.h>
#include <netdb.h>
#include <unistd.h>

#include <string>

namespace openmldb {
namespace base {

bool GetLocalIp(std::string* ip) {
    if (ip == nullptr) {
        return false;
    }
    char name[256];
    gethostname(name, sizeof(name));
    struct hostent* host = gethostbyname(name);
    char ip_str[32];
    const char* ret = inet_ntop(host->h_addrtype, host->h_addr_list[0], ip_str, sizeof(ip_str));
    if (ret == NULL) {
        return false;
    }
    *ip = ip_str;
    return true;
}

}  // namespace base
}  // namespace openmldb
