//
// Copyright (C) 2020 4paradigm.com
// Author wangbao
// Date 2020-07-07
//

#pragma once

#include <unistd.h>
#include <netdb.h>
#include <arpa/inet.h>

namespace rtidb {
namespace base {

bool GetLocalIp(std::string* ip) {
    char name[256];
    gethostname(name, sizeof(name));
    struct hostent* host = gethostbyname(name);
    char ip_str[32];
    const char* ret = inet_ntop(host->h_addrtype,
            host->h_addr_list[0], ip_str, sizeof(ip_str));
    if (ret == NULL) {
        return false;
    }
    *ip = ip_str;
    return true;
}

}  // namespace base
}  // namespace rtidb
