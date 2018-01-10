//
// ns_client.h
// Copyright (C) 2017 4paradigm.com
// Author vagrant
// Date 2017-09-18
//


#ifndef RTIDB_NS_CLIENT_H
#define RTIDB_NS_CLIENT_H

#include <stdint.h>
#include <vector>
#include "rpc/rpc_client.h"
#include "proto/name_server.pb.h"

namespace rtidb {
namespace client {

struct TabletInfo {
    std::string endpoint;
    std::string state;
    uint64_t age;
};

class NsClient {

public:
    NsClient(const std::string& endpoint);

    int Init();

    bool ShowTablet(std::vector<TabletInfo>& tablets, std::string& msg);
    
    bool ShowTable(const std::string& name, std::vector<::rtidb::nameserver::TableInfo>& tables, std::string& msg);

    bool MakeSnapshot(const std::string& name, uint32_t pid, std::string& msg);

    bool ShowOPStatus(::rtidb::nameserver::ShowOPStatusResponse& response, std::string& msg);

    bool CreateTable(const ::rtidb::nameserver::TableInfo& table_info, std::string& msg);

    bool DropTable(const std::string& name, std::string& msg);

    bool AddReplica(const std::string& name, uint32_t pid, const std::string& endpoint, std::string& msg);

    bool DelReplica(const std::string& name, uint32_t pid, const std::string& endpoint, std::string& msg);
	
private:
    std::string endpoint_;
    ::rtidb::RpcClient<::rtidb::nameserver::NameServer_Stub> client_;
};

}
}

#endif /* !RTIDB_NS_CLIENT_H */
