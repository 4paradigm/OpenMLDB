//
// name_server_impl.h
// Copyright (C) 2017 4paradigm.com
// Author denglong
// Date 2017-09-05

#ifndef RTIDB_NAME_SERVER_H
#define RTIDB_NAME_SERVER_H

#include "proto/name_server.pb.h"
#include <sofa/pbrpc/pbrpc.h>
#include "client/tablet_client.h"
#include "mutex.h"

namespace rtidb {
namespace nameserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;

struct TableInfo {
    TableInfo() {

    }
    std::string name_;
    std::string endpoint_;
    uint32_t tid_;
    uint32_t pid_;
    bool is_leader_;
    uint32_t ttl_;
    uint32_t replicate_num_;
    uint32_t partition_num_;
    uint32_t seg_cnt_;
};

class NameServerImpl : public NameServer {
public:
    NameServerImpl();
    ~NameServerImpl();
    int Init();
    NameServerImpl(const NameServerImpl&) = delete;
    NameServerImpl& operator= (const NameServerImpl&) = delete; 
    bool WebService(const sofa::pbrpc::HTTPRequest& request,
                sofa::pbrpc::HTTPResponse& response);

    void CreateTable(RpcController* controller,
        const CreateTableRequest* request,
        GeneralResponse* response, 
        Closure* done);
    std::string GetMaster();

private:    
    uint32_t table_index_;
    ::baidu::common::Mutex mu_;
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient> > tablet_client_;
    std::map<std::string, std::vector<TableInfo> > table_info_;

};

}
}
#endif
