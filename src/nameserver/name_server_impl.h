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
#include <map>
#include <atomic>
#include "zk/zk_client.h"

namespace rtidb {
namespace nameserver {

using ::google::protobuf::RpcController;
using ::google::protobuf::Closure;
using ::rtidb::zk::ZkClient;

class NameServerImpl : public NameServer {
public:
    NameServerImpl();
    ~NameServerImpl();
    bool Init();
    void SetOnline();
    NameServerImpl(const NameServerImpl&) = delete;
    NameServerImpl& operator= (const NameServerImpl&) = delete; 
    bool WebService(const sofa::pbrpc::HTTPRequest& request,
                sofa::pbrpc::HTTPResponse& response);

    void CreateTable(RpcController* controller,
        const CreateTableRequest* request,
        GeneralResponse* response, 
        Closure* done);
    int CreateTable(const ::rtidb::nameserver::TableMeta& table_meta, uint32_t tid,
                bool is_leader, std::map<uint32_t, std::vector<std::string> >& endpoint_vec);
    void CheckZkClient();

private:    
    ::baidu::common::Mutex mu_;
    std::map<std::string, std::shared_ptr<::rtidb::client::TabletClient> > tablet_client_;
    std::map<std::string, ::rtidb::nameserver::TableMeta> table_info_;
    ZkClient* zk_client_;
    ::baidu::common::ThreadPool thread_pool_;
    std::string zk_table_path_;
    std::string zk_data_path_;
    std::string zk_table_index_node_;
    std::atomic<bool> running_;

};

}
}
#endif
