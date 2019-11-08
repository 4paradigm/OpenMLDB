/*
 * dbms_server_impl.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
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

#ifndef FESQL_DBMS_SERVER_IMPL_H_
#define FESQL_DBMS_SERVER_IMPL_H_

#include <map>
#include <mutex>
#include "proto/dbms.pb.h"
#include "proto/type.pb.h"

namespace fesql {
namespace dbms {

using ::google::protobuf::Closure;
using ::google::protobuf::RpcController;

typedef std::map<std::string, ::fesql::type::Group> Groups;
typedef std::map<std::string, ::fesql::type::TableDef> Tables;

class DBMSServerImpl : public DBMSServer {
 public:
  DBMSServerImpl();
  ~DBMSServerImpl();

  void AddGroup(RpcController* ctr, const AddGroupRequest* request,
                AddGroupResponse* response, Closure* done);

  void AddTable(RpcController* ctr, const AddTableRequest* request,
                AddTableResponse* response, Closure* done);

  void ShowSchema(RpcController* controller, const ShowSchemaRequest* request,
                  ShowSchemaResponse* response, Closure* done);

 private:
  std::mutex mu_;
  Groups groups_;
  Tables tables_;
};

}  // namespace dbms
}  // namespace fesql
#endif /* !FESQL_DBMS_SERVER_IMPL_H_ */
