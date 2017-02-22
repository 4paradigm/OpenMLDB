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

#include <stdio.h>
#include <signal.h>
#include <unistd.h>
#include <gflags/gflags.h>
#include <sofa/pbrpc/pbrpc.h>
#include "storage/memtable.h"
#include "storage/dbformat.h"
#include "util/comparator.h"
#include "version.h"
#include "dbms/baidu_common.h"
#include "dbms/rtidb_tablet_server_impl.h"

DEFINE_string(ts_endpoint, "127.0.0.1:9527", "config the ip and port that ts serves for");
DEFINE_int32(ts_partition_count, 8, "config the partition count of ts");


static volatile bool s_quit = false;
static void SignalIntHandler(int /*sig*/){
  s_quit = true;
}

int main(int argc, char* args[]) {
  //::baidu::common::SetLogFile("./rtidb_ts.log", true);
  //::baidu::common::SetLogSize(1024);//1024M
  ::baidu::common::SetLogLevel(INFO);
  ::google::ParseCommandLineFlags(&argc, &args, true);
  sofa::pbrpc::RpcServerOptions options;
  sofa::pbrpc::RpcServer rpc_server(options);  
  rtidb::RtiDBTabletServerImpl* ts = new rtidb::RtiDBTabletServerImpl(FLAGS_ts_partition_count);
  ts->Init();
  if (!rpc_server.RegisterService(ts)) {
    LOG(WARNING, "fail to start ts service");
    exit(1);
  }
  if (!rpc_server.Start(FLAGS_ts_endpoint)) {
    LOG(WARNING, "fail to listen endpoint %s", FLAGS_ts_endpoint.c_str());
    exit(1);
  }
  LOG(INFO, "start ts with endpoint %s successfully", FLAGS_ts_endpoint.c_str());
  signal(SIGINT, SignalIntHandler);
  signal(SIGTERM, SignalIntHandler);
  while (!s_quit) {
    sleep(1);
  }
  return 0;
}

/* vim: set expandtab ts=2 sw=2 sts=2 tw=100: */
