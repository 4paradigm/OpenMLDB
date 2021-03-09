/*
 * Copyright 2021 4Paradigm
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

#include <gflags/gflags.h>
// cluster config
DEFINE_string(fesql_endpoint, "",
        "config the ip and port that fesql serves for");
DEFINE_int32(fesql_port, 0, "config the port that fesql serves for");
DEFINE_int32(fesql_thread_pool_size, 8,
        "config the thread pool for dbms and tablet");
DEFINE_string(tablet_endpoint, "",
              "config the ip and port that fesql tablet for");
// for tablet
DEFINE_string(dbms_endpoint, "", "config the ip and port that fesql dbms for");
DEFINE_bool(enable_keep_alive, true, "config if tablet keep alive with dbms");
