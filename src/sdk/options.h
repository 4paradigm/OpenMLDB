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

#ifndef SRC_SDK_OPTIONS_H_
#define SRC_SDK_OPTIONS_H_

#include <sstream>
#include <string>

namespace openmldb {
namespace sdk {

struct BasicRouterOptions {
    virtual ~BasicRouterOptions() = default;
    bool enable_debug = false;
    uint32_t max_sql_cache_size = 50;
    // == gflag `request_timeout` default value(no gflags here cuz swig)
    uint32_t request_timeout = 60000;
    // default 0(INFO), INFO, WARNING, ERROR, and FATAL are 0, 1, 2, and 3
    int glog_level = 0;
    // empty means to stderr
    std::string glog_dir = "";
    std::string user = "root";
    std::string password;
};

struct SQLRouterOptions : BasicRouterOptions {
    std::string zk_cluster;
    std::string zk_path;
    uint32_t zk_session_timeout = 2000;
    std::string spark_conf_path;
    uint32_t zk_log_level = 3;  // PY/JAVA SDK default info log
    std::string zk_log_file;
    std::string zk_auth_schema = "digest";
    std::string zk_cert;

    std::string to_string() {
        std::stringstream ss;
        ss << "zk options [cluster:" << zk_cluster << ", path:" << zk_path
           << ", zk_session_timeout:" << zk_session_timeout
           << ", log_level:" << zk_log_level << ", log_file:" << zk_log_file
           << ", zk_auth_schema:" << zk_auth_schema << ", zk_cert:" << zk_cert << "]";
        return ss.str();
    }
};

struct StandaloneOptions : BasicRouterOptions {
    std::string host;
    uint32_t port;
};

}  // namespace sdk
}  // namespace openmldb
#endif  // SRC_SDK_OPTIONS_H_
