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

#ifndef SRC_BASE_GLOG_WRAPPER_H_
#define SRC_BASE_GLOG_WRAPPER_H_

#include <cstdarg>
#include <iostream>
#include <mutex>
#include <string>

#include "boost/filesystem.hpp"
#include "boost/format.hpp"
#include "gflags/gflags.h"
#include "glog/logging.h"

using google::ERROR;
using google::FATAL;
using google::INFO;
using google::WARNING;

DECLARE_string(openmldb_log_dir);
DECLARE_string(role);
DECLARE_string(glog_dir);
DECLARE_int32(glog_level);

namespace openmldb {
namespace base {

const int DEBUG = -1;
static int log_level = INFO;

template <typename... Arguments>
inline std::string FormatArgs(const char* fmt, const Arguments&... args) {
    boost::format f(fmt);
    std::initializer_list<char>{(static_cast<void>(f % args), char{})...};

    return boost::str(f);
}

inline void SetLogLevel(int level) { log_level = level; }

// DO NOT use this func, to avoid init glog twice coredump
// For compatibility, use openmldb_log_dir instead of glog log_dir for server
// If we want write log to stderr, set it empty
inline void UnprotectedSetupGlog(bool origin_flags = false) {
    std::string role = "unknown_possibly_a_test";
    std::string log_dir;
    if (!origin_flags) {
        role = (FLAGS_role.empty() ? "client" : FLAGS_role);
        // client: role == ""(client) or "sql_client", use glog_dir
        // server: others, use openmldb_log_dir
        log_dir = FLAGS_openmldb_log_dir;
        if (role == "sql_client" || role == "client") {
            log_dir = FLAGS_glog_dir;
            FLAGS_minloglevel = FLAGS_glog_level;
        }
    } else {
        // if origin_flags, use the original FLAGS_minloglevel, FLAGS_log_dir
        // FLAGS_log_dir should be create first
        log_dir = FLAGS_log_dir;
    }

    if (log_dir.empty()) {
        // If we don't set glog dir, it'll write to /tmp. So we'd set to stderr
        FLAGS_logtostderr = true;
        ::google::InitGoogleLogging(role.c_str());
    } else {
        boost::filesystem::create_directories(log_dir);
        std::string path = log_dir + "/" + role;
        ::google::InitGoogleLogging(path.c_str());
        std::string info_log_path = path + ".info.log.";
        std::string warning_log_path = path + ".warning.log.";
        FLAGS_logbufsecs = 0;
        ::google::SetLogDestination(::google::INFO, info_log_path.c_str());
        ::google::SetLogDestination(::google::WARNING, warning_log_path.c_str());
    }
}

// This func will init glog, use once_flag to avoid init glog twice
// It'll use FLAGS_glog_dir/FLAGS_openmldb_log_dir and FLAGS_role to set log dir,
// and set FLAGS_minloglevel by FLAGS_glog_level(only for clients) in here
inline bool SetupGlog(bool origin_flags = false) {
    static std::once_flag oc;
    bool setup = false;
    std::call_once(oc, [&setup, origin_flags] {
        UnprotectedSetupGlog(origin_flags);
        setup = true;
    });
    return setup;
}

}  // namespace base
}  // namespace openmldb

using ::openmldb::base::DEBUG;

#define PDLOG(level, fmt, args...) COMPACT_GOOGLE_LOG_##level.stream() << ::openmldb::base::FormatArgs(fmt, ##args)

#define DEBUGLOG(fmt, args...)                                                             \
    {                                                                                      \
        if (::openmldb::base::log_level == -1)                                             \
            COMPACT_GOOGLE_LOG_INFO.stream() << ::openmldb::base::FormatArgs(fmt, ##args); \
    }                                                                                      \
    while (0)

#endif  // SRC_BASE_GLOG_WRAPPER_H_
