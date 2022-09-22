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
// For compatibility, use openmldb_log_dir instead of glog log_dir
// If we want write log to stdout, set it empty
inline void UnprotectedSetupGlog() {
    // client: role == "" or "sql_client", use glog_dir
    // server: others, use openmldb_log_dir
    std::string log_dir = FLAGS_openmldb_log_dir;
    if (FLAGS_role.empty()) {
        // give sdk or standalone client a name
        FLAGS_role = "client";
        log_dir = FLAGS_glog_dir;
    } else if (FLAGS_role == "sql_client") {
        log_dir = FLAGS_glog_dir;
    }

    if (!log_dir.empty()) {
        boost::filesystem::create_directories(log_dir);
        std::string path = log_dir + "/" + FLAGS_role;
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
// and it's better to set FLAGS_minloglevel before it
inline void SetupGLog() {
    static std::once_flag oc;
    std::call_once(oc, [] { UnprotectedSetupGlog(); });
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
