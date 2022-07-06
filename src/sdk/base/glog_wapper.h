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

#ifndef SRC_BASE_GLOG_WAPPER_H_
#define SRC_BASE_GLOG_WAPPER_H_

#include <cstdarg>
#include <iostream>
#include <string>

#include "glog/logging.h"
#include <boost/format.hpp>

using google::ERROR;
using google::FATAL;
using google::INFO;
using google::WARNING;

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

inline void SetLogFile(std::string path) {
    ::google::InitGoogleLogging(path.c_str());
    std::string info_log_path = path + ".info.log.";
    std::string warning_log_path = path + ".warning.log.";
    FLAGS_logbufsecs = 0;
    ::google::SetLogDestination(::google::INFO, info_log_path.c_str());
    ::google::SetLogDestination(::google::WARNING, warning_log_path.c_str());
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

#endif  // SRC_BASE_GLOG_WAPPER_H_
