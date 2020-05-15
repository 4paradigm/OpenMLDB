//
// glog_wapper.cc
// Copyright 2017 4paradigm.com

#ifndef SRC_BASE_GLOG_WAPPER_H_
#define SRC_BASE_GLOG_WAPPER_H_

#include <cstdarg>
#include <iostream>
#include <string>
#include <boost/format.hpp>
#include "glog/logging.h"

using google::ERROR;
using google::FATAL;
using google::INFO;
using google::WARNING;

namespace rtidb {
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
    ::google::SetLogDestination(::google::INFO, info_log_path.c_str());
    ::google::SetLogDestination(::google::WARNING, warning_log_path.c_str());
}

}  // namespace base
}  // namespace rtidb

using ::rtidb::base::DEBUG;

#define PDLOG(level, fmt, args...)      \
    COMPACT_GOOGLE_LOG_##level.stream() \
        << ::rtidb::base::FormatArgs(fmt, ##args)

#define DEBUGLOG(fmt, args...)                             \
    {                                                      \
        if (::rtidb::base::log_level == -1)                \
            COMPACT_GOOGLE_LOG_INFO.stream()               \
                << ::rtidb::base::FormatArgs(fmt, ##args); \
    }                                                      \
    while (0)

#endif  // SRC_BASE_GLOG_WAPPER_H_
