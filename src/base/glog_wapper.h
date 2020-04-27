//
// glog_wapper.cc
// Copyright 2017 4paradigm.com


#ifndef GLOG_WAPPER_H_
#define GLOG_WAPPER_H_

#include <iostream>
#include <cstdarg>
#include "glog/logging.h"
#include <boost/format.hpp>

using google::INFO;
using google::WARNING;
using google::ERROR;
using google::FATAL;

namespace rtidb {
namespace base {

    const int DEBUG = -1;

    template<typename... Arguments>
    std::string FormatArgs(const char* fmt, const Arguments&... args) {
        boost::format f(fmt);
        std::initializer_list<char> {(static_cast<void>(
            f % args
        ), char{}) ...};

        return boost::str(f);
    }

} // namespace base
} // namespace rtidb

#define PDLOG(level, fmt, args...) COMPACT_GOOGLE_LOG_ ## level.stream() << ::rtidb::base::FormatArgs(fmt, args)

#if DCHECK_IS_ON()
#define DEBUGLOG(fmt, args...) COMPACT_GOOGLE_LOG_INFO.stream() << ::rtidb::base::FormatArgs(fmt, args)
#else
#define DEBUGLOG(fmt, args...) \
  static_cast<void>(0), \
  true ? (void) 0 : google::LogMessageVoidify() & COMPACT_GOOGLE_LOG_INFO.stream() << ::rtidb::base::FormatArgs(fmt, args)
#endif

#endif // GLOG_WAPPER_H_