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

    template<typename... Arguments>
    std::string FormatArgs(const std::string& fmt, const Arguments&... args)
    {
        boost::format f(fmt);
        std::initializer_list<char> {(static_cast<void>(
            f % args
        ), char{}) ...};

        return boost::str(f);
    }

} // namespace base
} // namespace rtidb

#define PDLOG(level, fmt, args...) COMPACT_GOOGLE_LOG_ ## level.stream() << ::rtidb::base::FormatArgs(fmt, args)

#endif // GLOG_WAPPER_H_