#ifndef SRC_AUTH_AUTH_UTILS_H_
#define SRC_AUTH_AUTH_UTILS_H_

#include <string>

namespace openmldb::auth {
std::string FormUserHost(const std::string& username, const std::string& host);
}  // namespace openmldb::auth

#endif  // SRC_AUTH_AUTH_UTILS_H_
