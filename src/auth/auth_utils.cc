#include "auth_utils.h"

namespace openmldb::auth {
std::string FormUserHost(const std::string& username, const std::string& host) { return username + "@" + host; }
}  // namespace openmldb::auth