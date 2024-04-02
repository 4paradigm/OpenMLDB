#ifndef SRC_AUTH_BRPC_AUTHENTICATOR_H_
#define SRC_AUTH_BRPC_AUTHENTICATOR_H_

#include <functional>  // Include for std::function
#include <string>
#include <variant>
#include <utility>
#include "brpc/authenticator.h"

namespace openmldb::authn {

struct ServiceToken {
    std::string token;
};

struct UserToken {
    std::string user, password;
};

using AuthToken = std::variant<ServiceToken, UserToken>;

inline AuthToken g_auth_token;

class BRPCAuthenticator : public brpc::Authenticator {
 public:
    // Update to use std::function
    using IsAuthenticatedFunc = std::function<bool(const std::string&, const std::string&, const std::string&)>;

    // Default constructor
    BRPCAuthenticator() {
        is_authenticated_ = [](const std::string& host, const std::string& username, const std::string& password) {
            return true;
        };
    }

    // Constructor that initializes the std::function member
    explicit BRPCAuthenticator(IsAuthenticatedFunc is_authenticated) : is_authenticated_(std::move(is_authenticated)) {}

    int GenerateCredential(std::string* auth_str) const override;
    int VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                         brpc::AuthContext* out_ctx) const override;

 private:
    IsAuthenticatedFunc is_authenticated_;  // Now a std::function
    bool VerifyToken(const std::string& token) const;
};

}  // namespace openmldb::authn
#endif  // SRC_AUTH_BRPC_AUTHENTICATOR_H_
