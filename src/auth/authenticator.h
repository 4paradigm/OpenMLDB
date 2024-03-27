#ifndef SRC_AUTH_AUTHENTICATOR_H_
#define SRC_AUTH_AUTHENTICATOR_H_

#include <string>
#include <variant>

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

class Authenticator : public brpc::Authenticator {
 public:
    using GetUserPasswordFunc = std::optional<std::string> (*)(const std::string&);
    Authenticator() : getUserPassword(DefaultGetUserPassword) {}
    Authenticator(GetUserPasswordFunc getUserPasswordFunc) : getUserPassword(getUserPasswordFunc) {}
    int GenerateCredential(std::string* auth_str) const;

    int VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                         brpc::AuthContext* out_ctx) const;

 private:
    GetUserPasswordFunc getUserPassword;

    // Stub function for GetUserPasswordFunc
    static std::optional<std::string> DefaultGetUserPassword(const std::string&) {
        return std::optional<std::string>("1e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855");
    }

    bool VerifyUsernamePassword(const std::string& username, const std::string& password) const;
    bool VerifyToken(const std::string& token) const;
};

}  // namespace openmldb::authn
#endif  // SRC_AUTH_AUTHENTICATOR_H_
