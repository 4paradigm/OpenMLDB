#ifndef SRC_AUTH_AUTHENTICATOR_H_
#define SRC_AUTH_AUTHENTICATOR_H_

#include <string>
#include <variant>

#include "brpc/authenticator.h"
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
    int GenerateCredential(std::string* auth_str) const;

    int VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                         brpc::AuthContext* out_ctx) const;

 private:
    bool VerifyUsernamePassword(const std::string& username, const std::string& password) const;
    bool VerifyToken(const std::string& token) const;
};

#endif  // SRC_AUTH_AUTHENTICATOR_H_
