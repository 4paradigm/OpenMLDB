#include "authenticator.h"

#include "base/glog_wrapper.h"
namespace openmldb::authn {

int Authenticator::GenerateCredential(std::string* auth_str) const {
    std::visit(
        [auth_str](const auto& s) {
            using T = std::decay_t<decltype(s)>;
            if constexpr (std::is_same_v<T, UserToken>) {
                *auth_str = "u" + s.user + ":" + s.password;
            } else if constexpr (std::is_same_v<T, ServiceToken>) {
                *auth_str = "s" + s.token;
            }
        },
        g_auth_token);
    return 0;
}

int Authenticator::VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                                    brpc::AuthContext* out_ctx) const {
    if (auth_str.length() < 2) {
        return -1;
    }

    char auth_type = auth_str[0];
    std::string credential = auth_str.substr(1);
    if (auth_type == 'u') {
        size_t pos = credential.find(':');
        if (pos == std::string::npos) {
            return -1;
        }
        std::string username = credential.substr(0, pos);
        std::string password = credential.substr(pos + 1);
        if (VerifyUsernamePassword(username, password)) {
            out_ctx->set_user(username);
            out_ctx->set_is_service(false);
            return 0;
        }
    } else if (auth_type == 's') {
        if (VerifyToken(credential)) {
            out_ctx->set_is_service(true);
            return 0;
        }
    }

    return -1;
}

bool Authenticator::VerifyUsernamePassword(const std::string& username, const std::string& password) const {
    return username == "root";
}

bool Authenticator::VerifyToken(const std::string& token) const { return token == "default"; }

}  // namespace openmldb::authn
