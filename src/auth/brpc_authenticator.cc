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

#include "brpc_authenticator.h"

#include "auth_utils.h"
#include "butil/endpoint.h"

namespace openmldb::authn {

int BRPCAuthenticator::GenerateCredential(std::string* auth_str) const {
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

int BRPCAuthenticator::VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
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
        auto host = butil::ip2str(client_addr.ip).c_str();
        std::string username = credential.substr(0, pos);
        std::string password = credential.substr(pos + 1);
        if (is_authenticated_(host, username, password)) {
            out_ctx->set_user(auth::FormUserHost(username, host));
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

bool BRPCAuthenticator::VerifyToken(const std::string& token) const { return token == "default"; }

}  // namespace openmldb::authn
