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

#ifndef SRC_AUTH_BRPC_AUTHENTICATOR_H_
#define SRC_AUTH_BRPC_AUTHENTICATOR_H_
#include <functional>
#include <string>
#include <utility>
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

class BRPCAuthenticator : public brpc::Authenticator {
 public:
    using IsAuthenticatedFunc = std::function<bool(const std::string&, const std::string&, const std::string&)>;

    BRPCAuthenticator() {
        is_authenticated_ = [](const std::string& host, const std::string& username, const std::string& password) {
            return true;
        };
    }

    explicit BRPCAuthenticator(IsAuthenticatedFunc is_authenticated) : is_authenticated_(std::move(is_authenticated)) {}

    int GenerateCredential(std::string* auth_str) const override;
    int VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                         brpc::AuthContext* out_ctx) const override;

 private:
    IsAuthenticatedFunc is_authenticated_;
    bool VerifyToken(const std::string& token) const;
};

}  // namespace openmldb::authn
#endif  // SRC_AUTH_BRPC_AUTHENTICATOR_H_
