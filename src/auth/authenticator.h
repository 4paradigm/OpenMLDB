#ifndef AUTHENTICATOR_H
#define AUTHENTICATOR_H

#include "brpc/authenticator.h"

class Authenticator : public brpc::Authenticator {
 public:
    Authenticator() : username_(""), password_("") {}
    Authenticator(const std::string& username, const std::string& password);
    int GenerateCredential(std::string* auth_str) const override;
    int VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                         brpc::AuthContext* out_ctx) const override;

 private:
    bool VerifyUsernamePassword(const std::string& username, const std::string& password) const;
    std::string username_;
    std::string password_;
};

#endif  // AUTHENTICATOR_H