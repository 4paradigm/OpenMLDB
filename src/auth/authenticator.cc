#include "authenticator.h"

Authenticator::Authenticator(const std::string& username, const std::string& password)
    : username_(username), password_(password) {}

int Authenticator::GenerateCredential(std::string* auth_str) const {
    *auth_str = username_ + ":" + password_;
    return 0;
}

int Authenticator::VerifyCredential(const std::string& auth_str, const butil::EndPoint& client_addr,
                                    brpc::AuthContext* out_ctx) const {
    // Parse the auth_str to extract username and password
    std::string username, password;
    size_t pos = auth_str.find(':');
    if (pos != std::string::npos) {
        username = auth_str.substr(0, pos);
        password = auth_str.substr(pos + 1);
        // Verify the username and password against your user database or authentication server
        if (VerifyUsernamePassword(username, password)) {
            out_ctx->set_user(username);
            return 0;  // Authentication successful
        }
    }
    return -1;  // Authentication failed
}
bool Authenticator::VerifyUsernamePassword(const std::string& username, const std::string& password) const {
    // Implement your username and password verification logic here
    // Return true if the username and password are valid, false otherwise
    // You can connect to your user database or authentication server to perform the verification
    // For simplicity, let's assume a hardcoded username and password
    return true;
}