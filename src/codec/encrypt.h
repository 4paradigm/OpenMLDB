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

#ifndef SRC_CODEC_ENCRYPT_H_
#define SRC_CODEC_ENCRYPT_H_

#include <iomanip>
#include <string>
#include <sstream>

#include "absl/strings/str_cat.h"
#include "openssl/sha.h"

namespace openmldb {
namespace codec {

inline constexpr uint8_t VERSION = 1;

static inline std::string SHA256(const std::string& str) {
    unsigned char hash[SHA256_DIGEST_LENGTH];
    SHA256_CTX sha256;
    SHA256_Init(&sha256);
    SHA256_Update(&sha256, str.c_str(), str.size());
    SHA256_Final(hash, &sha256);
    std::stringstream ss;
    for (int i = 0; i < SHA256_DIGEST_LENGTH; i++) {
        ss << std::hex << std::setw(2) << std::setfill('0') << static_cast<int>(hash[i]);
    }
    return ss.str();
}

static inline std::string Encrypt(const std::string& passwd) {
    return absl::StrCat(VERSION, SHA256(passwd));
}

}  // namespace codec
}  // namespace openmldb

#endif  // SRC_CODEC_ENCRYPT_H_
