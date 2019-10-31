/*
 * dbms_sdk.h
 * Copyright (C) 4paradigm.com 2019 wangtaize <wangtaize@4paradigm.com>
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#ifndef FESQL_DBMS_SDK_H_
#define FESQL_DBMS_SDK_H_

namespace fesql {
namespace sdk {

struct GroupDef {
    std::string name;
};

class DBMSSdk {

public:
    DBMSSdk(const std::string& endpoint);
    ~DBMSSdk();
    bool Init();
    int32_t CreateGroup(const GroupDef& group);
private:
    std::string endpoint_;
};

} // namespace of sdk
} // namespace of fesql
#endif /* !FESQL_DBMS_SDK_H_ */
