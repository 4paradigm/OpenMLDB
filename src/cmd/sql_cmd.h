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

#ifndef SRC_CMD_SQL_CMD_H_
#define SRC_CMD_SQL_CMD_H_
#include <algorithm>
#include <fstream>
#include <iostream>
#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/strip.h"
#include "base/linenoise.h"
#include "boost/regex.hpp"
#include "base/texttable.h"
#include "gflags/gflags.h"
#include "sdk/db_sdk.h"
#include "sdk/sql_cluster_router.h"
#include "version.h"  // NOLINT
DEFINE_bool(interactive, true, "Set the interactive");
DEFINE_string(database, "", "Set database");
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_string(cmd);
// stand-alone mode
DECLARE_string(host);
DECLARE_int32(port);
DECLARE_int32(request_timeout_ms);

namespace openmldb::cmd {
const std::string LOGO =  // NOLINT

    "  _____                    ______  _       _____   ______   \n"
    " / ___ \\                  |  ___ \\| |     (____ \\ (____  \\  \n"
    "| |   | |____   ____ ____ | | _ | | |      _   \\ \\ ____)  ) \n"
    "| |   | |  _ \\ / _  )  _ \\| || || | |     | |   | |  __  (  \n"
    "| |___| | | | ( (/ /| | | | || || | |_____| |__/ /| |__)  ) \n"
    " \\_____/| ||_/ \\____)_| |_|_||_||_|_______)_____/ |______/  \n"
    "        |_|                                                 \n";

const std::string VERSION = std::to_string(OPENMLDB_VERSION_MAJOR) + "." +  // NOLINT
                            std::to_string(OPENMLDB_VERSION_MINOR) + "." + std::to_string(OPENMLDB_VERSION_BUG) + "." +
                            OPENMLDB_COMMIT_ID;

::openmldb::sdk::DBSDK* cs = nullptr;
::openmldb::sdk::SQLClusterRouter* sr = nullptr;

void HandleSQL(const std::string& sql) {
    hybridse::sdk::Status status;
    auto result_set = sr->ExecuteSQL(sql, &status);
    if (status.IsOK()) {
        if (result_set) {
            auto schema = result_set->GetSchema();
            if (schema->GetColumnCnt() == 1 && schema->GetColumnName(0) == ::openmldb::sdk::FORMAT_STRING_KEY) {
                while (result_set->Next()) {
                    std::string val;
                    result_set->GetAsString(0, val);
                    std::cout << val;
                }
            } else {
                ::hybridse::base::TextTable t('-', ' ', ' ');
                for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
                    std::string name = schema->GetColumnName(idx);
                    t.add(name);
                }
                t.end_of_row();
                while (result_set->Next()) {
                    for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
                        std::string val;
                        result_set->GetAsString(idx, val);
                        t.add(val);
                    }
                    t.end_of_row();
                }
                std::cout << t;
                std::cout << std::endl << result_set->Size() << " rows in set" << std::endl;
            }
        } else {
            if (status.msg != "ok") {
                std::cout << "SUCCEED: " << status.msg << std::endl;
            } else {
                std::cout << "SUCCEED" << std::endl;
            }
        }
    } else {
        std::cout << "Error: " << status.msg << std::endl;
    }
}

// cluster mode: if zk_cluster is not empty,
// standalone mode:
void Shell() {
    DCHECK(cs);
    DCHECK(sr);
    if (FLAGS_interactive) {
        std::cout << LOGO << std::endl;
        std::cout << "v" << VERSION << std::endl;
    }

    std::string ns_endpoint;
    auto ns_client = cs->GetNsClient();
    if (!ns_client) {
        LOG(WARNING) << "fail to connect nameserver";
        return;
    } else {
        ns_endpoint = ns_client->GetEndpoint();
    }

    std::string display_prefix = ns_endpoint + "/" + sr->GetDatabase() + "> ";
    std::string multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
    std::string sql;
    bool multi_line = false;
    while (true) {
        std::string buffer;
        if (!FLAGS_cmd.empty()) {
            buffer = FLAGS_cmd;
            std::string db = FLAGS_database;
            auto ns = cs->GetNsClient();
            std::string error;
            ns->Use(db, error);
            sr->SetDatabase(db);
        } else {
            char* line = ::openmldb::base::linenoise(multi_line ? multi_line_perfix.c_str() : display_prefix.c_str());
            if (line == nullptr) {
                return;
            }
            if (line[0] != '\0' && line[0] != '/') {
                buffer.assign(line);
                if (!buffer.empty()) {
                    ::openmldb::base::linenoiseHistoryAdd(line);
                }
            }
            ::openmldb::base::linenoiseFree(line);
            if (buffer.empty()) {
                continue;
            }
        }
        // todo: should support multiple sql.
        // trim space after last semicolon in sql
        auto last_semicolon_pos = buffer.find_last_of(';');
        if (last_semicolon_pos != std::string::npos && buffer.back() != ';') {
            absl::string_view input(buffer.substr(last_semicolon_pos + 1, buffer.size() - last_semicolon_pos));
            std::string prefix(" ");
            while (true) {
                if (!absl::ConsumePrefix(&input, prefix)) {
                    break;
                }
            }
            sql.append(buffer.begin(), buffer.begin() + last_semicolon_pos + 1);
            sql.append(input.begin(), input.end());
        } else {
            sql.append(buffer);
        }
        if (sql.length() == 4 || sql.length() == 5) {
            if (absl::EqualsIgnoreCase(sql, "quit;") || absl::EqualsIgnoreCase(sql, "exit;") ||
                absl::EqualsIgnoreCase(sql, "quit") || absl::EqualsIgnoreCase(sql, "exit")) {
                std::cout << "Bye" << std::endl;
                return;
            }
        }
        if (sql.back() == ';') {
            HandleSQL(sql);
            multi_line = false;
            display_prefix = ns_endpoint + "/" + sr->GetDatabase() + "> ";
            multi_line_perfix = std::string(display_prefix.length() - 3, ' ') + "-> ";
            sql.clear();
        } else {
            sql.append("\n");
            multi_line = true;
        }
        if (!FLAGS_interactive) {
            return;
        }
    }
}

void ClusterSQLClient() {
    ::openmldb::sdk::ClusterOptions copt;
    copt.zk_cluster = FLAGS_zk_cluster;
    copt.zk_path = FLAGS_zk_root_path;
    cs = new ::openmldb::sdk::ClusterSDK(copt);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return;
    }
    sr->SetInteractive(FLAGS_interactive);
    Shell();
}

bool StandAloneInit() {
    // connect to nameserver
    if (FLAGS_host.empty() || FLAGS_port == 0) {
        std::cout << "ERROR: Host or port is missing" << std::endl;
        return false;
    }
    cs = new ::openmldb::sdk::StandAloneSDK(FLAGS_host, FLAGS_port);
    bool ok = cs->Init();
    if (!ok) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return false;
    }
    sr = new ::openmldb::sdk::SQLClusterRouter(cs);
    if (!sr->Init()) {
        std::cout << "ERROR: Failed to connect to db" << std::endl;
        return false;
    }
    sr->SetInteractive(FLAGS_interactive);
    return true;
}

void StandAloneSQLClient() {
    if (!StandAloneInit()) {
        return;
    }
    Shell();
}

}  // namespace openmldb::cmd

#endif  // SRC_CMD_SQL_CMD_H_
