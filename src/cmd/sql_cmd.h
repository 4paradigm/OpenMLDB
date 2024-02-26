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

#include <termios.h>
#include <stdio.h>

#include <map>
#include <memory>
#include <set>
#include <string>
#include <utility>
#include <vector>

#include "absl/strings/match.h"
#include "absl/strings/strip.h"
#include "base/linenoise.h"
#include "base/texttable.h"
#include "gflags/gflags.h"
#include "sdk/db_sdk.h"
#include "sdk/sql_cluster_router.h"
#include "sdk/sql_router.h"
#include "version.h"  // NOLINT

DEFINE_bool(interactive, true, "Set the interactive");
DEFINE_string(database, "", "Set database, only works in sql_client when cmd is not empty, or ns_client");
DECLARE_string(cmd);
DEFINE_string(spark_conf, "", "The config file of Spark job");

// cluster mode
DECLARE_string(zk_cluster);
DECLARE_string(zk_root_path);
DECLARE_string(zk_auth_schema);
DECLARE_string(zk_cert);
DECLARE_int32(zk_session_timeout);
DECLARE_uint32(zk_log_level);
DECLARE_string(zk_log_file);
DECLARE_string(user);
DECLARE_string(password);

// stand-alone mode
DECLARE_string(host);
DECLARE_int32(port);

// rpc request timeout of CLI
DECLARE_int32(request_timeout);

DECLARE_int32(glog_level);

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
                            std::to_string(OPENMLDB_VERSION_MINOR) + "." + std::to_string(OPENMLDB_VERSION_BUG) + "-" +
                            OPENMLDB_COMMIT_ID;

::openmldb::sdk::DBSDK* cs = nullptr;
::openmldb::sdk::SQLClusterRouter* sr = nullptr;

// strip any whitespace characters begining of the last unfinished statement from `input` (string after last semicolon)
// final SQL strings are appended into `output`
//
// this help handle SQL strings that has space trailing but do not expected to be a statement after semicolon
void StripStartingSpaceOfLastStmt(absl::string_view input, std::string* output) {
    auto last_semicolon_pos = input.find_last_of(';');
    if (last_semicolon_pos != std::string::npos && input.back() != ';') {
        absl::string_view last_stmt = input;
        last_stmt.remove_prefix(last_semicolon_pos + 1);
        while (!last_stmt.empty() && std::isspace(static_cast<unsigned char>(last_stmt.front()))) {
            last_stmt.remove_prefix(1);
        }
        output->append(input.begin(), input.begin() + last_semicolon_pos + 1);
        if (!last_stmt.empty()) {
            output->append(last_stmt);
        }
    } else {
        output->append(input);
    }
}

std::string ExecFetch(const std::string& sql) {
    std::stringstream ss;
    hybridse::sdk::Status status;
    auto result_set = sr->ExecuteSQL(sql, &status);
    if (status.IsOK()) {
        if (result_set) {
            auto schema = result_set->GetSchema();
            if (schema->GetColumnCnt() == 1 && schema->GetColumnName(0) == ::openmldb::sdk::FORMAT_STRING_KEY) {
                while (result_set->Next()) {
                    std::string val;
                    result_set->GetAsString(0, val);
                    ss << val;
                }
            } else {
                ::hybridse::base::TextTable t('-', ' ', ' ');
                for (int idx = 0; idx < schema->GetColumnCnt(); idx++) {
                    t.add(schema->GetColumnName(idx));
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
                ss << t;
                ss << std::endl << result_set->Size() << " rows in set" << std::endl;
            }
        } else {
            if (status.msg != "ok") {
                // status is ok, but we want to print more info by msg
                ss << "SUCCEED: " << status.msg << std::endl;
            } else {
                ss << "SUCCEED" << std::endl;
            }
        }
    } else {
        std::cout << "Error: " << status.ToString() << std::endl;
        if (sr->IsEnableTrace()) {
            // trace has '\n' already
            ss << status.trace;
        }
    }
    return ss.str();
}

base::Status GetPassword(std::string* password) {
    // refer https://www.gnu.org/software/libc/manual/html_mono/libc.html#getpass
    struct termios old_attr, new_attr;
    if (tcgetattr(fileno(stdin), &old_attr) != 0) {
        return {base::ReturnCode::kError, "tcgetattr execute failed!"};
    }
    new_attr = old_attr;
    new_attr.c_lflag &= ~ECHO;
    if (tcsetattr(fileno(stdin), TCSAFLUSH, &new_attr) != 0) {
        return {base::ReturnCode::kError, "tcsetattr execute failed!"};
    }
    size_t len = 0;
    char* lineptr = nullptr;
    if (ssize_t nread = getline(&lineptr, &len, stdin); nread == -1) {
        free(lineptr);
        return {base::ReturnCode::kError, "read input failed!"};
    } else if (nread > 1) {
        password->assign(lineptr, nread - 1);
    }
    free(lineptr);
    (void) tcsetattr(fileno(stdin), TCSAFLUSH, &old_attr);
    return {};
}

void HandleSQL(const std::string& sql) { std::cout << ExecFetch(sql); }

std::string SafeGetString(std::shared_ptr<hybridse::sdk::ResultSet> rs, int idx) {
    std::string tmp;
    if (!rs->GetString(idx, &tmp)) {
        LOG(WARNING) << "fail to get string in col " << idx;
        return "";
    }
    return tmp;
}

bool CheckAllTableStatus() {
    hybridse::sdk::Status status;
    auto rs = sr->ExecuteSQL("show table status like '%'", &status);
    bool is_ok = true;
    if (status.IsOK()) {
        // ref GetTableStatusSchema, just use idx to get column
        while (rs->Next()) {
            auto table = SafeGetString(rs, 1);
            auto db = SafeGetString(rs, 2);
            auto pu = SafeGetString(rs, 8);
            auto warnings = SafeGetString(rs, 13);
            std::string msg = absl::StrCat("table ", db, ".", table, " is broken, `show table status` to check detail");
            bool is_broken = false;
            if (pu != "0") {
                is_broken = true;
                msg.append(", unalive partition: ").append(pu);
            }
            if (!warnings.empty()) {
                is_broken = true;
                msg.append(", warning preview: ").append(warnings.substr(0, 100));
            }
            if (is_broken) {
                is_ok = false;
                std::cout << "ERROR: " << msg << std::endl;
            }
        }
    } else {
        std::cout << "ERROR: fail to get all table status, " << status.ToString() << std::endl;
        is_ok = false;
    }
    return is_ok;
}

// cluster mode if zk_cluster is not empty, otherwise standalone mode
void Shell() {
    DCHECK(cs);
    DCHECK(sr);
    // before all, check all table status
    if (!CheckAllTableStatus()) {
        std::cout << "HINT: Use `openmldb_tool inspect` to get full report." << std::endl;
    }

    // If use FLAGS_cmd, non-interactive. No Logo and make sure router interactive is false
    if (!FLAGS_cmd.empty()) {
        std::string db = FLAGS_database;
        auto ns = cs->GetNsClient();
        std::string error;
        ns->Use(db, error);
        sr->SetDatabase(db);
        sr->SetInteractive(false);
        // No multi sql in cmd str, it'll cause some add troubles.
        // e.g. the first sql is cmd, the following sql won't be executed;
        // the first sql is select, it'll send to offline, all sql will be executed(cmd plan is invalid), output is the
        // last result
        HandleSQL(FLAGS_cmd);
        return;
    }

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

        // todo: should support multiple sql.
        // trim space after last semicolon in sql
        StripStartingSpaceOfLastStmt(buffer, &sql);

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
    }
}

bool InitClusterSDK() {
    auto options = std::make_shared<sdk::SQLRouterOptions>();
    ::openmldb::sdk::ClusterOptions copt;
    options->zk_cluster = FLAGS_zk_cluster;
    options->zk_path = FLAGS_zk_root_path;
    options->zk_session_timeout = FLAGS_zk_session_timeout;
    options->zk_log_level = FLAGS_zk_log_level;
    options->zk_log_file = FLAGS_zk_log_file;
    options->zk_auth_schema = FLAGS_zk_auth_schema;
    options->zk_cert = FLAGS_zk_cert;
    options->spark_conf_path = FLAGS_spark_conf;
    options->request_timeout = FLAGS_request_timeout;
    options->user = FLAGS_user;
    options->password = FLAGS_password;
    if (!::google::GetCommandLineFlagInfoOrDie("user").is_default &&
            ::google::GetCommandLineFlagInfoOrDie("password").is_default) {
        std::cout << "Please enter password:" << std::endl;
        if (auto status = GetPassword(&options->password); !status.OK()) {
            std::cout << status.GetMsg() << std::endl;
            return false;
        }
    }
    cs = new ::openmldb::sdk::ClusterSDK(options);
    if (!cs->Init()) {
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

void ClusterSQLClient() {
    // setup here cuz init xx sdk will print log too
    base::SetupGlog();
    if (!InitClusterSDK()) {
        return;
    }
    Shell();
}

bool InitStandAloneSDK() {
    // connect to nameserver
    if (FLAGS_host.empty() || FLAGS_port == 0) {
        std::cout << "ERROR: Host or port is missing" << std::endl;
        return false;
    }
    auto options = std::make_shared<sdk::StandaloneOptions>(FLAGS_host, FLAGS_port);
    options->user = FLAGS_user;
    options->password = FLAGS_password;
    if (!::google::GetCommandLineFlagInfoOrDie("user").is_default &&
            ::google::GetCommandLineFlagInfoOrDie("password").is_default) {
        std::cout << "Please enter password:" << std::endl;
        if (auto status = GetPassword(&options->password); !status.OK()) {
            std::cout << status.GetMsg() << std::endl;
            return false;
        }
    }
    options->request_timeout = FLAGS_request_timeout;
    cs = new ::openmldb::sdk::StandAloneSDK(options);
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
    base::SetupGlog();
    if (!InitStandAloneSDK()) {
        return;
    }
    Shell();
}

}  // namespace openmldb::cmd

#endif  // SRC_CMD_SQL_CMD_H_
