/*
 * fesql.cc
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

#include <fcntl.h>
#include <sched.h>
#include <signal.h>
#include <unistd.h>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>

#include "base/linenoise.h"
#include "base/strings.h"
#include "brpc/server.h"
#include "cmd/version.h"
#include "dbms/dbms_server_impl.h"
#include "glog/logging.h"
#include "sdk/dbms_sdk.h"

DECLARE_string(endpoint);
DECLARE_int32(port);
DECLARE_int32(thread_pool_size);

DEFINE_string(role, "tablet | dbms | client ", "Set the fesql role");

static ::fesql::sdk::DBMSSdk *dbms_sdk = NULL;



void HandleSQLScript(std::string basicString);
void HandleCreateSchema(std::string str, ::fesql::base::Status &status);

void HandleEnterDatabase(std::string &db_name);
void SetupLogging(char *argv[]) { google::InitGoogleLogging(argv[0]); }

void StartDBMS(char *argv[]) {
    SetupLogging(argv);
    ::fesql::dbms::DBMSServerImpl *dbms = new ::fesql::dbms::DBMSServerImpl();
    brpc::ServerOptions options;
    options.num_threads = FLAGS_thread_pool_size;
    brpc::Server server;

    if (server.AddService(dbms, brpc::SERVER_DOESNT_OWN_SERVICE) != 0) {
        LOG(WARNING) << "Fail to add dbms service";
        exit(1);
    }

    if (server.Start(FLAGS_port, &options) != 0) {
        LOG(WARNING) << "Fail to start dbms server";
        exit(1);
    }

    std::ostringstream oss;
    oss << FESQL_VERSION_MAJOR << "." << FESQL_VERSION_MEDIUM << "."
        << FESQL_VERSION_MINOR << "." << FESQL_VERSION_BUG;
    LOG(INFO) << "start dbms on port " << FLAGS_port << " with version "
              << oss.str();
    server.set_version(oss.str());
    server.RunUntilAskedToQuit();
}

void HandleCreateGroup(const std::vector<std::string> &args) {
    if (args.size() < 3) {
        std::cout << "invalid input args" << std::endl;
        return;
    }

    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }

    ::fesql::sdk::GroupDef group;
    group.name = args[2];
    ::fesql::base::Status status;
    dbms_sdk->CreateGroup(group, status);
    if (status.code == 0) {
        std::cout << "Create group " << args[2] << " success" << std::endl;
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}

void HandleCreateDatabase(std::vector<std::string> &args) {
    if (args.size() != 3) {
        std::cout << "invalid input args: require create database db_name"
                  << std::endl;
    }

    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }

    ::fesql::sdk::DatabaseDef database;
    database.name = args[2];
    ::fesql::base::Status status;
    dbms_sdk->CreateDatabase(database, status);
    if (status.code == 0) {
        std::cout << "Create database" << args[2] << " success" << std::endl;
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}
void HandleCreateSchema(std::vector<std::string> &args) {
    if (args.size() != 3) {
        std::cout
            << "invalid input args: require create table path_to/schema.sql"
            << std::endl;
    }

    std::ifstream in;
    in.open(args[2]);  // open the input file
    if (!in.is_open()) {
        std::cout << "Error! Incorrect file." << std::endl;
        return;
    }
    std::stringstream str_stream;
    str_stream << in.rdbuf();            // read the file
    std::string str = str_stream.str();  // str holds the content of the file
    std::cout << str << "\n";  // you can do anything with the string!!!
    ::fesql::base::Status status;
    HandleCreateSchema(str, status);
    if (status.code == 0) {
        std::cout << "create table " << args[2] << " sucess" << std::endl;
    } else {
        std::cout << "create table fail with error " << status.msg << std::endl;
    }
}

void HandleCreateSchema(std::string str, ::fesql::base::Status &status) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    dbms_sdk->CreateTable(str, status);
}

void HandleShowDatabases() {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    std::vector<std::string> items;
    dbms_sdk->GetDatabases(items, status);
    if (status.code == 0) {
        dbms_sdk->PrintItems(items);
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}
void HandleShowTables() {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    std::vector<std::string> items;
    dbms_sdk->GetTables(items, status);
    if (status.code == 0) {
        dbms_sdk->PrintItems(items);
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}

void HandleShowSchema(std::string table_name) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    ::fesql::type::TableDef table;
    dbms_sdk->GetSchema(table_name, table, status);
    if (status.code == 0) {
        dbms_sdk->PrintTableSchema(table);
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}


void StartClient(char *argv[]) {
    SetupLogging(argv);
    std::cout << "Welcome to FeSQL " << FESQL_VERSION_MAJOR << "."
              << FESQL_VERSION_MEDIUM << "." << FESQL_VERSION_MINOR << "."
              << FESQL_VERSION_BUG << std::endl;
    std::string display_prefix = ">";
    std::string continue_prefix = "...";
    std::string cmd_str;
    bool cmd_mode = true;
    while (true) {
        std::string buf;
        char *line = ::fesql::base::linenoise(
            cmd_mode ? display_prefix.c_str() : continue_prefix.c_str());
        if (line == NULL) {
            return;
        }
        if (line[0] != '\0' && line[0] != '/') {
            buf.assign(line);
            if (!buf.empty()) {
                ::fesql::base::linenoiseHistoryAdd(line);
            }
        }
        ::fesql::base::linenoiseFree(line);
        if (buf.empty()) {
            continue;
        }

        cmd_str.append(buf);
        //TODO(CHENJING) remove
        if (cmd_mode && cmd_str[0] == '.') {
            cmd_mode = true;
            std::vector<std::string> parts;
            ::fesql::base::SplitString(cmd_str, " ", parts);
            if (parts[0] == ".create") {
                if (parts.size() < 3) {
                    std::cout << "invalid input args: require\n"
                              << ".create group group_name \n"
                              << ".create database db_name\n"
                              << "| .create table table_schema_path\n";
                }
                if (parts[1] == "group") {
                    HandleCreateGroup(parts);
                } else if (parts[1] == "schema") {
                    HandleCreateSchema(parts);
                } else if (parts[1] == "database") {
                    HandleCreateDatabase(parts);
                }
            } else if (parts[0] == ".use") {
                if (parts.size() != 2) {
                    std::cout << "invalid input args: require"
                              << " .use db_name\n";
                } else {
                    HandleEnterDatabase(parts[1]);
                }
            } else if (parts[0] == ".show") {
                if (parts.size() < 2) {
                    std::cout << "invalid input args: require\n"
                              << ".show tables\n"
                              << "| .show databases\n"
                              << "| .show table table_name\n";
                } else {
                    if (parts[1] == "table") {
                        if (parts.size() != 3) {
                            std::cout << "invalid input args: require"
                                      << " .show table table_name";
                        } else {
                            HandleShowSchema(parts[2]);
                        }
                    } else if (parts[1] == "tables") {
                        if (parts.size() != 2) {
                            std::cout << "invalid input args: require"
                                      << ".show tables\n";
                        } else {
                            HandleShowTables();
                        }
                    } else if (parts[1] == "databases") {
                        if (parts.size() != 2) {
                            std::cout << "invalid input args: require"
                                      << " .show databases\n";
                        } else {
                            HandleShowDatabases();
                        }
                    } else {
                        std::cout << "invalid input args: require\n"
                                  << ".show tables\n"
                                  << "| .show databases\n"
                                  << "| .show table table_name\n";
                    }
                }
            } else {
                std::cout << "Invalid command" << std::endl;
            }
            cmd_str.clear();
        } else {
            cmd_mode = false;
            if (cmd_str.back() == ';') {
                HandleSQLScript(cmd_str);
                cmd_str.clear();
                cmd_mode = true;
            }
        }
    }
}
void HandleEnterDatabase(std::string &db_name) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    ::fesql::sdk::DatabaseDef database;
    database.name = db_name;
    dbms_sdk->EnterDatabase(database, status);
    if (status.code == 0) {
        std::cout << "sucess" << std::endl;
    } else {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}
void HandleSQLScript(std::string script) {
    if (dbms_sdk == NULL) {
        dbms_sdk = ::fesql::sdk::CreateDBMSSdk(FLAGS_endpoint);
        if (dbms_sdk == NULL) {
            std::cout << "Fail to connect to dbms" << std::endl;
            return;
        }
    }
    ::fesql::base::Status status;
    dbms_sdk->ExecuteScript(script, status);
    if (status.code != 0) {
        std::cout << "ERROR " << status.code << ":" << status.msg << std::endl;
    }
}

int main(int argc, char *argv[]) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    if (FLAGS_role == "dbms") {
        StartDBMS(argv);
    } else if (FLAGS_role == "client") {
        StartClient(argv);
    } else {
        std::cout << "Start failed! FLAGS_role must be tablet, client, dbms"
                  << std::endl;
        return 1;
    }
    return 0;
}
