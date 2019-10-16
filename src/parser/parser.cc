/*
 * parser/parser.cc
 * Copyright (C) 2019 chenjing <chenjing@4paradigm.com>
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

//
// FeSQL Parser
//
// Desc: Parse FeSQL Command
//
#include "parser/node.h"
#include "parser/parser.h"
#include <iostream>

/**
 * FeSQL command parser
 * @param sqlstr
 * @param list
 * @return 1 if success
 */
int FeSqlParse(const char* sqlstr, ::fedb::sql::SQLNodeList* list) {
    yyscan_t scanner;
    yylex_init(&scanner);
    yy_scan_string(sqlstr, scanner);
    int ret = yyparse(scanner, list);
    std::cout<<"ret: "<<ret<<" list size:"<<list->Size()<<std::endl;
    return ret;
}


