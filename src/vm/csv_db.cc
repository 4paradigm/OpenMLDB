/*
 * csv_db.cc
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#include "gflags/gflags.h"
#include "vm/csv_catalog.h"
#include "arrow/filesystem/localfs.h"
#include "vm/engine.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "base/texttable.h"

DEFINE_string(format, "", "config the format of output, csv or nothing");
DEFINE_string(db_dir, "", "config the dir of database");
DEFINE_string(db, "", "config the db to use");
DEFINE_string(query, "", "config the sql to query");


using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)

namespace fesql {
namespace vm {

void PrintRows(const Schema& schema, const std::vector<int8_t*>& rows) {
    base::TextTable t('-', '|', '+');
    // Add Header
    for (int32_t i = 0; i < schema.size(); i++) {
        const type::ColumnDef& column = schema.Get(i);
        t.add(column.name());
    }
    t.endOfRow();
    storage::RowView row_decoder(schema);
    auto it = rows.begin();
    for (; it != rows.end(); ++it) {
        row_decoder.Reset(*it);
        for (int32_t i = 0; i < schema.size(); i++) {
            const type::ColumnDef& column = schema.Get(i);
            switch(column.type()) {
                case type::kInt16:
                    {
                        int16_t value;
                        row_decoder.GetInt16((uint32_t)i, &value);
                        t.add(std::to_string(value));
                        break;
                    }
                case type::kInt32:
                    {
                        int32_t value;
                        row_decoder.GetInt32((uint32_t)i, &value);
                        t.add(std::to_string(value));
                        break;
                    }
                case type::kInt64:
                    {
                        int64_t value;
                        row_decoder.GetInt64((uint32_t)i, &value);
                        t.add(std::to_string(value));
                        break;
                    }
                case type::kFloat:
                    {
                        float value;
                        row_decoder.GetFloat((uint32_t)i, &value);
                        t.add(std::to_string(value));
                        break;
                    }
                case type::kDouble:
                    {
                        double value;
                        row_decoder.GetDouble((uint32_t)i, &value);
                        t.add(std::to_string(value));
                        break;
                    }
                case type::kVarchar:
                    {
                        char *data = NULL;
                        uint32_t size = 0;
                        row_decoder.GetString((uint32_t)i, &data, &size);
                        t.add(std::string(data, size));
                        break;
                    }
                default: {
                    t.add("NA");
                }
            }
        }
        t.endOfRow();
    }

    std::cout << t << std::endl;
    std::cout << rows.size() << " row in set" << std::endl;
}

void Run() {
    std::cout << "run " << FLAGS_query << std::endl;
    std::shared_ptr<CSVCatalog> catalog(new CSVCatalog(FLAGS_db_dir));
    bool ok = catalog->Init();
    if (!ok) {
        std::cout << "fail to init catalog from path " << FLAGS_db_dir << std::endl;
        return;
    }
    Engine engine(catalog);
    RunSession session;
    base::Status status;
    ok = engine.Get(FLAGS_query, FLAGS_db, session, status);
    if (!ok) {
        std::cout << "fail to compile sql for " << status.msg << std::endl;
        return;
    }
    std::vector<int8_t*> buf;
    int32_t code = session.Run(buf, 1000);
    if (code == 0) {
        ::fesql::base::TextTable t('-', '|', '+');
        PrintRows(session.GetSchema(), buf);
    }else {
        std::cout << "fail to execute sql" << std::endl;
    }
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char **argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    fesql::vm::Run();
    return 0;
}
