/*
 * csv_catalog_test.cc
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

#include "vm/csv_catalog.h"
#include "gtest/gtest.h"
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

using namespace llvm;       // NOLINT (build/namespaces)
using namespace llvm::orc;  // NOLINT (build/namespaces)


namespace fesql {
namespace vm {

class CSVCatalogTest : public ::testing::Test {

 public:
    CSVCatalogTest() {}
    ~CSVCatalogTest() {}
};

TEST_F(CSVCatalogTest, test_engine) {
    std::string db_dir = "./db_dir";
    std::shared_ptr<CSVCatalog> catalog(new CSVCatalog(db_dir));
    ASSERT_TRUE(catalog->Init());
    Engine engine(catalog);
    std::string sql = "select col1 from table1 limit 1;";
    std::string db = "db1";
    RunSession session;
    base::Status status;
    bool ok = engine.Get(sql, db, session, status);
    ASSERT_TRUE(ok);
    std::vector<int8_t*> buf;
    int32_t code = session.Run(buf, 1);
    ASSERT_EQ(0, code);
}

TEST_F(CSVCatalogTest, test_catalog_init) {
    std::string db_dir = "./db_dir";
    CSVCatalog catalog(db_dir);
    ASSERT_TRUE(catalog.Init());
}

TEST_F(CSVCatalogTest, test_handler_init) { 
    std::string table_dir = "./table1";
    std::string table_name = "table1";
    std::string db = "db1";
    std::shared_ptr<::arrow::fs::FileSystem> fs(new arrow::fs::LocalFileSystem());
    CSVTableHandler handler(table_dir, table_name, db, fs);
    bool ok = handler.Init();
    ASSERT_TRUE(ok);
    storage::RowView rv(handler.GetSchema());
    auto it = handler.GetIterator();
    while (it->Valid()) {
        auto value = it->GetValue();
        rv.Reset(reinterpret_cast<const int8_t*>(value.data()), value.size());
        char* data = NULL;
        uint32_t size = 0;
        rv.GetString(0, &data, &size);
        std::string view(data, size);
        std::cout<< view << std::endl;
        rv.GetString(1, &data, &size);
        std::string view2(data, size);
        std::cout<< view2 << std::endl;
        it->Next();
    }
}

} // namespace vm
} // namepsace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
