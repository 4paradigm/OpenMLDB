/*
 * bitcode_test.cc
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

#include <memory>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
#include "llvm/ADT/SmallVector.h"
#include "llvm/Bitcode/BitcodeReader.h"
#include "llvm/Bitcode/BitcodeWriter.h"
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/IR/Module.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

using namespace ::llvm;       // NOLINT
using namespace ::llvm::orc;  // NOLINT

ExitOnError ExitOnErr;

namespace fesql {
namespace vm {

class BitcodeTest : public ::testing::Test {
 public:
    BitcodeTest() {}
    ~BitcodeTest() {}
};

/// Check E. If it's in a success state then return the contained value. If
/// it's in a failure state log the error(s) and exit.
template <typename T>
T FeCheck(::llvm::Expected<T> &&E) {
    if (E.takeError()) {
        // NOLINT
    }
    return std::move(*E);
}

TEST_F(BitcodeTest, write_read_module) {
    std::string buf;
    buf.reserve(256 * 1024);
    raw_string_ostream buf_wrapper(buf);
    // write bitcode
    {
        LLVMContext Context;
        // Create the "module" or "program" or "translation unit" to hold the
        // function
        Module *M = new Module("test", Context);
        // Create the main function: first create the type 'int ()'
        FunctionType *FT =
            FunctionType::get(Type::getInt32Ty(Context), /*not vararg*/ false);
        // By passing a module as the last parameter to the Function
        // constructor, it automatically gets appended to the Module.
        Function *F =
            Function::Create(FT, Function::ExternalLinkage, "main", M);

        // Add a basic block to the function... again, it automatically inserts
        // because of the last argument.
        BasicBlock *BB = BasicBlock::Create(Context, "EntryBlock", F);

        // Get pointers to the constant integers...
        Value *Two = ConstantInt::get(Type::getInt32Ty(Context), 2);
        Value *Three = ConstantInt::get(Type::getInt32Ty(Context), 3);

        // Create the add instruction... does not insert...
        Instruction *Add =
            BinaryOperator::Create(Instruction::Add, Two, Three, "addresult");
        // explicitly insert it into the basic block...
        BB->getInstList().push_back(Add);
        // Create the return instruction and add it to the basic block
        BB->getInstList().push_back(ReturnInst::Create(Context, Add));

        WriteBitcodeToFile(*M, buf_wrapper);
        M->print(::llvm::errs(), NULL);
        buf_wrapper.flush();
    }
    {

        std::cout << "buf size " << buf.size() << std::endl;
        auto ctx = llvm::make_unique<LLVMContext>();
        std::string id = "id";
        StringRef id_ref(id);
        StringRef buf_ref(buf);
        MemoryBufferRef mem_buf_ref(buf_ref, id_ref);
        auto module = parseBitcodeFile(mem_buf_ref, *ctx);
        auto m = ExitOnErr(std::move(module));
        auto J = ExitOnErr(::llvm::orc::LLJITBuilder().create());
        ExitOnErr(J->addIRModule(
        std::move(ThreadSafeModule(std::move(m), std::move(ctx)))));
        auto load_fn_jit = ExitOnErr(J->lookup("main"));
        int32_t (*add)() = reinterpret_cast<int32_t (*)()>(load_fn_jit.getAddress());
        int32_t ret = add();
        ASSERT_EQ(5, ret);
    }
}

}  // namespace vm
}  // namespace fesql

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    return RUN_ALL_TESTS();
}
