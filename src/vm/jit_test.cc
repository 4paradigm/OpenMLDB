/*
 * jit_test.cc
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

#include "vm/jit.h"

#include <memory>
#include <utility>
#include <vector>
#include "gtest/gtest.h"
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

int32_t test_fn(int32_t a) { return a + 1; }

namespace fesql {
namespace vm {

class JITTest : public ::testing::Test {
 public:
    JITTest() {}
    ~JITTest() {}
};

/// Check E. If it's in a success state then return the contained value. If
/// it's in a failure state log the error(s) and exit.
template <typename T>
T FeCheck(::llvm::Expected<T> &&E) {
    E.takeError();
    return std::move(*E);
}

TEST_F(JITTest, test_release_module) {
    auto jit = FeCheck((FeSQLJITBuilder().create()));
    ::llvm::orc::JITDylib &jd = jit->createJITDylib("test");
    ::llvm::orc::VModuleKey m1 = jit->CreateVModule();
    int (*Add1)(int) = NULL;
    {
        auto ct2 = llvm::make_unique<LLVMContext>();
        auto m = make_unique<Module>("custom_fn", *ct2);
        Function *Add1F =
            Function::Create(FunctionType::get(Type::getInt32Ty(*ct2),
                                               {Type::getInt32Ty(*ct2)}, false),
                             Function::ExternalLinkage, "add1", m.get());
        BasicBlock *BB = BasicBlock::Create(*ct2, "EntryBlock", Add1F);
        IRBuilder<> builder(BB);
        Value *One = builder.getInt32(1);
        assert(Add1F->arg_begin() !=
               Add1F->arg_end());               // Make sure there's an arg
        Argument *ArgX = &*Add1F->arg_begin();  // Get the arg
        ArgX->setName("AnArg");  // Give it a nice symbolic name for fun.
        Value *Add = builder.CreateAdd(One, ArgX);
        ::llvm::Type *i32_ty = builder.getInt32Ty();
        ::llvm::FunctionCallee callee =
            m->getOrInsertFunction("test_fn", i32_ty, i32_ty);
        std::vector<Value *> call_args;
        call_args.push_back(Add);
        ::llvm::ArrayRef<Value *> call_args_ref(call_args);
        ::llvm::Value *ret = builder.CreateCall(callee, call_args_ref);
        builder.CreateRet(ret);
        ::llvm::Error e =
            jit->AddIRModule(jd,
                             std::move(::llvm::orc::ThreadSafeModule(
                                 std::move(m), std::move(ct2))),
                             m1);
        jit->AddSymbol(jd, "test_fn", reinterpret_cast<void *>(&test_fn));
        if (e) {
            ASSERT_TRUE(false);
        }
        auto Add1Sym = FeCheck((jit->lookup(jd, "add1")));
        jit->getExecutionSession().dump(::llvm::errs());
        Add1 = (int (*)(int))Add1Sym.getAddress();
        ASSERT_EQ(Add1(1), 3);
        Add1 = NULL;
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
