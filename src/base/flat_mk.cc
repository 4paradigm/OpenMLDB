/*
 * flat_mk.cc
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

#include "benchmark/benchmark.h"
#include "flatbuffers/flatbuffers.h"
#include "jit/jit.h"
#include "jit/jit-dump.h"
#include <iostream>
#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "llvm/IR/LegacyPassManager.h"
#include "llvm/Transforms/InstCombine/InstCombine.h"
#include "llvm/Transforms/AggressiveInstCombine/AggressiveInstCombine.h"
#include "llvm/Transforms/Scalar.h"
#include "llvm/Transforms/Scalar/GVN.h"

using namespace llvm;
using namespace llvm::orc;

ExitOnError ExitOnErr;

ThreadSafeModule createDemoModule() {
    auto ctx = llvm::make_unique<LLVMContext>();
    auto m = make_unique<Module>("test", *ctx);

    // Create the add1 function entry and insert this entry into module M.  The
    // function will have a return type of "int" and take an argument of "int".
    Function *decode =
      Function::Create(FunctionType::get(Type::getFloatTy(*ctx),
                                         {Type::getInt8PtrTy(*ctx)}, 
                                         false),
                       Function::ExternalLinkage, "decode", m.get());

    // Add a basic block to the function. As before, it automatically inserts
    // because of the last argument.
    BasicBlock *entry_block = BasicBlock::Create(*ctx, "EntryBlock", decode);

    // Create a basic block builder with default parameters.  The builder will
    // automatically append instructions to the basic block `BB'.
    IRBuilder<> builder(entry_block);

    // Get pointers to the integer argument of the add1 function...
    assert(decode->arg_begin() != decode->arg_end()); // Make sure there's an arg
    Argument *arg0 = &*decode->arg_begin();          // Get the arg
    arg0->setName("void_pointer"); // Give it a nice symbolic name for fun.
    IntegerType* int32_type = Type::getInt32Ty(*ctx);
    IntegerType* int16_type = Type::getInt16Ty(*ctx);
    PointerType* int32_type_ptr = Type::getInt32PtrTy(*ctx);
    PointerType* float_type_ptr = Type::getFloatPtrTy(*ctx);
    Type* float_type = Type::getFloatTy(*ctx);
    PointerType* int16_type_ptr = Type::getInt16PtrTy(*ctx);
    // Create the add instruction, inserting it into the end of BB.
    Value* int32_ptr = builder.CreatePointerCast(arg0, int32_type_ptr, "uint8_to_int32");
    LoadInst *table_start = builder.CreateLoad(int32_type, int32_ptr, "table_start");
    //table_start->getType()->dump();
    // arg0->getType()->dump();
    Value* arg0_int32 = builder.CreatePtrToInt(arg0, int32_type);
    Value* table_start_ptr = builder.CreateAdd(arg0_int32, table_start, "table_start_ptr");
    Value* table_start_ptr_int32 = builder.CreateIntToPtr(table_start_ptr, int32_type_ptr);
    Value* vtable_offset_relative = builder.CreateLoad(int32_type, table_start_ptr_int32, "load_vtable_offset");
    Value* vtable_start = builder.CreateSub(table_start, vtable_offset_relative, "vtable_start");
    Value *four = builder.getInt32(4);
    Value* add_four_vtable_start = builder.CreateAdd(vtable_start, four, "add_four");
    // Create the return instruction and add it to the basic block
    Value* float_field_voffset_ptr = builder.CreateAdd(arg0_int32, add_four_vtable_start, "float_field");
    Value* float_field_voffset_ptr_int16_ptr = builder.CreateIntToPtr(float_field_voffset_ptr, int16_type_ptr);
    Value* float_field_voffset = builder.CreateLoad(int16_type, float_field_voffset_ptr_int16_ptr, "float_offset");
    Value* float_field_voffset_int32 = builder.CreateIntCast(float_field_voffset, int32_type, true, "cast_16_to_32");
    Value* table_start_add_float_offset = builder.CreateAdd(float_field_voffset_int32, table_start, "add_float_offset");
    Value* float_start_offset_ptr = builder.CreateAdd(arg0_int32, table_start_add_float_offset, "add_offset");
    Value* float_value = builder.CreateLoad(float_type, builder.CreateIntToPtr(float_start_offset_ptr, float_type_ptr), "load_float");
    builder.CreateRet(float_value);
    // m->dump();
    return ThreadSafeModule(std::move(m), std::move(ctx));
}

::flatbuffers::Offset<::flatbuffers::Table> Decode(flatbuffers::FlatBufferBuilder& builder) {
    // decode a struct
    ::flatbuffers::uoffset_t start = builder.StartTable();
    builder.AddElement<float>(4, 1.0f, 0.0f);
    builder.AddElement<int32_t>(6, 1, 0);
    ::flatbuffers::uoffset_t end = builder.EndTable(start);
    return flatbuffers::Offset<flatbuffers::Table>(end);
}

class AccessFixture : public benchmark::Fixture {

public:
    void SetUp(const ::benchmark::State& state) {
    }

    void TearDown(const ::benchmark::State& state) {
    }

private:
};
BENCHMARK_F(AccessFixture, DecodeTest)(benchmark::State& st) {
    for (auto _ : st) {
        flatbuffers::FlatBufferBuilder fb_;
        ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
        fb_.Finish(table);
    }
}

BENCHMARK_F(AccessFixture, InitTest)(benchmark::State& st) {
    flatbuffers::FlatBufferBuilder fb_;
    ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
    fb_.Finish(table);
    for (auto _ : st) {
        uint8_t* ptr = fb_.GetBufferPointer();
        ::flatbuffers::GetRoot<::flatbuffers::Table>((void*)ptr);
    }
}


BENCHMARK_F(AccessFixture, AccessInt)(benchmark::State& st) {
    flatbuffers::FlatBufferBuilder fb_;
    ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
    fb_.Finish(table);
    uint8_t* ptr = fb_.GetBufferPointer();
    const ::flatbuffers::Table* t = ::flatbuffers::GetRoot<::flatbuffers::Table>((void*)ptr);
    int32_t* i = new int32_t[1];

    for (auto _ : st) {
        i[0] = t->GetField<int32_t>(6, 0);
    }

}

BENCHMARK_F(AccessFixture, AccessFloat)(benchmark::State& st) {
    flatbuffers::FlatBufferBuilder fb_;
    ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
    fb_.Finish(table);
    uint8_t* ptr = fb_.GetBufferPointer();
    const ::flatbuffers::Table* t = ::flatbuffers::GetRoot<::flatbuffers::Table>((void*)ptr);
    float* f = new float[1];
    for (auto _ : st) {
        f[0] = t->GetField<float>(4, 0.0f);
    }
}

BENCHMARK_F(AccessFixture, ManualAccessFloat)(benchmark::State& st) {
    flatbuffers::FlatBufferBuilder fb_;
    ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
    fb_.Finish(table);
    uint8_t* ptr = fb_.GetBufferPointer();
    float* f = new float[1];
    for (auto _ : st) {
        ::flatbuffers::uoffset_t table_start = *((::flatbuffers::uoffset_t*)ptr);
        ::flatbuffers::uoffset_t vtable_start = table_start - *((::flatbuffers::soffset_t*)(ptr + table_start));
        ::flatbuffers::voffset_t float_field_voffset = *((::flatbuffers::voffset_t*)(ptr + vtable_start + 4));
        f[0] = *(float*)(ptr + table_start + float_field_voffset);
    }
}


BENCHMARK_F(AccessFixture, JITAccessFloat)(benchmark::State& st) {
    flatbuffers::FlatBufferBuilder fb_;
    ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
    fb_.Finish(table);
    jit_context_t ctx = jit_context_create();
    jit_context_build_start(ctx);
    jit_type_t args[1];
    args[0] = jit_type_void_ptr;
    jit_type_t signature = jit_type_create_signature(jit_abi_cdecl,
                           jit_type_float32, args, 1, 1);

    jit_function_t fn = jit_function_create(ctx, signature);
    jit_type_free(signature);
    jit_value_t input = jit_value_get_param(fn, 0);
    uint32_t zero_offset = 0;

    jit_value_t table_offset = jit_insn_load_relative(fn, 
            input, zero_offset, jit_type_uint);

    jit_value_t table_start_ptr = jit_insn_add(fn, input, table_offset);
    jit_value_t vtable_offset_relative = jit_insn_load_relative(fn, table_start_ptr, 0, jit_type_int);
    jit_value_t vtable_offset = jit_insn_convert(fn, jit_insn_sub(fn, table_offset, vtable_offset_relative), jit_type_uint,0);
    jit_value_t float_field_ptr = jit_insn_add(fn, input, vtable_offset);
    jit_value_t float_field_voffset = jit_insn_load_relative(fn, float_field_ptr, 4, jit_type_ushort);
    jit_value_t float_ptr = jit_insn_add(fn, table_start_ptr, float_field_voffset);
    jit_value_t float_val = jit_insn_load_relative(fn, float_ptr, 0, jit_type_float32);
    jit_insn_return(fn, float_val);
    jit_function_set_optimization_level(fn, jit_function_get_max_optimization_level());
    int ret = jit_function_compile(fn);
    if (ret == 0) {
        std::cout << "compile error" << std::endl;
        return;
    }
    typedef float(*fn_entry)(void*);
    fn_entry entry = reinterpret_cast<float(*)(void*)>(jit_function_to_closure(fn));
    jit_context_build_end(ctx);
    uint8_t* ptr = fb_.GetBufferPointer();
    float* output = new float[1];
    output[0] = entry((void*)ptr);
    for (auto _ : st) {
        output[0] = entry((void*)ptr);
    }
}

BENCHMARK_F(AccessFixture, LLVM_NO_OPT_AccessFloat)(benchmark::State& st) {
    flatbuffers::FlatBufferBuilder fb_;
    ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
    fb_.Finish(table);
    InitializeNativeTarget();
    InitializeNativeTargetAsmPrinter();
    // Create an LLJIT instance.
    auto J = ExitOnErr(LLJITBuilder().create());
    auto M = createDemoModule();
    M.getModule()->setDataLayout(J->getDataLayout());
    std::unique_ptr<llvm::legacy::FunctionPassManager> theFPM = llvm::make_unique<llvm::legacy::FunctionPassManager>(M.getModule());

    //InstructionCombining - Combine instructions to form fewer, simple instructions.
    theFPM->add(llvm::createInstructionCombiningPass());

    //Reassociate - This pass reassociates commutative expressions in an order that
    //is designed to promote better constant propagation
    //For example:  4 + (x + 5)  ->  x + (4 + 5)
    theFPM->add(llvm::createReassociatePass());

    //Create a legacy GVN pass. This also allows parameterizing whether or not loads are eliminated by the pass.
    theFPM->add(llvm::createGVNPass());

    //CFGSimplification - Merge basic blocks, eliminate unreachable blocks,
    // simplify terminator instructions, convert switches to lookup tables, etc.
    theFPM->add(llvm::createCFGSimplificationPass());
    theFPM->add(llvm::createAggressiveInstCombinerPass());

    theFPM->doInitialization();

    ExitOnErr(J->addIRModule(std::move(M)));
    // Look up the JIT'd function, cast it to a function pointer, then call it.
    auto decode_symble = ExitOnErr(J->lookup("decode"));
    float (*decode)(int8_t*) = (float (*)(int8_t*))decode_symble.getAddress();
    float* output = new float[1];
    int8_t* ptr = (int8_t*)fb_.GetBufferPointer();
    output[0] = decode(ptr);

    for (auto _ : st) {
        output[0] = decode(ptr);
    }
}


BENCHMARK_MAIN();
