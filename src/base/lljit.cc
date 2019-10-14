//===-- examples/HowToUseJIT/HowToUseJIT.cpp - An example use of the JIT --===//
//
// Part of the LLVM Project, under the Apache License v2.0 with LLVM Exceptions.
// See https://llvm.org/LICENSE.txt for license information.
// SPDX-License-Identifier: Apache-2.0 WITH LLVM-exception
//
//===----------------------------------------------------------------------===//

#include "llvm/ExecutionEngine/Orc/LLJIT.h"
#include "llvm/IR/Function.h"
#include "llvm/IR/IRBuilder.h"
#include "llvm/IR/Module.h"
#include "llvm/IR/InstrTypes.h"
#include "llvm/Support/InitLLVM.h"
#include "llvm/Support/TargetSelect.h"
#include "llvm/Support/raw_ostream.h"
#include "flatbuffers/flatbuffers.h"

using namespace llvm;
using namespace llvm::orc;

ExitOnError ExitOnErr;

::flatbuffers::Offset<::flatbuffers::Table> Decode(flatbuffers::FlatBufferBuilder& builder) {
    // decode a struct
    ::flatbuffers::uoffset_t start = builder.StartTable();
    builder.AddElement<float>(4, 1.0f, 0.0f);
    builder.AddElement<int32_t>(6, 1, 0);
    ::flatbuffers::uoffset_t end = builder.EndTable(start);
    return flatbuffers::Offset<flatbuffers::Table>(end);
}
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

int main(int argc, char *argv[]) {
  // Initialize LLVM.
  InitLLVM X(argc, argv);
  InitializeNativeTarget();
  InitializeNativeTargetAsmPrinter();
  cl::ParseCommandLineOptions(argc, argv, "HowToUseLLJIT");
  ExitOnErr.setBanner(std::string(argv[0]) + ": ");
  // Create an LLJIT instance.
  auto J = ExitOnErr(LLJITBuilder().create());
  auto M = createDemoModule();
  ExitOnErr(J->addIRModule(std::move(M)));
  // Look up the JIT'd function, cast it to a function pointer, then call it.
  auto Add1Sym = ExitOnErr(J->lookup("decode"));
  float (*decode)(int8_t*) = (float (*)(int8_t*))Add1Sym.getAddress();
  flatbuffers::FlatBufferBuilder fb_;
  ::flatbuffers::Offset<::flatbuffers::Table> table = Decode(fb_);
  fb_.Finish(table);
  int8_t* ptr = (int8_t*)fb_.GetBufferPointer();
  float Result = decode(ptr);
  outs() << "decode = " << Result << "\n";
  return 0;
}
