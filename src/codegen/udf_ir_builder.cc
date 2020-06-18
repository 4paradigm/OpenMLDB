/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/
#include "codegen/udf_ir_builder.h"
#include "codegen/block_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
namespace fesql {
namespace codegen {
bool UDFIRBuilder::BuildDayDate(::llvm::Module* module, base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(nm.MakeFnParaNode(
        "date", nm.MakeTypeNode(fesql::node::kPointer, fesql::node::kDate)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "day", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: day(date): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* date = &(*iter);

    codegen::DateIRBuilder date_ir_builder(module);
    ::llvm::Value* ret;
    if (!date_ir_builder.Day(block, date, &ret, status)) {
        LOG(WARNING) << "Fail build udf: day(date): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}

bool UDFIRBuilder::BuildMonthDate(::llvm::Module* module,
                                  base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(nm.MakeFnParaNode(
        "date", nm.MakeTypeNode(fesql::node::kPointer, fesql::node::kDate)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "month", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: month(date): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* date = &(*iter);

    codegen::DateIRBuilder date_ir_builder(module);
    ::llvm::Value* ret;
    if (!date_ir_builder.Month(block, date, &ret, status)) {
        LOG(WARNING) << "Fail build udf: month(date): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}

bool UDFIRBuilder::BuildYearDate(::llvm::Module* module, base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(nm.MakeFnParaNode(
        "date", nm.MakeTypeNode(fesql::node::kPointer, fesql::node::kDate)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "year", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: year(date): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* date = &(*iter);

    codegen::DateIRBuilder date_ir_builder(module);
    ::llvm::Value* ret;
    if (!date_ir_builder.Year(block, date, &ret, status)) {
        LOG(WARNING) << "Fail build udf: year(date): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildTimeUDF(::llvm::Module* module, base::Status& status) {
    return BuildDayDate(module, status) && BuildMonthDate(module, status) &&
           BuildYearDate(module, status);
}
}  // namespace codegen
}  // namespace fesql
