/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * udf_ir_builder.cc
 *
 * Author: chenjing
 * Date: 2020/6/17
 *--------------------------------------------------------------------------
 **/
#include "codegen/udf_ir_builder.h"
#include <fstream>
#include <iostream>
#include <utility>
#include <vector>
#include "boost/filesystem.hpp"
#include "boost/filesystem/string_file.hpp"
#include "codegen/block_ir_builder.h"
#include "codegen/date_ir_builder.h"
#include "codegen/fn_ir_builder.h"
#include "codegen/timestamp_ir_builder.h"
#include "node/node_manager.h"
#include "node/sql_node.h"
#include "parser/parser.h"
#include "plan/planner.h"
#include "udf/udf.h"
DECLARE_string(native_fesql_libs_path);
namespace fesql {
namespace codegen {
bool UDFIRBuilder::BuildMinuteTimestamp(::llvm::Module* module,
                                        base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("time", nm.MakeTypeNode(fesql::node::kTimestamp)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "minute", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: minute(timestamp): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* time = &(*iter);

    codegen::TimestampIRBuilder timestamp_ir_builder(module);
    ::llvm::Value* ret;
    if (!timestamp_ir_builder.Minute(block, time, &ret, status)) {
        LOG(WARNING) << "Fail build udf: minute(timestamp): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildHourTimestamp(::llvm::Module* module,
                                      base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("time", nm.MakeTypeNode(fesql::node::kTimestamp)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "hour", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: hour(timestamp): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* time = &(*iter);

    codegen::TimestampIRBuilder timestamp_ir_builder(module);
    ::llvm::Value* ret;
    if (!timestamp_ir_builder.Hour(block, time, &ret, status)) {
        LOG(WARNING) << "Fail build udf: hour(timestamp): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildSecondTimestamp(::llvm::Module* module,
                                        base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("time", nm.MakeTypeNode(fesql::node::kTimestamp)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "second", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: second(timestamp): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* time = &(*iter);

    codegen::TimestampIRBuilder timestamp_ir_builder(module);
    ::llvm::Value* ret;
    if (!timestamp_ir_builder.Second(block, time, &ret, status)) {
        LOG(WARNING) << "Fail build udf: second(timestamp): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildMinuteInt64(::llvm::Module* module,
                                    base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("time", nm.MakeTypeNode(fesql::node::kInt64)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "minute", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: minute(int64): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* time = &(*iter);

    codegen::TimestampIRBuilder timestamp_ir_builder(module);
    ::llvm::Value* ret;
    if (!timestamp_ir_builder.Minute(block, time, &ret, status)) {
        LOG(WARNING) << "Fail build udf: minute(int64): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildHourInt64(::llvm::Module* module,
                                  base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("time", nm.MakeTypeNode(fesql::node::kInt64)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "hour", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: hour(int64): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* time = &(*iter);

    codegen::TimestampIRBuilder timestamp_ir_builder(module);
    ::llvm::Value* ret;
    if (!timestamp_ir_builder.Hour(block, time, &ret, status)) {
        LOG(WARNING) << "Fail build udf: hour(int64): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildSecondInt64(::llvm::Module* module,
                                    base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("time", nm.MakeTypeNode(fesql::node::kInt64)));
    auto header = dynamic_cast<node::FnNodeFnHeander*>(nm.MakeFnHeaderNode(
        "second", fn_args, nm.MakeTypeNode(fesql::node::kInt32)));

    ScopeVar sv;
    sv.Enter("module");
    ::llvm::Function* fn;

    if (!fn_ir_builder.BuildFnHead(header, &sv, &fn, status)) {
        LOG(WARNING) << "Fail build udf: second(int64): " << status.msg;
        return false;
    }
    auto block = ::llvm::BasicBlock::Create(module->getContext(), "entry", fn);
    auto iter = fn->arg_begin();
    ::llvm::Value* time = &(*iter);

    codegen::TimestampIRBuilder timestamp_ir_builder(module);
    ::llvm::Value* ret;
    if (!timestamp_ir_builder.Second(block, time, &ret, status)) {
        LOG(WARNING) << "Fail build udf: second(int64): " << status.msg;
        return false;
    }
    ::llvm::IRBuilder<> builder(block);
    builder.CreateRet(ret);
    return true;
}
bool UDFIRBuilder::BuildDayDate(::llvm::Module* module, base::Status& status) {
    codegen::FnIRBuilder fn_ir_builder(module);
    node::NodeManager nm;
    auto fn_args = nm.MakeFnListNode();
    fn_args->AddChild(
        nm.MakeFnParaNode("date", nm.MakeTypeNode(fesql::node::kDate)));
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
    fn_args->AddChild(
        nm.MakeFnParaNode("date", nm.MakeTypeNode(fesql::node::kDate)));
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
    fn_args->AddChild(
        nm.MakeFnParaNode("date", nm.MakeTypeNode(fesql::node::kDate)));
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
bool UDFIRBuilder::BuildUDF(::llvm::Module* module, base::Status& status) {
    return BuildDayDate(module, status) && BuildMonthDate(module, status) &&
           BuildYearDate(module, status) &&
           BuildMinuteTimestamp(module, status) &&
           BuildHourTimestamp(module, status) &&
           BuildSecondTimestamp(module, status) &&
           BuildMinuteInt64(module, status) && BuildHourInt64(module, status) &&
           BuildSecondInt64(module, status);
}
bool UDFIRBuilder::BuildNativeCUDF(::llvm::Module* module,
                                   fesql::node::FnNodeFnHeander* header,
                                   void* fn_ptr, base::Status& status) {
    if (nullptr == name_function_map_) {
        LOG(WARNING) << "Fail Build Native UDF: name and functin map is null";
        return false;
    }
    codegen::FnIRBuilder fn_ir_builder(module);
    ::llvm::Function* fn;
    if (!fn_ir_builder.CreateFunction(header, &fn, status)) {
        LOG(WARNING) << "Fail to register native udf: "
                     << header->GeIRFunctionName();
        return false;
    }

    if (name_function_map_->find(fn->getName().str()) !=
        name_function_map_->cend()) {
        return false;
    }
    name_function_map_->insert(std::make_pair(fn->getName().str(), fn_ptr));
    DLOG(INFO) << "register native udf: " << fn->getName().str();
    return true;
}
bool UDFIRBuilder::GetLibsFiles(const std::string& dir_path,
                                std::vector<std::string>& filenames,
                                Status& status) {
    boost::filesystem::path path(dir_path);
    if (!boost::filesystem::exists(path)) {
        status.msg = "Libs path " + dir_path + " not exist";
        status.code = common::kSQLError;
        return false;
    }
    boost::filesystem::directory_iterator end_iter;
    for (boost::filesystem::directory_iterator iter(path); iter != end_iter;
         ++iter) {
        if (boost::filesystem::is_regular_file(iter->status())) {
            filenames.push_back(iter->path().string());
        }

        if (boost::filesystem::is_directory(iter->status())) {
            if (!GetLibsFiles(iter->path().string(), filenames, status)) {
                return false;
            }
        }
    }
    return true;
}
bool UDFIRBuilder::CompileFeScript(llvm::Module* m, const std::string& path_str,
                                   base::Status& status) {
    boost::filesystem::path path(path_str);
    std::string script;
    boost::filesystem::load_string_file(path, script);
    DLOG(INFO) << "Script file : " << script << "\n" << script;
    ::fesql::node::NodeManager node_mgr;
    ::fesql::node::NodePointVector parser_trees;
    ::fesql::parser::FeSQLParser parser;
    ::fesql::plan::SimplePlanner planer(&node_mgr);
    ::fesql::node::PlanNodeList plan_trees;

    int ret = parser.parse(script, parser_trees, &node_mgr, status);
    if (ret != 0) {
        LOG(WARNING) << "fail to parse sql " << script << " with error "
                     << status.msg;
        return false;
    }

    ret = planer.CreatePlanTree(parser_trees, plan_trees, status);
    if (ret != 0) {
        LOG(WARNING) << "Fail create sql plan: " << status.msg;
        return false;
    }

    auto it = plan_trees.begin();
    for (; it != plan_trees.end(); ++it) {
        const ::fesql::node::PlanNode* node = *it;
        if (nullptr == node) {
            status.msg = "Fail compile null plan";
            LOG(WARNING) << status.msg;
            return false;
        }
        switch (node->GetType()) {
            case ::fesql::node::kPlanTypeFuncDef: {
                const ::fesql::node::FuncDefPlanNode* func_def_plan =
                    dynamic_cast<const ::fesql::node::FuncDefPlanNode*>(node);
                if (nullptr == m || nullptr == func_def_plan ||
                    nullptr == func_def_plan->fn_def_) {
                    status.msg =
                        "fail to codegen function: module or fn_def node is "
                        "null";
                    status.code = common::kOpGenError;
                    LOG(WARNING) << status.msg;
                    return false;
                }

                ::fesql::codegen::FnIRBuilder builder(m);
                bool ok = builder.Build(func_def_plan->fn_def_, status);
                if (!ok) {
                    LOG(WARNING) << status.msg;
                    return false;
                }
                break;
            }
            default: {
                status.msg =
                    "fail to codegen fe script: unrecognized plan type " +
                    node::NameOfPlanNodeType(node->GetType());
                status.code = common::kCodegenError;
                LOG(WARNING) << status.msg;
                return false;
            }
        }
    }
    return true;
}
bool UDFIRBuilder::BuildFeLibs(llvm::Module* m, Status& status) {
    std::vector<std::string> filepaths;
    if (!GetLibsFiles(FLAGS_native_fesql_libs_path, filepaths, status)) {
        return false;
    }
    std::ostringstream runner_oss;
    runner_oss << "Libs:\n";
    for_each(filepaths.cbegin(), filepaths.cend(),
             [&](const std::string item) { runner_oss << item << "\n"; });
    DLOG(INFO) << runner_oss.str();

    for (auto path_str : filepaths) {
        if (!CompileFeScript(m, path_str, status)) {
            LOG(WARNING) << status.msg;
            return false;
        }
    }
    return true;
}

}  // namespace codegen
}  // namespace fesql
