/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

// HybridSe Parser
#include "parser/parser.h"
#include <utility>
#include "node/sql_node.h"
#include "proto/fe_common.pb.h"
namespace hybridse {
namespace parser {

int HybridSeParser::parse(
    const std::string &sqlstr,
    node::NodePointVector &trees,  // NOLINT (runtime/references)
    node::NodeManager *manager,
    base::Status &status) {  // NOLINT (runtime/references)
    yyscan_t scanner;
    yylex_init(&scanner);
    yy_scan_string(sqlstr.c_str(), scanner);
    yyset_lineno(1, scanner);
    yyset_column(1, scanner);
    int ret = yyparse(scanner, trees, manager, status);
    yylex_destroy(scanner);

    if (0 != status.code) {
        LOG(WARNING) << status;
        return ret;
    }

    for (node::SqlNode *tree : trees) {
        if (nullptr == tree) {
            status.code = common::kSqlError;
            status.msg = "fail to parse: parsed tree is null";
            return -1;
        }
        switch (tree->GetType()) {
            case node::kFnDef: {
                int ret = ReflectFnDefNode(
                    dynamic_cast<node::FnNodeFnDef *>(tree), manager, status);
                if (status.code != common::kOk) {
                    return ret;
                }
            }
            default: {
                // do nothing
            }
        }
    }
    return ret;
}

int HybridSeParser::ReflectFnDefNode(node::FnNodeFnDef *fn_def,
                                     node::NodeManager *node_manager,
                                     base::Status &status) {  // NOLINT
    if (nullptr == fn_def->header_) {
        status.code = common::kFunError;
        status.msg = "fail to create function def plan, first fn node is null";
        return -1;
    }
    if (nullptr == fn_def->block_ || 0 == fn_def->block_->children.size()) {
        status.code = common::kFunError;
        status.msg =
            "fail to create function def plan, function block is null or empty";
        return -1;
    }

    node::FnNodeList *fn_block = node_manager->MakeFnListNode();
    CreateFnBlock(fn_def->block_->children, 0, fn_def->block_->children.size(),
                  4, fn_block, node_manager, status);

    if (status.code != common::kOk) {
        return -1;
    }

    std::map<std::string, node::FnNode *> assign_var_map;
    if (false == SSAOptimized(fn_block, assign_var_map, status)) {
        return -1;
    }
    fn_def->block_ = fn_block;
    return 0;
}

int HybridSeParser::CreateFnBlock(std::vector<node::FnNode *> statements,
                                  int start, int end, int32_t indent,
                                  node::FnNodeList *block,
                                  node::NodeManager *node_manager,
                                  base::Status &status) {
    if (nullptr == block) {
        status.msg = "fail to create fn block node: block null";
        status.code = common::kSqlError;
        LOG(WARNING) << status;
        return -1;
    }

    int pos = start;
    node::FnNode *fn_block = nullptr;
    while (pos < end) {
        node::FnNode *node = statements[pos];
        if (nullptr == node) {
            status.msg = "fail to create fn block node: node is null";
            status.code = common::kSqlError;
            return -1;
        }
        if (indent < node->indent) {
            status.code = common::kFunError;
            status.msg = "fail to create block: fn node indent " +
                         std::to_string(node->indent) + " not match " +
                         std::to_string(indent);
            LOG(WARNING) << status << "\n" << *node;
            return -1;
        }

        if (indent > node->indent) {
            if (nullptr != fn_block) {
                block->AddChild(fn_block);
                fn_block = nullptr;
            }
            break;
        }

        pos++;
        switch (node->GetType()) {
            case node::kFnAssignStmt: {
                if (nullptr != fn_block) {
                    block->AddChild(fn_block);
                    fn_block = nullptr;
                }
                block->AddChild(node);
                break;
            }
            case node::kFnReturnStmt: {
                if (nullptr != fn_block) {
                    block->AddChild(fn_block);
                    fn_block = nullptr;
                }
                block->AddChild(node);
                return pos;
            }

            case node::kFnIfStmt: {
                if (nullptr != fn_block) {
                    block->AddChild(fn_block);
                    fn_block = nullptr;
                }

                node::FnNodeList *inner_block = node_manager->MakeFnListNode();
                pos = CreateFnBlock(statements, pos, end, node->indent + 4,
                                    inner_block, node_manager, status);
                if (status.code != common::kOk) {
                    return -1;
                }

                node::FnIfNode *if_node = dynamic_cast<node::FnIfNode *>(node);
                node::FnIfBlock *if_block =
                    node_manager->MakeFnIfBlock(if_node, inner_block);
                // start if_elif_else block
                fn_block = node_manager->MakeFnIfElseBlock(if_block, nullptr);
                break;
            }
            case node::kFnElifStmt: {
                if (nullptr == fn_block ||
                    node::kFnIfElseBlock != fn_block->GetType()) {
                    status.code = common::kFunError;
                    status.msg = "fail to create block: elif not match";
                    LOG(WARNING) << status;
                    return -1;
                }

                node::FnNodeList *inner_block = node_manager->MakeFnListNode();
                pos = CreateFnBlock(statements, pos, end, node->indent + 4,
                                    inner_block, node_manager, status);
                if (status.code != common::kOk) {
                    return -1;
                }

                node::FnElifNode *elif_node =
                    dynamic_cast<node::FnElifNode *>(node);

                node::FnElifBlock *elif_block =
                    node_manager->MakeFnElifBlock(elif_node, inner_block);
                dynamic_cast<node::FnIfElseBlock *>(fn_block)
                    ->elif_blocks_.push_back(elif_block);
                break;
            }
            case node::kFnElseStmt: {
                if (nullptr == fn_block ||
                    node::kFnIfElseBlock != fn_block->GetType()) {
                    status.code = common::kFunError;
                    status.msg = "fail to create block: else not match";
                    LOG(WARNING) << status;
                    return -1;
                }
                node::FnNodeList *inner_block = node_manager->MakeFnListNode();
                pos = CreateFnBlock(statements, pos, end, node->indent + 4,
                                    inner_block, node_manager, status);
                if (status.code != common::kOk) {
                    return -1;
                }
                node::FnElseBlock *else_block =
                    node_manager->MakeFnElseBlock(inner_block);
                dynamic_cast<node::FnIfElseBlock *>(fn_block)->else_block_ =
                    else_block;
                // end if_elif_else block
                block->AddChild(fn_block);
                fn_block = nullptr;
                break;
            }
            case node::kFnForInStmt: {
                if (nullptr != fn_block) {
                    block->AddChild(fn_block);
                }
                node::FnNodeList *inner_block = node_manager->MakeFnListNode();
                pos = CreateFnBlock(statements, pos, end, node->indent + 4,
                                    inner_block, node_manager, status);
                if (status.code != common::kOk) {
                    return -1;
                }

                node::FnForInNode *for_in_node =
                    dynamic_cast<node::FnForInNode *>(node);
                fn_block =
                    node_manager->MakeForInBlock(for_in_node, inner_block);
                // end if_elif_else block
                block->AddChild(fn_block);
                fn_block = nullptr;
                break;
            }
            default: {
                status.code = common::kFunError;
                status.msg =
                    "fail to create block, unrecognized statement type " +
                    node::NameOfSqlNodeType(node->GetType());
                LOG(WARNING) << status;
                return -1;
            }
        }
    }
    if (nullptr != fn_block) {
        block->AddChild(fn_block);
    }
    return pos;
}
bool HybridSeParser::SSAOptimized(
    const node::FnNodeList *block,
    std::map<std::string, node::FnNode *> &assign_var_map,
    base::Status &status) {
    if (nullptr == block || block->children.empty()) {
        return true;
    }

    for (node::FnNode *node : block->children) {
        if (nullptr == node) {
            // skip handle null node
            continue;
        }
        switch (node->GetType()) {
            case node::kFnAssignStmt: {
                node::FnAssignNode *assgin_node =
                    dynamic_cast<node::FnAssignNode *>(node);
                std::string var_name = assgin_node->var_->GetName();
                std::map<std::string, node::FnNode *>::iterator it =
                    assign_var_map.find(var_name);
                if (it == assign_var_map.end()) {
                    assgin_node->EnableSSA();
                    assign_var_map.insert(
                        std::pair<std::string, node::FnNode *>(var_name,
                                                               assgin_node));
                } else {
                    dynamic_cast<node::FnAssignNode *>(it->second)
                        ->DisableSSA();
                }
                break;
            }
            case node::kFnIfElseBlock: {
                node::FnIfElseBlock *block =
                    dynamic_cast<node::FnIfElseBlock *>(node);
                if (false == SSAOptimized(block->if_block_->block_,
                                          assign_var_map, status)) {
                    return false;
                }
                if (!block->elif_blocks_.empty()) {
                    for (node::FnNode *elif_block : block->elif_blocks_) {
                        if (false ==
                            SSAOptimized(
                                dynamic_cast<node::FnElifBlock *>(elif_block)
                                    ->block_,
                                assign_var_map, status)) {
                            return false;
                        }
                    }
                }

                if (nullptr != block->else_block_) {
                    if (false == SSAOptimized(block->else_block_->block_,
                                              assign_var_map, status)) {
                        return false;
                    }
                }
                break;
            }
            default: {
                break;
            }
        }
    }

    return true;
}

}  // namespace parser
}  // namespace hybridse
