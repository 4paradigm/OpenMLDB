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

#include "vm/runner.h"
#include <memory>
#include <string>
#include <utility>
#include <vector>
#include "base/texttable.h"
#include "udf/udf.h"
#include "vm/catalog_wrapper.h"
#include "vm/core_api.h"
#include "vm/jit_runtime.h"
#include "vm/mem_catalog.h"

namespace hybridse {
namespace vm {
#define MAX_DEBUG_BATCH_SiZE 5
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_MAX 20

// Build Runner for each physical node
// return cluster task of given runner
//
// DataRunner(kProviderTypePartition) --> cluster task
// RequestRunner --> local task
// DataRunner(kProviderTypeTable) --> LocalTask, Unsupport in distribute
// database
//
// SimpleProjectRunner --> inherit task
// TableProjectRunner --> inherit task
// WindowAggRunner --> LocalTask , Unsupport in distribute database
// GroupAggRunner --> LocalTask, Unsupport in distribute database
//
// RowProjectRunner --> inherit task
// ConstProjectRunner --> local task
//
// RequestUnionRunner
//      --> complete route_info of right cluster task
//      --> build proxy runner if need
// RequestJoinRunner
//      --> complete route_info of right cluster task
//      --> build proxy runner if need
// kPhysicalOpJoin
//      --> kJoinTypeLast->RequestJoinRunner
//              --> complete route_info of right cluster task
//              --> build proxy runner if need
//      --> kJoinTypeConcat
//              --> build proxy runner if need
// kPhysicalOpPostRequestUnion
//      --> build proxy runner if need
// GroupRunner --> LocalTask, Unsupport in distribute database
// kPhysicalOpFilter
// kPhysicalOpLimit
// kPhysicalOpRename
ClusterTask RunnerBuilder::Build(PhysicalOpNode* node, Status& status) {
    auto fail = InvalidTask();
    if (nullptr == node) {
        status.msg = "fail to build runner : physical node is null";
        status.code = common::kOpGenError;
        LOG(WARNING) << status;
        return fail;
    }
    auto iter = task_map_.find(node);
    if (iter != task_map_.cend()) {
        iter->second.GetRoot()->EnableCache();
        return iter->second;
    }
    switch (node->GetOpType()) {
        case kPhysicalOpDataProvider: {
            auto op = dynamic_cast<const PhysicalDataProviderNode*>(node);
            switch (op->provider_type_) {
                case kProviderTypeTable: {
                    auto provider =
                        dynamic_cast<const PhysicalTableProviderNode*>(node);
                    DataRunner* runner = nullptr;
                    CreateRunner<DataRunner>(&runner, id_++,
                                             node->schemas_ctx(),
                                             provider->table_handler_);
                    return RegisterTask(node, CommonTask(runner));
                }
                case kProviderTypePartition: {
                    auto provider =
                        dynamic_cast<const PhysicalPartitionProviderNode*>(
                            node);
                    DataRunner* runner = nullptr;
                    CreateRunner<DataRunner>(
                        &runner, id_++, node->schemas_ctx(),
                        provider->table_handler_->GetPartition(
                            provider->index_name_));
                    if (support_cluster_optimized_) {
                        return RegisterTask(
                            node, UnCompletedClusterTask(
                                      runner, provider->table_handler_,
                                      provider->index_name_));
                    } else {
                        return RegisterTask(node, CommonTask(runner));
                    }
                }
                case kProviderTypeRequest: {
                    RequestRunner* runner = nullptr;
                    CreateRunner<RequestRunner>(&runner, id_++,
                                                node->schemas_ctx());
                    return RegisterTask(node, BuildRequestTask(runner));
                }
                default: {
                    status.msg = "fail to support data provider type " +
                                 DataProviderTypeName(op->provider_type_);
                    status.code = common::kOpGenError;
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpSimpleProject: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalSimpleProjectNode*>(node);
            int select_slice = op->GetSelectSourceIndex();
            if (select_slice >= 0) {
                SelectSliceRunner* runner = nullptr;
                CreateRunner<SelectSliceRunner>(
                    &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                    select_slice);
                return RegisterTask(node,
                                    UnaryInheritTask(cluster_task, runner));
            } else {
                SimpleProjectRunner* runner = nullptr;
                CreateRunner<SimpleProjectRunner>(
                    &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                    op->project().fn_info());
                return RegisterTask(node,
                                    UnaryInheritTask(cluster_task, runner));
            }
        }
        case kPhysicalOpConstProject: {
            auto op = dynamic_cast<const PhysicalConstProjectNode*>(node);
            ConstProjectRunner* runner = nullptr;
            CreateRunner<ConstProjectRunner>(
                &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                op->project().fn_info());
            return RegisterTask(node, CommonTask(runner));
        }
        case kPhysicalOpProject: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto input = cluster_task.GetRoot();
            auto op = dynamic_cast<const PhysicalProjectNode*>(node);
            switch (op->project_type_) {
                case kTableProject: {
                    if (support_cluster_optimized_) {
                        // 边界检查, 分布式计划暂时不支持表拼接
                        status.msg = "fail to build cluster with table project";
                        status.code = common::kOpGenError;
                        LOG(WARNING) << status;
                        return fail;
                    }
                    TableProjectRunner* runner = nullptr;
                    CreateRunner<TableProjectRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->project().fn_info());
                    return RegisterTask(node,
                                        UnaryInheritTask(cluster_task, runner));
                }
                case kAggregation: {
                    AggRunner* runner = nullptr;
                    CreateRunner<AggRunner>(&runner, id_++, node->schemas_ctx(),
                                            op->GetLimitCnt(),
                                            op->project().fn_info());
                    return RegisterTask(node,
                                        UnaryInheritTask(cluster_task, runner));
                }
                case kGroupAggregation: {
                    if (support_cluster_optimized_) {
                        // 边界检查, 分布式计划暂时不支持表拼接
                        status.msg =
                            "fail to build cluster with group agg project";
                        status.code = common::kOpGenError;
                        LOG(WARNING) << status;
                        return fail;
                    }
                    auto op =
                        dynamic_cast<const PhysicalGroupAggrerationNode*>(node);
                    GroupAggRunner* runner = nullptr;
                    CreateRunner<GroupAggRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->group_, op->project().fn_info());
                    return RegisterTask(node,
                                        UnaryInheritTask(cluster_task, runner));
                }
                case kWindowAggregation: {
                    if (support_cluster_optimized_) {
                        // 边界检查, 分布式计划暂时不支持表滑动窗口聚合
                        status.msg =
                            "fail to build cluster with window agg project";
                        status.code = common::kOpGenError;
                        LOG(WARNING) << status;
                        return fail;
                    }
                    auto op =
                        dynamic_cast<const PhysicalWindowAggrerationNode*>(
                            node);
                    WindowAggRunner* runner = nullptr;
                    CreateRunner<WindowAggRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->window_, op->project().fn_info(),
                        op->instance_not_in_window(),
                        op->exclude_current_time(), op->need_append_input());
                    size_t input_slices =
                        input->output_schemas()->GetSchemaSourceSize();
                    if (!op->window_unions_.Empty()) {
                        for (auto window_union :
                             op->window_unions_.window_unions_) {
                            auto union_task = Build(window_union.first, status);
                            auto union_table = union_task.GetRoot();
                            if (nullptr == union_table) {
                                return RegisterTask(node, fail);
                            }
                            runner->AddWindowUnion(window_union.second,
                                                   union_table);
                        }
                    }
                    if (!op->window_joins_.Empty()) {
                        for (auto& window_join :
                             op->window_joins_.window_joins_) {
                            auto join_task =  // NOLINT
                                Build(window_join.first, status);
                            auto join_right_runner = join_task.GetRoot();
                            if (nullptr == join_right_runner) {
                                return RegisterTask(node, fail);
                            }
                            runner->AddWindowJoin(window_join.second,
                                                  input_slices,
                                                  join_right_runner);
                        }
                    }
                    return RegisterTask(node,
                                        UnaryInheritTask(cluster_task, runner));
                }
                case kRowProject: {
                    RowProjectRunner* runner = nullptr;
                    CreateRunner<RowProjectRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->project().fn_info());
                    return RegisterTask(node,
                                        UnaryInheritTask(cluster_task, runner));
                }
                default: {
                    status.msg = "fail to support project type " +
                                 ProjectTypeName(op->project_type_);
                    status.code = common::kOpGenError;
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpRequestUnion: {
            auto left_task = Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right_task = Build(node->producers().at(1), status);
            auto right = right_task.GetRoot();
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalRequestUnionNode*>(node);
            RequestUnionRunner* runner = nullptr;
            CreateRunner<RequestUnionRunner>(
                &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                op->window().range_, op->exclude_current_time(),
                op->output_request_row());
            Key index_key;
            if (!op->instance_not_in_window()) {
                runner->AddWindowUnion(op->window_, right);
                index_key = op->window_.index_key_;
            }
            if (!op->window_unions_.Empty()) {
                for (auto window_union : op->window_unions_.window_unions_) {
                    auto union_task = Build(window_union.first, status);
                    auto union_table = union_task.GetRoot();
                    if (nullptr == union_table) {
                        return RegisterTask(node, fail);
                    }
                    runner->AddWindowUnion(window_union.second, union_table);
                    if (!index_key.ValidKey()) {
                        index_key = window_union.second.index_key_;
                        right_task = union_task;
                        right_task.SetRoot(right);
                    } else if (support_cluster_optimized_) {
                        LOG(WARNING) << "Fail to build RequestUnionRunner with "
                                        "multi union table in cluster mode";
                        return ClusterTask();
                    }
                }
            }
            return RegisterTask(
                node, BinaryInherit(left_task, right_task, runner, index_key,
                                    kRightBias));
        }
        case kPhysicalOpRequestJoin: {
            auto left_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto left = left_task.GetRoot();
            auto right_task =  // NOLINT
                Build(node->producers().at(1), status);
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right = right_task.GetRoot();
            auto op = dynamic_cast<const PhysicalRequestJoinNode*>(node);
            switch (op->join().join_type()) {
                case node::kJoinTypeLast: {
                    RequestLastJoinRunner* runner = nullptr;
                    CreateRunner<RequestLastJoinRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->join_,
                        left->output_schemas()->GetSchemaSourceSize(),
                        right->output_schemas()->GetSchemaSourceSize(),
                        op->output_right_only());

                    return RegisterTask(
                        node, BinaryInherit(left_task, right_task, runner,
                                            op->join().index_key(), kLeftBias));
                }
                case node::kJoinTypeConcat: {
                    ConcatRunner* runner = nullptr;
                    CreateRunner<ConcatRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt());
                    return RegisterTask(
                        node, BinaryInherit(left_task, right_task, runner,
                                            Key(), kNoBias));
                }
                default: {
                    status.code = common::kOpGenError;
                    status.msg = "can't handle join type " +
                                 node::JoinTypeName(op->join().join_type());
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpJoin: {
            auto left_task = Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto left = left_task.GetRoot();
            auto right_task = Build(node->producers().at(1), status);
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right = right_task.GetRoot();
            auto op = dynamic_cast<const PhysicalJoinNode*>(node);
            switch (op->join().join_type()) {
                case node::kJoinTypeLast: {
                    // 分布式模式下, TableLastJoin convert to
                    // Batch Request RequestLastJoin
                    if (support_cluster_optimized_) {
                        RequestLastJoinRunner* runner = nullptr;
                        CreateRunner<RequestLastJoinRunner>(
                            &runner, id_++, node->schemas_ctx(),
                            op->GetLimitCnt(), op->join_,
                            left->output_schemas()->GetSchemaSourceSize(),
                            right->output_schemas()->GetSchemaSourceSize(),
                            op->output_right_only_);
                        return RegisterTask(
                            node,
                            BinaryInherit(left_task, right_task, runner,
                                          op->join().index_key(), kLeftBias));
                    } else {
                        LastJoinRunner* runner = nullptr;
                        CreateRunner<LastJoinRunner>(
                            &runner, id_++, node->schemas_ctx(),
                            op->GetLimitCnt(), op->join_,
                            left->output_schemas()->GetSchemaSourceSize(),
                            right->output_schemas()->GetSchemaSourceSize());
                        return RegisterTask(
                            node, BinaryInherit(left_task, right_task, runner,
                                                Key(), kLeftBias));
                    }
                }
                case node::kJoinTypeConcat: {
                    ConcatRunner* runner = nullptr;
                    CreateRunner<ConcatRunner>(
                        &runner, id_++, node->schemas_ctx(), op->GetLimitCnt());
                    return RegisterTask(
                        node, BinaryInherit(left_task, right_task, runner,
                                            op->join().index_key(), kNoBias));
                }
                default: {
                    status.code = common::kOpGenError;
                    status.msg = "can't handle join type " +
                                 node::JoinTypeName(op->join().join_type());
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpGroupBy: {
            if (support_cluster_optimized_) {
                // 边界检查, 分布式计划暂时不支持表分组处理
                status.msg = "fail to build cluster with group by node";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto cluster_task = Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            GroupRunner* runner = nullptr;
            CreateRunner<GroupRunner>(&runner, id_++, node->schemas_ctx(),
                                      op->GetLimitCnt(), op->group());
            return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
        }
        case kPhysicalOpFilter: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalFilterNode*>(node);
            FilterRunner* runner = nullptr;
            CreateRunner<FilterRunner>(&runner, id_++, node->schemas_ctx(),
                                       op->GetLimitCnt(), op->filter_);
            return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
        }
        case kPhysicalOpLimit: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            if (op->GetLimitCnt() == 0 || op->GetLimitOptimized()) {
                return RegisterTask(node, cluster_task);
            }
            LimitRunner* runner = nullptr;
            CreateRunner<LimitRunner>(&runner, id_++, node->schemas_ctx(),
                                      op->GetLimitCnt());
            return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
        }
        case kPhysicalOpRename: {
            return Build(node->producers().at(0), status);
        }
        case kPhysicalOpPostRequestUnion: {
            auto left_task = Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right_task = Build(node->producers().at(1), status);
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto union_op = dynamic_cast<PhysicalPostRequestUnionNode*>(node);
            PostRequestUnionRunner* runner = nullptr;
            CreateRunner<PostRequestUnionRunner>(
                &runner, id_++, node->schemas_ctx(), union_op->request_ts());
            return RegisterTask(node, BinaryInherit(left_task, right_task,
                                                    runner, Key(), kRightBias));
        }
        default: {
            status.code = common::kOpGenError;
            status.msg = "can't handle node " +
                         std::to_string(node->GetOpType()) + " " +
                         PhysicalOpTypeName(node->GetOpType());
            LOG(WARNING) << status;
            return RegisterTask(node, fail);
        }
    }
}

ClusterTask RunnerBuilder::BinaryInherit(const ClusterTask& left,
                                         const ClusterTask& right,
                                         Runner* runner, const Key& index_key,
                                         const TaskBiasType bias) {
    if (support_cluster_optimized_) {
        return BuildClusterTaskForBinaryRunner(left, right, runner, index_key,
                                               bias);
    } else {
        return BuildLocalTaskForBinaryRunner(left, right, runner);
    }
}
ClusterTask RunnerBuilder::BuildLocalTaskForBinaryRunner(
    const ClusterTask& left, const ClusterTask& right, Runner* runner) {
    if (left.IsClusterTask() || right.IsClusterTask()) {
        LOG(WARNING) << "fail to build local task for binary runner";
        return ClusterTask();
    }
    runner->AddProducer(left.GetRoot());
    runner->AddProducer(right.GetRoot());
    return ClusterTask(runner);
}
ClusterTask RunnerBuilder::BuildClusterTaskForBinaryRunner(
    const ClusterTask& left, const ClusterTask& right, Runner* runner,
    const Key& index_key, const TaskBiasType bias) {
    if (nullptr == runner) {
        LOG(WARNING) << "Fail to build cluster task for null runner";
        return ClusterTask();
    }
    ClusterTask new_left = left;
    ClusterTask new_right = right;

    Runner* right_runner = new_right.GetRoot();
    Runner* left_runner = new_left.GetRoot();
    // if index key is valid, try to complete route info of right cluster
    // task
    if (index_key.ValidKey()) {
        if (!right.IsClusterTask()) {
            LOG(WARNING) << "Fail to buidl cluster task for "
                         << "[" << runner->id_ << "]"
                         << RunnerTypeName(runner->type_)
                         << ": can't handler local task with index key";
            return ClusterTask();
        }
        if (right.IsCompletedClusterTask()) {
            LOG(WARNING) << "Fail to complete cluster task for "
                         << "[" << runner->id_ << "]"
                         << RunnerTypeName(runner->type_)
                         << ": task is completed already";
            return ClusterTask();
        }
        RequestRunner* request_runner = nullptr;
        CreateRunner(&request_runner, id_++, left_runner->output_schemas());
        runner->AddProducer(request_runner);
        runner->AddProducer(right_runner);
        // build complete cluster task
        const RouteInfo& right_route_info = new_right.GetRouteInfo();
        ClusterTask cluster_task(
            runner, std::vector<Runner*>({runner}),
            RouteInfo(right_route_info.index_, index_key,
                      std::make_shared<ClusterTask>(new_left),
                      right_route_info.table_handler_));
        // TODO(chenjing): opt
        if (new_left.IsCompletedClusterTask()) {
            return BuildProxyRunnerForClusterTask(cluster_task);
        } else {
            return cluster_task;
        }
    }

    // Concat
    //      Agg1(Proxy(RequestUnion(Request, DATA))
    //      Agg2(Proxy(RequestUnion(Request, DATA))
    // -->
    // Proxy(Concat
    //          Agg1(RequestUnion(Request,DATA)
    //          Agg2(RequestUnion(Request,DATA)
    //      )

    // if left and right is completed cluster task
    while (new_left.IsCompletedClusterTask() &&
           new_right.IsCompletedClusterTask()) {
        // merge left and right task if tasks can be merged
        if (ClusterTask::TaskCanBeMerge(new_left, new_right)) {
            ClusterTask task =
                ClusterTask::TaskMerge(runner, new_left, new_right);
            runner->AddProducer(new_left.GetRoot());
            runner->AddProducer(new_right.GetRoot());
            return task;
        }
        switch (bias) {
            case kNoBias: {
                // Add build left proxy task into cluster job,
                // and update new_left
                new_left = BuildProxyRunnerForClusterTask(new_left);
                new_right = BuildProxyRunnerForClusterTask(new_right);
                break;
            }
            case kLeftBias: {
                // build proxy runner for right task
                new_right = BuildProxyRunnerForClusterTask(new_right);
                break;
            }
            case kRightBias: {
                // build proxy runner for right task
                new_left = BuildProxyRunnerForClusterTask(new_left);
                break;
            }
        }
    }
    if (new_left.IsUnCompletedClusterTask() ||
        new_right.IsUnCompletedClusterTask()) {
        LOG(WARNING) << "Fail to build cluster task, can't handler "
                        "uncompleted cluster task";
        return ClusterTask();
    }

    // prepare left and right for runner

    // left local task + right cluster task
    if (new_right.IsCompletedClusterTask()) {
        switch (bias) {
            case kNoBias:
            case kLeftBias: {
                new_right = BuildProxyRunnerForClusterTask(new_right);
                runner->AddProducer(new_left.GetRoot());
                runner->AddProducer(new_right.GetRoot());
                return ClusterTask::TaskMergeToLeft(runner, new_left,
                                                    new_right);
            }
            case kRightBias: {
                auto new_left_root_input =
                    ClusterTask::GetRequestInput(new_left);
                auto new_right_root_input =
                    ClusterTask::GetRequestInput(new_right);
                // task can be merge simply when their inputs are the same
                if (new_right_root_input == new_left_root_input) {
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToRight(runner, new_left,
                                                         new_right);
                } else if (new_left_root_input == nullptr) {
                    // reset replace inputs as request runner
                    new_right.ResetInputs(nullptr);
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToRight(runner, new_left,
                                                         new_right);
                } else {
                    LOG(WARNING) << "fail to merge local left task and cluster "
                                    "right task";
                    return ClusterTask();
                }
            }
            default:
                return ClusterTask();
        }
    } else if (new_left.IsCompletedClusterTask()) {
        switch (bias) {
            case kNoBias:
            case kRightBias: {
                new_left = BuildProxyRunnerForClusterTask(new_left);
                runner->AddProducer(new_left.GetRoot());
                runner->AddProducer(new_right.GetRoot());
                return ClusterTask::TaskMergeToRight(runner, new_left,
                                                     new_right);
            }
            case kLeftBias: {
                auto new_left_root_input =
                    ClusterTask::GetRequestInput(new_right);
                auto new_right_root_input =
                    ClusterTask::GetRequestInput(new_right);
                // task can be merge simply
                if (new_right_root_input == new_left_root_input) {
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToLeft(runner, new_left,
                                                        new_right);
                } else if (new_right_root_input == nullptr) {
                    // reset replace inputs as request runner
                    new_left.ResetInputs(nullptr);
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToLeft(runner, new_left,
                                                        new_right);
                } else {
                    LOG(WARNING) << "fail to merge cluster left task and local "
                                    "right task";
                    return ClusterTask();
                }
            }
            default:
                return ClusterTask();
        }
    } else {
        runner->AddProducer(new_left.GetRoot());
        runner->AddProducer(new_right.GetRoot());
        return ClusterTask::TaskMergeToLeft(runner, new_left, new_right);
    }
}
ClusterTask RunnerBuilder::BuildProxyRunnerForClusterTask(
    const ClusterTask& task) {
    if (!task.IsCompletedClusterTask()) {
        LOG(WARNING)
            << "Fail to build proxy runner, cluster task is uncompleted";
        return ClusterTask();
    }
    // return cached proxy runner
    Runner* proxy_runner = nullptr;
    auto find_iter = proxy_runner_map_.find(task.GetRoot());
    if (find_iter != proxy_runner_map_.cend()) {
        proxy_runner = find_iter->second;
        proxy_runner->EnableCache();
    } else {
        ProxyRequestRunner* new_proxy_runner = nullptr;
        uint32_t remote_task_id = cluster_job_.AddTask(task);
        CreateRunner<ProxyRequestRunner>(
            &new_proxy_runner, id_++, remote_task_id, task.GetIndexKeyInput(),
            task.GetRoot()->output_schemas());
        if (nullptr != task.GetIndexKeyInput()) {
            task.GetIndexKeyInput()->EnableCache();
        }
        if (task.GetRoot()->need_batch_cache()) {
            new_proxy_runner->EnableBatchCache();
        }
        proxy_runner_map_.insert(
            std::make_pair(task.GetRoot(), new_proxy_runner));
        proxy_runner = new_proxy_runner;
    }

    if (task.GetInput()) {
        return UnaryInheritTask(*task.GetInput(), proxy_runner);
    } else {
        return UnaryInheritTask(*request_task_, proxy_runner);
    }
    LOG(WARNING) << "Fail to build proxy runner for cluster job";
    return ClusterTask();
}
ClusterTask RunnerBuilder::UnCompletedClusterTask(
    Runner* runner, const std::shared_ptr<TableHandler> table_handler,
    std::string index) {
    return ClusterTask(runner, table_handler, index);
}
ClusterTask RunnerBuilder::BuildRequestTask(RequestRunner* runner) {
    if (nullptr == runner) {
        LOG(WARNING) << "fail to build request task with null runner";
        return ClusterTask();
    }
    ClusterTask request_task(runner);
    request_task_ = std::make_shared<ClusterTask>(request_task);
    return request_task;
}
ClusterTask RunnerBuilder::UnaryInheritTask(const ClusterTask& input,
                                            Runner* runner) {
    ClusterTask task = input;
    runner->AddProducer(task.GetRoot());
    task.SetRoot(runner);
    return task;
}

bool Runner::GetColumnBool(const int8_t* buf, const RowView* row_view, int idx,
                           type::Type type) {
    bool key = false;
    switch (type) {
        case hybridse::type::kInt32: {
            int32_t value = 0;
            if (0 == row_view->GetValue(buf, idx, type,
                                        reinterpret_cast<void*>(&value))) {
                return !(value == 0);
            }
            break;
        }
        case hybridse::type::kInt64: {
            int64_t value = 0;
            if (0 == row_view->GetValue(buf, idx, type,
                                        reinterpret_cast<void*>(&value))) {
                return !(value == 0);
            }
            break;
        }
        case hybridse::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetValue(buf, idx, type,
                                        reinterpret_cast<void*>(&value))) {
                return !(value == 0);
            }
            break;
        }
        case hybridse::type::kFloat: {
            float value;
            if (0 == row_view->GetValue(buf, idx, type,
                                        reinterpret_cast<void*>(&value))) {
                return !(value == 0);
            }
            break;
        }
        case hybridse::type::kDouble: {
            double value;
            if (0 == row_view->GetValue(buf, idx, type,
                                        reinterpret_cast<void*>(&value))) {
                return !(value == 0);
            }
            break;
        }
        case hybridse::type::kBool: {
            bool value;
            if (0 == row_view->GetValue(buf, idx, type,
                                        reinterpret_cast<void*>(&value))) {
                return value;
            }
            break;
        }
        default: {
            LOG(WARNING) << "fail to get bool for "
                            "current row";
            break;
        }
    }
    return key;
}

Row Runner::WindowProject(const int8_t* fn, const uint64_t row_key,
                          const Row row,
                          const codec::Row& parameter,
                          const bool is_instance,
                          size_t append_slices, Window* window) {
    if (row.empty()) {
        return row;
    }
    if (!window->BufferData(row_key, row)) {
        LOG(WARNING) << "fail to buffer data";
        return Row();
    }
    if (!is_instance) {
        return Row();
    }
    // Init current run step runtime
    JitRuntime::get()->InitRunStep();

    auto udf = reinterpret_cast<int32_t (*)(const int64_t key, const int8_t*,
                                            const int8_t*, const int8_t*, int8_t**)>(
        const_cast<int8_t*>(fn));
    int8_t* out_buf = nullptr;

    codec::ListRef<Row> window_ref;
    window_ref.list = reinterpret_cast<int8_t*>(window);
    auto window_ptr = reinterpret_cast<const int8_t*>(&window_ref);
    auto row_ptr = reinterpret_cast<const int8_t*>(&row);
    auto parameter_ptr = reinterpret_cast<const int8_t*>(&parameter);

    uint32_t ret = udf(row_key, row_ptr, window_ptr, parameter_ptr, &out_buf);

    // Release current run step resources
    JitRuntime::get()->ReleaseRunStep();

    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    if (window->instance_not_in_window()) {
        window->PopFrontData();
    }
    if (append_slices > 0) {
        return Row(base::RefCountedSlice::CreateManaged(
                       out_buf, RowView::GetSize(out_buf)),
                   append_slices, row);
    } else {
        return Row(base::RefCountedSlice::CreateManaged(
            out_buf, RowView::GetSize(out_buf)));
    }
}

int64_t Runner::GetColumnInt64(const int8_t* buf, const RowView* row_view,
                               int key_idx, type::Type key_type) {
    int64_t key = -1;
    switch (key_type) {
        case hybridse::type::kInt32: {
            int32_t value = 0;
            if (0 == row_view->GetValue(buf, key_idx, key_type,
                                        reinterpret_cast<void*>(&value))) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case hybridse::type::kInt64: {
            int64_t value = 0;
            if (0 == row_view->GetValue(buf, key_idx, key_type,
                                        reinterpret_cast<void*>(&value))) {
                return value;
            }
            break;
        }
        case hybridse::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetValue(buf, key_idx, key_type,
                                        reinterpret_cast<void*>(&value))) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case hybridse::type::kTimestamp: {
            int64_t value;
            if (0 == row_view->GetValue(buf, key_idx, key_type,
                                        reinterpret_cast<void*>(&value))) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        default: {
            LOG(WARNING) << "fail to get int64 for "
                            "current row";
            break;
        }
    }
    return key;
}

// TODO(chenjing/baoxinqi): TableHandler support reverse interface
std::shared_ptr<TableHandler> Runner::TableReverse(
    std::shared_ptr<TableHandler> table) {
    if (!table) {
        LOG(WARNING) << "fail to reverse null table";
        return std::shared_ptr<TableHandler>();
    }
    auto output_table = std::shared_ptr<MemTimeTableHandler>(
        new MemTimeTableHandler(table->GetSchema()));
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "fail to reverse empty table";
        return std::shared_ptr<TableHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        output_table->AddRow(iter->GetKey(), iter->GetValue());
        iter->Next();
    }
    output_table->Reverse();
    return output_table;
}
std::shared_ptr<DataHandlerList> Runner::BatchRequestRun(RunnerContext& ctx) {
    if (need_cache_) {
        auto cached = ctx.GetBatchCache(id_);
        if (cached != nullptr) {
            DLOG(INFO) << "RUNNER ID " << id_ << " HIT CACHE!";
            return cached;
        }
    }
    std::shared_ptr<DataHandlerVector> outputs =
        std::make_shared<DataHandlerVector>();
    std::vector<std::shared_ptr<DataHandler>> inputs(producers_.size());
    std::vector<std::shared_ptr<DataHandlerList>> batch_inputs(
        producers_.size());
    for (size_t idx = producers_.size(); idx > 0; idx--) {
        batch_inputs[idx - 1] = producers_[idx - 1]->BatchRequestRun(ctx);
    }

    for (size_t idx = 0; idx < ctx.GetRequestSize(); idx++) {
        inputs.clear();
        for (size_t producer_idx = 0; producer_idx < producers_.size();
             producer_idx++) {
            inputs.push_back(batch_inputs[producer_idx]->Get(idx));
        }
        auto res = Run(ctx, inputs);
        if (need_batch_cache_) {
            if (ctx.is_debug()) {
                std::ostringstream oss;
                oss << "RUNNER TYPE: " << RunnerTypeName(type_)
                    << ", ID: " << id_ << " HIT BATCH CACHE!"
                    << "\n";
                Runner::PrintData(oss, output_schemas_, res);
                LOG(INFO) << oss.str();
            }
            auto repeated_data = std::shared_ptr<DataHandlerList>(
                new DataHandlerRepeater(res, ctx.GetRequestSize()));
            if (need_cache_) {
                ctx.SetBatchCache(id_, repeated_data);
            }
            return repeated_data;
        }
        outputs->Add(res);
    }
    if (ctx.is_debug()) {
        std::ostringstream oss;
        oss << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
            << "\n";
        for (size_t idx = 0; idx < outputs->GetSize(); idx++) {
            if (idx >= MAX_DEBUG_BATCH_SiZE) {
                oss << ">= MAX_DEBUG_BATCH_SiZE...\n";
                break;
            }
            Runner::PrintData(oss, output_schemas_, outputs->Get(idx));
        }
        LOG(INFO) << oss.str();
    }
    if (need_cache_) {
        ctx.SetBatchCache(id_, outputs);
    }
    return outputs;
}
std::shared_ptr<DataHandler> Runner::RunWithCache(RunnerContext& ctx) {
    if (need_cache_) {
        auto cached = ctx.GetCache(id_);
        if (cached != nullptr) {
            DLOG(INFO) << "RUNNER ID " << id_ << " HIT CACHE!";
            return cached;
        }
    }
    std::vector<std::shared_ptr<DataHandler>> inputs(producers_.size());
    for (size_t idx = producers_.size(); idx > 0; idx--) {
        inputs[idx - 1] = producers_[idx - 1]->RunWithCache(ctx);
    }

    auto res = Run(ctx, inputs);
    if (ctx.is_debug()) {
        std::ostringstream oss;
        oss << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
            << "\n";
        Runner::PrintData(oss, output_schemas_, res);
        LOG(INFO) << oss.str();
    }
    if (need_cache_) {
        ctx.SetCache(id_, res);
    }
    return res;
}
std::shared_ptr<DataHandler> DataRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    return data_handler_;
}
std::shared_ptr<DataHandlerList> DataRunner::BatchRequestRun(
    RunnerContext& ctx) {
    if (need_cache_) {
        auto cached = ctx.GetBatchCache(id_);
        if (cached != nullptr) {
            DLOG(INFO) << "RUNNER ID " << id_ << " HIT CACHE!";
            return cached;
        }
    }
    auto res = std::shared_ptr<DataHandlerList>(
        new DataHandlerRepeater(data_handler_, ctx.GetRequestSize()));

    if (ctx.is_debug()) {
        std::ostringstream oss;
        oss << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
            << ", Repeated " << ctx.GetRequestSize() << "\n";
        Runner::PrintData(oss, output_schemas_, res->Get(0));
        LOG(INFO) << oss.str();
    }
    if (need_cache_) {
        ctx.SetBatchCache(id_, res);
    }
    return res;
}
std::shared_ptr<DataHandler> RequestRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    return std::shared_ptr<MemRowHandler>(new MemRowHandler(ctx.GetRequest()));
}
std::shared_ptr<DataHandlerList> RequestRunner::BatchRequestRun(
    RunnerContext& ctx) {
    if (need_cache_) {
        auto cached = ctx.GetBatchCache(id_);
        if (cached != nullptr) {
            DLOG(INFO) << "RUNNER ID " << id_ << " HIT CACHE!";
            return cached;
        }
    }
    std::shared_ptr<DataHandlerVector> res =
        std::shared_ptr<DataHandlerVector>(new DataHandlerVector());
    for (size_t idx = 0; idx < ctx.GetRequestSize(); idx++) {
        res->Add(std::shared_ptr<MemRowHandler>(
            new MemRowHandler(ctx.GetRequest(idx))));
    }

    if (ctx.is_debug()) {
        std::ostringstream oss;
        oss << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
            << "\n";
        for (size_t idx = 0; idx < res->GetSize(); idx++) {
            if (idx >= MAX_DEBUG_BATCH_SiZE) {
                oss << ">= MAX_DEBUG_BATCH_SiZE...\n";
                break;
            }
            Runner::PrintData(oss, output_schemas_, res->Get(idx));
        }
        LOG(INFO) << oss.str();
    }
    if (need_cache_) {
        ctx.SetBatchCache(id_, res);
    }
    return res;
}
std::shared_ptr<DataHandler> GroupRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    return partition_gen_.Partition(input, ctx.GetParameterRow());
}
std::shared_ptr<DataHandler> SortRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    return sort_gen_.Sort(input);
}

std::shared_ptr<DataHandler> ConstProjectRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    output_table->AddRow(project_gen_.Gen(ctx.GetParameterRow()));
    return output_table;
}
std::shared_ptr<DataHandler> TableProjectRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto input = inputs[0];
    if (!input) {
        return std::shared_ptr<DataHandler>();
    }

    if (kTableHandler != input->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    auto iter = std::dynamic_pointer_cast<TableHandler>(input)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "Table Project Fail: table iter is Empty";
        return std::shared_ptr<DataHandler>();
    }
    auto& parameter = ctx.GetParameterRow();
    iter->SeekToFirst();
    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
            break;
        }
        output_table->AddRow(project_gen_.Gen(iter->GetValue(), parameter));
        iter->Next();
    }
    return output_table;
}

std::shared_ptr<DataHandler> RowProjectRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto row = std::dynamic_pointer_cast<RowHandler>(inputs[0]);
    return std::shared_ptr<RowHandler>(
        new MemRowHandler(project_gen_.Gen(row->GetValue(), ctx.GetParameterRow())));
}

std::shared_ptr<DataHandler> SimpleProjectRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto input = inputs[0];
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "simple project fail: input is null";
        return fail_ptr;
    }

    auto& parameter = ctx.GetParameterRow();
    switch (input->GetHanlderType()) {
        case kTableHandler: {
            return std::shared_ptr<TableHandler>(new TableProjectWrapper(
                std::dynamic_pointer_cast<TableHandler>(input),
                parameter, &project_gen_.fun_));
        }
        case kPartitionHandler: {
            return std::shared_ptr<TableHandler>(new PartitionProjectWrapper(
                std::dynamic_pointer_cast<PartitionHandler>(input),
                parameter, &project_gen_.fun_));
        }
        case kRowHandler: {
            return std::shared_ptr<RowHandler>(new RowProjectWrapper(
                std::dynamic_pointer_cast<RowHandler>(input),
                parameter, &project_gen_.fun_));
        }
        default: {
            LOG(WARNING) << "Fail run simple project, invalid handler type "
                         << input->GetHandlerTypeName();
        }
    }

    return std::shared_ptr<DataHandler>();
}

Row SelectSliceRunner::GetSliceFn::operator()(const Row& row, const Row& parameter) const {
    if (slice_ < static_cast<size_t>(row.GetRowPtrCnt())) {
        return Row(row.GetSlice(slice_));
    } else {
        return Row();
    }
}

std::shared_ptr<DataHandler> SelectSliceRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "empty inputs";
        return nullptr;
    }
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "select slice fail: input is null";
        return nullptr;
    }
    auto& parameter = ctx.GetParameterRow();
    switch (input->GetHanlderType()) {
        case kTableHandler: {
            return std::shared_ptr<TableHandler>(new TableProjectWrapper(
                std::dynamic_pointer_cast<TableHandler>(input), parameter,
                &get_slice_fn_));
        }
        case kPartitionHandler: {
            return std::shared_ptr<TableHandler>(new PartitionProjectWrapper(
                std::dynamic_pointer_cast<PartitionHandler>(input), parameter,
                &get_slice_fn_));
        }
        case kRowHandler: {
            return std::make_shared<RowProjectWrapper>(
                std::dynamic_pointer_cast<RowHandler>(input), parameter, &get_slice_fn_);
        }
        default: {
            LOG(WARNING) << "Fail run select slice, invalid handler type "
                         << input->GetHandlerTypeName();
        }
    }
    return nullptr;
}

std::shared_ptr<DataHandler> WindowAggRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto input = inputs[0];
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (!input) {
        LOG(WARNING) << "window aggregation fail: input is null";
        return fail_ptr;
    }
    auto& parameter = ctx.GetParameterRow();
    // Partition Instance Table
    auto instance_partition =
        instance_window_gen_.partition_gen_.Partition(input, parameter);
    if (!instance_partition) {
        LOG(WARNING) << "Window Aggregation Fail: input partition is empty";
        return fail_ptr;
    }
    auto instance_partition_iter = instance_partition->GetWindowIterator();
    if (!instance_partition_iter) {
        LOG(WARNING)
            << "Window Aggregation Fail: when partition input is empty";
        return fail_ptr;
    }
    instance_partition_iter->SeekToFirst();

    // Partition Union Table
    auto union_inpus = windows_union_gen_.RunInputs(ctx);
    auto union_partitions = windows_union_gen_.PartitionEach(union_inpus, parameter);
    // Prepare Join Tables
    auto join_right_tables = windows_join_gen_.RunInputs(ctx);

    // Compute output
    std::shared_ptr<MemTableHandler> output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler());
    while (instance_partition_iter->Valid()) {
        auto key = instance_partition_iter->GetKey().ToString();
        RunWindowAggOnKey(parameter, instance_partition, union_partitions,
                          join_right_tables, key, output_table);
        instance_partition_iter->Next();
    }
    return output_table;
}

// Run Window Aggeregation on given key
void WindowAggRunner::RunWindowAggOnKey(
    const Row& parameter,
    std::shared_ptr<PartitionHandler> instance_partition,
    std::vector<std::shared_ptr<PartitionHandler>> union_partitions,
    std::vector<std::shared_ptr<DataHandler>> join_right_tables,
    const std::string& key, std::shared_ptr<MemTableHandler> output_table) {
    // Prepare Instance Segment
    auto instance_segment = instance_partition->GetSegment(key);
    instance_segment = instance_window_gen_.sort_gen_.Sort(instance_segment);
    if (!instance_segment) {
        LOG(WARNING) << "Instance Segment is Empty";
        return;
    }

    auto instance_segment_iter = instance_segment->GetIterator();
    if (!instance_segment_iter) {
        LOG(WARNING) << "Instance Segment is Empty";
        return;
    }
    instance_segment_iter->SeekToFirst();

    // Prepare Union Segment Iterators
    size_t unions_cnt = windows_union_gen_.inputs_cnt_;
    std::vector<std::shared_ptr<TableHandler>> union_segments(unions_cnt);
    std::vector<std::unique_ptr<RowIterator>> union_segment_iters(unions_cnt);
    std::vector<IteratorStatus> union_segment_status(unions_cnt);

    for (size_t i = 0; i < unions_cnt; i++) {
        if (!union_partitions[i]) {
            continue;
        }
        auto segment = union_partitions[i]->GetSegment(key);
        segment = windows_union_gen_.windows_gen_[i].sort_gen_.Sort(segment);
        union_segments[i] = segment;
        if (!segment) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i] = segment->GetIterator();
        if (!union_segment_iters[i]) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i]->SeekToFirst();
        if (!union_segment_iters[i]->Valid()) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        uint64_t ts = union_segment_iters[i]->GetKey();
        union_segment_status[i] = IteratorStatus(ts);
    }

    int32_t min_union_pos =
        0 == unions_cnt
            ? -1
            : IteratorStatus::PickIteratorWithMininumKey(&union_segment_status);
    int32_t cnt = output_table->GetCount();
    HistoryWindow window(instance_window_gen_.range_gen_.window_range_);
    window.set_instance_not_in_window(instance_not_in_window_);
    window.set_exclude_current_time(exclude_current_time_);

    while (instance_segment_iter->Valid()) {
        if (limit_cnt_ > 0 && cnt >= limit_cnt_) {
            break;
        }
        const Row& instance_row = instance_segment_iter->GetValue();
        uint64_t instance_order = instance_segment_iter->GetKey();
        while (min_union_pos >= 0 &&
               union_segment_status[min_union_pos].key_ < instance_order) {
            Row row = union_segment_iters[min_union_pos]->GetValue();
            if (windows_join_gen_.Valid()) {
                row = windows_join_gen_.Join(row, join_right_tables, parameter);
            }
            window_project_gen_.Gen(
                union_segment_iters[min_union_pos]->GetKey(), row, parameter,
                false, append_slices_, &window);

            // Update Iterator Status
            union_segment_iters[min_union_pos]->Next();
            if (!union_segment_iters[min_union_pos]->Valid()) {
                union_segment_status[min_union_pos].MarkInValid();
            } else {
                union_segment_status[min_union_pos].set_key(
                    union_segment_iters[min_union_pos]->GetKey());
            }
            // Pick new mininum union pos
            min_union_pos = IteratorStatus::PickIteratorWithMininumKey(
                &union_segment_status);
        }
        if (windows_join_gen_.Valid()) {
            Row row = instance_row;
            row = windows_join_gen_.Join(instance_row, join_right_tables, parameter);
            output_table->AddRow(window_project_gen_.Gen(instance_segment_iter->GetKey(), row, parameter, true,
                                                         append_slices_, &window));
        } else {
            output_table->AddRow(window_project_gen_.Gen(
                instance_segment_iter->GetKey(), instance_row, parameter, true,
                append_slices_, &window));
        }

        cnt++;
        instance_segment_iter->Next();
    }
}

std::shared_ptr<DataHandler> RequestLastJoinRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {  // NOLINT
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 2u) {
        LOG(WARNING) << "inputs size < 2";
        return std::shared_ptr<DataHandler>();
    }
    auto right = inputs[1];
    auto left = inputs[0];
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    auto left_row = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
    auto &parameter = ctx.GetParameterRow();
    if (output_right_only_) {
        return std::shared_ptr<RowHandler>(new MemRowHandler(
            join_gen_.RowLastJoinDropLeftSlices(left_row, right, parameter)));
    } else {
        return std::shared_ptr<RowHandler>(
            new MemRowHandler(join_gen_.RowLastJoin(left_row, right, parameter)));
    }
}

std::shared_ptr<DataHandler> LastJoinRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 2) {
        LOG(WARNING) << "inputs size < 2";
        return fail_ptr;
    }
    auto right = inputs[1];
    auto left = inputs[0];
    if (!left || !right) {
        LOG(WARNING) << "fail to run last join: left|right input is empty";
        return fail_ptr;
    }
    if (!right) {
        LOG(WARNING) << "fail to run last join: right partition is empty";
        return fail_ptr;
    }
    auto &parameter = ctx.GetParameterRow();

    switch (left->GetHanlderType()) {
        case kTableHandler: {
            if (join_gen_.right_group_gen_.Valid()) {
                right = join_gen_.right_group_gen_.Partition(right, parameter);
            }
            if (!right) {
                LOG(WARNING)
                    << "fail to run last join: right partition is empty";
                return fail_ptr;
            }
            auto left_table = std::dynamic_pointer_cast<TableHandler>(left);

            auto output_table =
                std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
            output_table->SetOrderType(left_table->GetOrderType());
            if (kPartitionHandler == right->GetHanlderType()) {
                if (!join_gen_.TableJoin(
                        left_table,
                        std::dynamic_pointer_cast<PartitionHandler>(right),
                        parameter,
                        output_table)) {
                    return fail_ptr;
                }
            } else {
                if (!join_gen_.TableJoin(
                        left_table,
                        std::dynamic_pointer_cast<TableHandler>(right),
                        parameter,
                        output_table)) {
                    return fail_ptr;
                }
            }
            return output_table;
        }
        case kPartitionHandler: {
            if (join_gen_.right_group_gen_.Valid()) {
                right = join_gen_.right_group_gen_.Partition(right, parameter);
            }
            if (!right) {
                LOG(WARNING)
                    << "fail to run last join: right partition is empty";
                return fail_ptr;
            }
            auto output_partition =
                std::shared_ptr<MemPartitionHandler>(new MemPartitionHandler());
            auto left_partition =
                std::dynamic_pointer_cast<PartitionHandler>(left);
            output_partition->SetOrderType(left_partition->GetOrderType());
            if (kPartitionHandler == right->GetHanlderType()) {
                if (!join_gen_.PartitionJoin(
                        left_partition,
                        std::dynamic_pointer_cast<PartitionHandler>(right),
                        parameter,
                        output_partition)) {
                    return fail_ptr;
                }

            } else {
                if (!join_gen_.PartitionJoin(
                        left_partition,
                        std::dynamic_pointer_cast<TableHandler>(right),
                        parameter,
                        output_partition)) {
                    return fail_ptr;
                }
            }
            return output_partition;
        }
        case kRowHandler: {
            auto left_row = std::dynamic_pointer_cast<RowHandler>(left);
            return std::make_shared<MemRowHandler>(
                join_gen_.RowLastJoin(left_row->GetValue(), right, parameter));
        }
        default:
            return fail_ptr;
    }
}

std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<DataHandler> input, const Row& parameter) {
    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            return Partition(
                std::dynamic_pointer_cast<PartitionHandler>(input), parameter);
        }
        case kTableHandler: {
            return Partition(std::dynamic_pointer_cast<TableHandler>(input), parameter);
        }
        default: {
            LOG(WARNING) << "Partition Fail: input isn't partition or table";
            return std::shared_ptr<PartitionHandler>();
        }
    }
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<PartitionHandler> table, const Row& parameter) {
    if (!key_gen_.Valid()) {
        return table;
    }
    if (!table) {
        return std::shared_ptr<PartitionHandler>();
    }
    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));
    auto partitions = std::dynamic_pointer_cast<PartitionHandler>(table);
    auto iter = partitions->GetWindowIterator();
    if (!iter) {
        LOG(WARNING) << "Partition Fail: partition is Empty";
        return std::shared_ptr<PartitionHandler>();
    }
    iter->SeekToFirst();
    output_partitions->SetOrderType(table->GetOrderType());
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            iter->Next();
            continue;
        }
        auto segment_key = iter->GetKey().ToString();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            std::string keys = key_gen_.Gen(segment_iter->GetValue(), parameter);
            output_partitions->AddRow(segment_key + "|" + keys,
                                      segment_iter->GetKey(),
                                      segment_iter->GetValue());
            segment_iter->Next();
        }
        iter->Next();
    }
    return output_partitions;
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<TableHandler> table, const Row& parameter) {
    auto fail_ptr = std::shared_ptr<PartitionHandler>();
    if (!key_gen_.Valid()) {
        return fail_ptr;
    }
    if (!table) {
        return fail_ptr;
    }
    if (kTableHandler != table->GetHanlderType()) {
        return fail_ptr;
    }

    auto output_partitions = std::shared_ptr<MemPartitionHandler>(
        new MemPartitionHandler(table->GetSchema()));

    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "Fail to group empty table: table is empty";
        return fail_ptr;
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        std::string keys = key_gen_.Gen(iter->GetValue(), parameter);
        output_partitions->AddRow(keys, iter->GetKey(), iter->GetValue());
        iter->Next();
    }
    output_partitions->SetOrderType(table->GetOrderType());
    return output_partitions;
}
std::shared_ptr<DataHandler> SortGenerator::Sort(
    std::shared_ptr<DataHandler> input, const bool reverse) {
    if (!input || !is_valid_ || !order_gen_.Valid()) {
        return input;
    }
    switch (input->GetHanlderType()) {
        case kTableHandler:
            return Sort(std::dynamic_pointer_cast<TableHandler>(input),
                        reverse);
        case kPartitionHandler:
            return Sort(std::dynamic_pointer_cast<PartitionHandler>(input),
                        reverse);
        default: {
            LOG(WARNING) << "Sort Fail: input isn't partition or table";
            return std::shared_ptr<PartitionHandler>();
        }
    }
}

std::shared_ptr<PartitionHandler> SortGenerator::Sort(
    std::shared_ptr<PartitionHandler> partition, const bool reverse) {
    bool is_asc = reverse ? !is_asc_ : is_asc_;
    if (!is_valid_) {
        return partition;
    }
    if (!partition) {
        return std::shared_ptr<PartitionHandler>();
    }
    if (!order_gen().Valid() &&
        is_asc == (partition->GetOrderType() == kAscOrder)) {
        DLOG(INFO) << "match the order redirect the table";
        return partition;
    }

    DLOG(INFO) << "mismatch the order and sort it";
    auto output =
        std::shared_ptr<MemPartitionHandler>(new MemPartitionHandler());

    auto iter = partition->GetWindowIterator();
    if (!iter) {
        LOG(WARNING) << "Sort partition fail: partition is Empty";
        return std::shared_ptr<PartitionHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            iter->Next();
            continue;
        }

        auto key = iter->GetKey().ToString();
        segment_iter->SeekToFirst();
        while (segment_iter->Valid()) {
            int64_t ts = order_gen_.Gen(segment_iter->GetValue());
            output->AddRow(key, static_cast<uint64_t>(ts),
                           segment_iter->GetValue());
            segment_iter->Next();
        }
    }
    if (order_gen_.Valid()) {
        output->Sort(is_asc);
    } else if (is_asc && OrderType::kDescOrder == partition->GetOrderType()) {
        output->Reverse();
    }
    return output;
}

std::shared_ptr<TableHandler> SortGenerator::Sort(
    std::shared_ptr<TableHandler> table, const bool reverse) {
    bool is_asc = reverse ? !is_asc_ : is_asc_;
    if (!table || !is_valid_) {
        return table;
    }
    if (!order_gen().Valid() &&
        is_asc == (table->GetOrderType() == kAscOrder)) {
        return table;
    }
    auto output_table = std::shared_ptr<MemTimeTableHandler>(
        new MemTimeTableHandler(table->GetSchema()));
    output_table->SetOrderType(table->GetOrderType());
    auto iter = std::dynamic_pointer_cast<TableHandler>(table)->GetIterator();
    if (!iter) {
        LOG(WARNING) << "Sort table fail: table is Empty";
        return std::shared_ptr<TableHandler>();
    }
    iter->SeekToFirst();
    while (iter->Valid()) {
        if (order_gen_.Valid()) {
            int64_t key = order_gen_.Gen(iter->GetValue());
            output_table->AddRow(static_cast<uint64_t>(key), iter->GetValue());
        } else {
            output_table->AddRow(iter->GetKey(), iter->GetValue());
        }
        iter->Next();
    }

    if (order_gen_.Valid()) {
        output_table->Sort(is_asc);
    } else {
        switch (table->GetOrderType()) {
            case kDescOrder:
                if (is_asc) {
                    output_table->Reverse();
                }
                break;
            case kAscOrder:
                if (!is_asc) {
                    output_table->Reverse();
                }
                break;
            default: {
                LOG(WARNING) << "Fail to Sort, order type invalid";
                return std::shared_ptr<TableHandler>();
            }
        }
    }
    return output_table;
}
Row JoinGenerator::RowLastJoinDropLeftSlices(
    const Row& left_row, std::shared_ptr<DataHandler> right, const Row& parameter) {
    Row joined = RowLastJoin(left_row, right, parameter);
    Row right_row(joined.GetSlice(left_slices_));
    for (size_t offset = 1; offset < right_slices_; offset++) {
        right_row.Append(joined.GetSlice(left_slices_ + offset));
    }
    return right_row;
}
Row JoinGenerator::RowLastJoin(const Row& left_row,
                               std::shared_ptr<DataHandler> right,
                               const Row& parameter) {
    switch (right->GetHanlderType()) {
        case kPartitionHandler: {
            return RowLastJoinPartition(
                left_row, std::dynamic_pointer_cast<PartitionHandler>(right), parameter);
        }
        case kTableHandler: {
            return RowLastJoinTable(
                left_row, std::dynamic_pointer_cast<TableHandler>(right), parameter);
        }
        case kRowHandler: {
            auto right_table =
                std::shared_ptr<MemTableHandler>(new MemTableHandler());
            right_table->AddRow(
                std::dynamic_pointer_cast<RowHandler>(right)->GetValue());
            return RowLastJoinTable(left_row, right_table, parameter);
        }
        default: {
            LOG(WARNING) << "Last Join right isn't row or table or partition";
            return Row(left_slices_, left_row, right_slices_, Row());
        }
    }
}
Row JoinGenerator::RowLastJoinPartition(
    const Row& left_row, std::shared_ptr<PartitionHandler> partition,
    const Row& parameter) {
    if (!index_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when partition "
                        "keys is empty";
        return Row();
    }
    std::string partition_key = index_key_gen_.Gen(left_row, parameter);
    auto right_table = partition->GetSegment(partition_key);
    return RowLastJoinTable(left_row, right_table, parameter);
}
Row JoinGenerator::RowLastJoinTable(const Row& left_row,
                                    std::shared_ptr<TableHandler> table,
                                    const Row& parameter) {
    if (right_sort_gen_.Valid()) {
        table = right_sort_gen_.Sort(table, true);
    }
    if (!table) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices_, left_row, right_slices_, Row());
    }
    auto right_iter = table->GetIterator();
    if (!right_iter) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices_, left_row, right_slices_, Row());
    }
    right_iter->SeekToFirst();
    if (!right_iter->Valid()) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices_, left_row, right_slices_, Row());
    }

    if (!left_key_gen_.Valid() && !condition_gen_.Valid()) {
        return Row(left_slices_, left_row, right_slices_,
                   right_iter->GetValue());
    }

    std::string left_key_str = "";
    if (left_key_gen_.Valid()) {
        left_key_str = left_key_gen_.Gen(left_row, parameter);
    }
    while (right_iter->Valid()) {
        if (right_group_gen_.Valid()) {
            auto right_key_str =
                right_group_gen_.GetKey(right_iter->GetValue(), parameter);
            if (left_key_gen_.Valid() && left_key_str != right_key_str) {
                right_iter->Next();
                continue;
            }
        }

        Row joined_row(left_slices_, left_row, right_slices_,
                       right_iter->GetValue());
        if (!condition_gen_.Valid()) {
            return joined_row;
        }
        if (condition_gen_.Gen(joined_row, parameter)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices_, left_row, right_slices_, Row());
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<TableHandler> right,
                              const Row& parameter,
                              std::shared_ptr<MemTimeTableHandler> output) {
    auto left_iter = left->GetIterator();
    if (!left_iter) {
        LOG(WARNING) << "Table Join with empty left table";
        return false;
    }
    left_iter->SeekToFirst();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        output->AddRow(
            left_iter->GetKey(),
            Runner::RowLastJoinTable(left_slices_, left_row, right_slices_,
                                     right, parameter, right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<PartitionHandler> right,
                              const Row& parameter,
                              std::shared_ptr<MemTimeTableHandler> output) {
    if (!left_key_gen_.Valid() && !index_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when join "
                        "left_key_gen_ and index_key_gen_ is invalid";
        return false;
    }
    auto left_iter = left->GetIterator();

    if (!left_iter) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }

    left_iter->SeekToFirst();
    while (left_iter->Valid()) {
        const Row& left_row = left_iter->GetValue();
        std::string key_str =
            index_key_gen_.Valid() ? index_key_gen_.Gen(left_row, parameter) : "";
        if (left_key_gen_.Valid()) {
            key_str = key_str.empty()
                          ? left_key_gen_.Gen(left_row, parameter)
                          : key_str + "|" + left_key_gen_.Gen(left_row, parameter);
        }
        DLOG(INFO) << "key_str " << key_str;
        auto right_table = right->GetSegment(key_str);
        output->AddRow(left_iter->GetKey(), Runner::RowLastJoinTable(left_slices_, left_row, right_slices_, right_table,
                                                                     parameter, right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<TableHandler> right,
                                  const Row& parameter,
                                  std::shared_ptr<MemPartitionHandler> output) {
    auto left_window_iter = left->GetWindowIterator();
    if (!left_window_iter) {
        LOG(WARNING) << "fail to run last join: left iter empty";
        return false;
    }
    left_window_iter->SeekToFirst();
    while (left_window_iter->Valid()) {
        auto left_iter = left_window_iter->GetValue();
        auto left_key = left_window_iter->GetKey();
        if (!left_iter) {
            left_window_iter->Next();
            continue;
        }
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            auto key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_, right,
                               parameter,
                               right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_window_iter->Next();
    }
    return true;
}
bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<PartitionHandler> right,
                                  const Row& parameter,
                                  std::shared_ptr<MemPartitionHandler> output) {
    if (!left) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }
    auto left_partition_iter = left->GetWindowIterator();
    if (!left_partition_iter) {
        LOG(WARNING) << "fail to run last join: left input empty";
        return false;
    }
    if (!left_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when join "
                        "left_key_gen_ is invalid";
        return false;
    }

    left_partition_iter->SeekToFirst();
    while (left_partition_iter->Valid()) {
        auto left_iter = left_partition_iter->GetValue();
        auto left_key = left_partition_iter->GetKey();
        if (!left_iter) {
            left_partition_iter->Next();
            continue;
        }
        left_iter->SeekToFirst();
        while (left_iter->Valid()) {
            const Row& left_row = left_iter->GetValue();
            const std::string& key_str =
                index_key_gen_.Valid() ? index_key_gen_.Gen(left_row, parameter) + "|" +
                                             left_key_gen_.Gen(left_row, parameter)
                                       : left_key_gen_.Gen(left_row, parameter);
            auto right_table = right->GetSegment(key_str);
            auto left_key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(left_key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_,
                               right_table, parameter, right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_partition_iter->Next();
    }
    return true;
}
const Row Runner::RowLastJoinTable(size_t left_slices, const Row& left_row,
                                   size_t right_slices,
                                   std::shared_ptr<TableHandler> right_table,
                                   const Row& parameter,
                                   SortGenerator& right_sort,
                                   ConditionGenerator& cond_gen) {
    right_table = right_sort.Sort(right_table, true);
    if (!right_table) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices, left_row, right_slices, Row());
    }
    auto right_iter = right_table->GetIterator();
    if (!right_iter) {
        DLOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices, left_row, right_slices, Row());
    }
    right_iter->SeekToFirst();

    if (!right_iter->Valid()) {
        LOG(WARNING) << "Last Join right table is empty";
        return Row(left_slices, left_row, right_slices, Row());
    }

    if (!cond_gen.Valid()) {
        return Row(left_slices, left_row, right_slices, right_iter->GetValue());
    }

    while (right_iter->Valid()) {
        Row joined_row(left_slices, left_row, right_slices,
                       right_iter->GetValue());
        if (cond_gen.Gen(joined_row, parameter)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices, left_row, right_slices, Row());
}
void Runner::PrintData(std::ostringstream& oss,
                       const vm::SchemasContext* schema_list,
                       std::shared_ptr<DataHandler> data) {
    std::vector<RowView> row_view_list;
    ::hybridse::base::TextTable t('-', '|', '+');
    // Add Header
    if (data) {
        t.add(data->GetHandlerTypeName());
    } else {
        t.add("EmptyDataHandler");
    }
    for (size_t i = 0; i < schema_list->GetSchemaSourceSize(); ++i) {
        auto source = schema_list->GetSchemaSource(i);
        for (int j = 0; j < source->GetSchema()->size(); j++) {
            if (source->GetSourceName().empty()) {
                t.add(source->GetSchema()->Get(j).name());
            } else {
                t.add(source->GetSourceName() + "." +
                      source->GetSchema()->Get(j).name());
            }
            if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                break;
            }
        }
        row_view_list.push_back(RowView(*source->GetSchema()));
        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
            t.add("...");
            break;
        }
    }

    t.end_of_row();
    if (!data) {
        t.add("Empty set");
        t.end_of_row();
        oss << t;
        return;
    }

    switch (data->GetHanlderType()) {
        case kRowHandler: {
            auto row_handler = std::dynamic_pointer_cast<RowHandler>(data);
            if (!row_handler) {
                t.add("NULL Row");
                t.end_of_row();
                break;
            }
            auto row = row_handler->GetValue();
            t.add("0");
            for (size_t id = 0; id < row_view_list.size(); id++) {
                RowView& row_view = row_view_list[id];
                row_view.Reset(row.buf(id), row.size(id));
                for (int idx = 0; idx < schema_list->GetSchema(id)->size();
                     idx++) {
                    std::string str = row_view.GetAsString(idx);
                    t.add(str);
                    if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                        break;
                    }
                }
                if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                    t.add("...");
                    break;
                }
            }

            t.end_of_row();
            break;
        }
        case kTableHandler: {
            auto table_handler = std::dynamic_pointer_cast<TableHandler>(data);
            if (!table_handler) {
                t.add("Empty set");
                t.end_of_row();
                break;
            }
            auto iter = table_handler->GetIterator();
            if (!iter) {
                t.add("Empty set");
                t.end_of_row();
                break;
            }
            iter->SeekToFirst();
            if (!iter->Valid()) {
                t.add("Empty set");
                t.end_of_row();
                break;
            } else {
                int cnt = 0;
                while (iter->Valid() && cnt++ < MAX_DEBUG_LINES_CNT) {
                    auto row = iter->GetValue();
                    t.add(std::to_string(iter->GetKey()));
                    for (size_t id = 0; id < row_view_list.size(); id++) {
                        RowView& row_view = row_view_list[id];
                        row_view.Reset(row.buf(id), row.size(id));
                        for (int idx = 0;
                             idx < schema_list->GetSchema(id)->size(); idx++) {
                            std::string str = row_view.GetAsString(idx);
                            t.add(str);
                            if (t.current_columns_size() >=
                                MAX_DEBUG_COLUMN_MAX) {
                                break;
                            }
                        }
                        if (t.current_columns_size() >= MAX_DEBUG_COLUMN_MAX) {
                            t.add("...");
                            break;
                        }
                    }
                    iter->Next();
                    t.end_of_row();
                }
            }

            break;
        }
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(data);
            if (!partition) {
                t.add("Empty set");
                t.end_of_row();
                break;
            }
            auto iter = partition->GetWindowIterator();
            int cnt = 0;
            if (!iter) {
                t.add("Empty set");
                t.end_of_row();
                break;
            }
            iter->SeekToFirst();
            if (!iter->Valid()) {
                t.add("Empty set");
                t.end_of_row();
                break;
            }
            while (iter->Valid() && cnt++ < MAX_DEBUG_LINES_CNT) {
                t.add("KEY: " + std::string(reinterpret_cast<const char*>(
                                                iter->GetKey().buf()),
                                            iter->GetKey().size()));
                t.end_of_row();
                auto segment_iter = iter->GetValue();
                if (!segment_iter) {
                    t.add("Empty set");
                    t.end_of_row();
                    break;
                }
                segment_iter->SeekToFirst();
                if (!segment_iter->Valid()) {
                    t.add("Empty set");
                    t.end_of_row();
                    break;
                } else {
                    int partition_row_cnt = 0;
                    while (segment_iter->Valid() &&
                           partition_row_cnt++ < MAX_DEBUG_LINES_CNT) {
                        auto row = segment_iter->GetValue();
                        t.add(std::to_string(segment_iter->GetKey()));
                        for (size_t id = 0; id < row_view_list.size(); id++) {
                            RowView& row_view = row_view_list[id];
                            row_view.Reset(row.buf(id), row.size(id));
                            for (int idx = 0;
                                 idx < schema_list->GetSchema(id)->size();
                                 idx++) {
                                std::string str = row_view.GetAsString(idx);
                                t.add(str);
                                if (t.current_columns_size() >=
                                    MAX_DEBUG_COLUMN_MAX) {
                                    break;
                                }
                            }
                            if (t.current_columns_size() >=
                                MAX_DEBUG_COLUMN_MAX) {
                                t.add("...");
                                break;
                            }
                        }
                        segment_iter->Next();
                        t.end_of_row();
                    }
                }

                iter->Next();
            }
            break;
        }
        default: {
            oss << "Invalid Set";
        }
    }
    oss << t;
}

bool Runner::ExtractRows(std::shared_ptr<DataHandlerList> handlers,

                         std::vector<Row>& out_rows) {
    if (!handlers) {
        LOG(WARNING) << "Extract batch rows error: data handler is null";
        return false;
    }
    for (size_t i = 0; i < handlers->GetSize(); i++) {
        auto handler = handlers->Get(i);
        if (!handler) {
            out_rows.push_back(Row());
            continue;
        }
        switch (handler->GetHanlderType()) {
            case kTableHandler: {
                auto iter = std::dynamic_pointer_cast<TableHandler>(handler)
                                ->GetIterator();
                if (!iter) {
                    LOG(WARNING) << "Extract batch rows error: iter is null";
                    return false;
                }
                iter->SeekToFirst();
                while (iter->Valid()) {
                    out_rows.push_back(iter->GetValue());
                    iter->Next();
                }
                break;
            }
            case kRowHandler: {
                out_rows.push_back(
                    std::dynamic_pointer_cast<RowHandler>(handler)->GetValue());
                break;
            }
            default: {
                LOG(WARNING) << "partition output is invalid";
                return false;
            }
        }
    }
    return true;
}
bool Runner::ExtractRow(std::shared_ptr<DataHandler> handler, Row* out_row) {
    switch (handler->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(handler)->GetIterator();
            if (!iter) {
                return false;
            }
            iter->SeekToFirst();
            if (iter->Valid()) {
                *out_row = iter->GetValue();
                return true;
            } else {
                return false;
            }
        }
        case kRowHandler: {
            *out_row =
                std::dynamic_pointer_cast<RowHandler>(handler)->GetValue();
            return true;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "partition output is invalid";
            return false;
        }
        default: {
            return false;
        }
    }
}
bool Runner::ExtractRows(std::shared_ptr<DataHandler> handler,
                         std::vector<Row>& out_rows) {  // NOLINT
    if (!handler) {
        LOG(WARNING) << "Extract batch rows error: data handler is null";
        return false;
    }
    switch (handler->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(handler)->GetIterator();
            if (!iter) {
                LOG(WARNING) << "Extract batch rows error: iter is null";
                return false;
            }
            iter->SeekToFirst();
            while (iter->Valid()) {
                out_rows.push_back(iter->GetValue());
                iter->Next();
            }
            break;
        }
        case kRowHandler: {
            out_rows.push_back(
                std::dynamic_pointer_cast<RowHandler>(handler)->GetValue());
            break;
        }
        default: {
            LOG(WARNING) << "partition output is invalid";
            return false;
        }
    }
    return true;
}
std::shared_ptr<DataHandler> ConcatRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 2) {
        LOG(WARNING) << "inputs size < 2";
        return fail_ptr;
    }
    auto right = inputs[1];
    auto left = inputs[0];
    size_t left_slices = producers_[0]->output_schemas()->GetSchemaSourceSize();
    size_t right_slices =
        producers_[1]->output_schemas()->GetSchemaSourceSize();
    if (!left) {
        return std::shared_ptr<DataHandler>();
    }
    switch (left->GetHanlderType()) {
        case kRowHandler:
            return std::shared_ptr<RowHandler>(new RowCombineWrapper(
                std::dynamic_pointer_cast<RowHandler>(left), left_slices,
                std::dynamic_pointer_cast<RowHandler>(right), right_slices));
        case kTableHandler:
            return std::shared_ptr<TableHandler>(new ConcatTableHandler(
                std::dynamic_pointer_cast<TableHandler>(left), left_slices,
                std::dynamic_pointer_cast<TableHandler>(right), right_slices));
        default: {
            LOG(WARNING)
                << "fail to run conncat runner: handler type unsupported";
            return fail_ptr;
        }
    }
}

std::shared_ptr<DataHandler> LimitRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return fail_ptr;
    }
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    switch (input->GetHanlderType()) {
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(input)->GetIterator();
            if (!iter) {
                LOG(WARNING) << "fail to get table it";
                return fail_ptr;
            }
            iter->SeekToFirst();
            auto output_table = std::shared_ptr<MemTableHandler>(
                new MemTableHandler(input->GetSchema()));
            int32_t cnt = 0;
            while (cnt++ < limit_cnt_ && iter->Valid()) {
                output_table->AddRow(iter->GetValue());
                iter->Next();
            }
            return output_table;
        }
        case kRowHandler: {
            DLOG(INFO) << "limit row handler";
            return input;
        }
        case kPartitionHandler: {
            LOG(WARNING) << "fail limit when input type isn't row or table";
            return fail_ptr;
        }
    }
    return fail_ptr;
}
std::shared_ptr<DataHandler> FilterRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return fail_ptr;
    }
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "fail to run filter: input is empty or null";
        return fail_ptr;
    }
    auto& parameter = ctx.GetParameterRow();
    // build window with start and end offset
    switch (input->GetHanlderType()) {
        case kTableHandler: {
            return filter_gen_.Filter(
                std::dynamic_pointer_cast<TableHandler>(input), parameter);
        }
        case kPartitionHandler: {
            return filter_gen_.Filter(
                std::dynamic_pointer_cast<PartitionHandler>(input), parameter);
        }
        default: {
            LOG(WARNING) << "fail to filter when input is row";
            return fail_ptr;
        }
    }
}

std::shared_ptr<DataHandler> GroupAggRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "group aggregation fail: input is null";
        return std::shared_ptr<DataHandler>();
    }

    if (kPartitionHandler != input->GetHanlderType()) {
        LOG(WARNING) << "group aggregation fail: input isn't partition ";
        return std::shared_ptr<DataHandler>();
    }
    auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
    auto output_table = std::shared_ptr<MemTableHandler>(new MemTableHandler());
    auto iter = partition->GetWindowIterator();
    if (!iter) {
        LOG(WARNING) << "group aggregation fail: input iterator is null";
        return std::shared_ptr<DataHandler>();
    }
    auto& parameter = ctx.GetParameterRow();
    iter->SeekToFirst();
    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
            break;
        }
        auto segment_iter = iter->GetValue();
        if (!segment_iter) {
            LOG(WARNING) << "group aggregation fail: segment iterator is null";
            return std::shared_ptr<DataHandler>();
        }
        auto key = iter->GetKey().ToString();
        auto segment = partition->GetSegment(key);
        output_table->AddRow(agg_gen_.Gen(parameter, segment));
        iter->Next();
    }
    return output_table;
}
std::shared_ptr<DataHandler> RequestUnionRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 2u) {
        LOG(WARNING) << "inputs size < 2";
        return std::shared_ptr<DataHandler>();
    }
    auto left = inputs[0];
    auto right = inputs[1];
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }

    auto request = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();

    int64_t ts_gen = range_gen_.Valid() ? range_gen_.ts_gen_.Gen(request) : -1;

    // Prepare Union Window
    auto union_inputs = windows_union_gen_.RunInputs(ctx);
    auto union_segments =
        windows_union_gen_.GetRequestWindows(request, ctx.GetParameterRow(), union_inputs);
    // build window with start and end offset
    return RequestUnionWindow(request, union_segments, ts_gen,
                              range_gen_.window_range_, output_request_row_,
                              exclude_current_time_);
}
std::shared_ptr<TableHandler> RequestUnionRunner::RequestUnionWindow(
    const Row& request,
    std::vector<std::shared_ptr<TableHandler>> union_segments, int64_t ts_gen,
    const WindowRange& window_range, const bool output_request_row,
    const bool exclude_current_time) {
    uint64_t start = 0;
    uint64_t end = UINT64_MAX;
    uint64_t rows_start_preceding = 0;
    uint64_t max_size = 0;
    if (ts_gen >= 0) {
        start = (ts_gen + window_range.start_offset_) < 0
                    ? 0
                    : (ts_gen + window_range.start_offset_);
        if (exclude_current_time && 0 == window_range.end_offset_) {
            end = (ts_gen - 1) < 0 ? 0 : (ts_gen - 1);
        } else {
            end = (ts_gen + window_range.end_offset_) < 0
                      ? 0
                      : (ts_gen + window_range.end_offset_);
        }
        rows_start_preceding = window_range.start_row_;
        max_size = window_range.max_size_;
    }
    uint64_t request_key = ts_gen > 0 ? static_cast<uint64_t>(ts_gen) : 0;

    auto window_table =
        std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());

    size_t unions_cnt = union_segments.size();
    // Prepare Union Segment Iterators
    std::vector<std::unique_ptr<RowIterator>> union_segment_iters(unions_cnt);
    std::vector<IteratorStatus> union_segment_status(unions_cnt);

    for (size_t i = 0; i < unions_cnt; i++) {
        if (!union_segments[i]) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i] = union_segments[i]->GetIterator();
        if (!union_segment_iters[i]) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        union_segment_iters[i]->Seek(end);
        if (!union_segment_iters[i]->Valid()) {
            union_segment_status[i] = IteratorStatus();
            continue;
        }
        uint64_t ts = union_segment_iters[i]->GetKey();
        union_segment_status[i] = IteratorStatus(ts);
    }
    int32_t max_union_pos = 0 == unions_cnt
                                ? -1
                                : IteratorStatus::PickIteratorWithMaximizeKey(
                                      &union_segment_status);
    uint64_t cnt = 0;
    auto range_status = window_range.GetWindowPositionStatus(
        cnt > rows_start_preceding, window_range.end_offset_ < 0,
        request_key < start);
    if (output_request_row) {
        window_table->AddRow(request_key, request);
    }
    if (WindowRange::kInWindow == range_status) {
        cnt++;
    }

    while (-1 != max_union_pos) {
        if (max_size > 0 && cnt >= max_size) {
            break;
        }
        auto range_status = window_range.GetWindowPositionStatus(
            cnt > rows_start_preceding,
            union_segment_status[max_union_pos].key_ > end,
            union_segment_status[max_union_pos].key_ < start);
        if (WindowRange::kExceedWindow == range_status) {
            break;
        }
        if (WindowRange::kInWindow == range_status) {
            window_table->AddRow(
                union_segment_status[max_union_pos].key_,
                union_segment_iters[max_union_pos]->GetValue());
            cnt++;
        }
        // Update Iterator Status
        union_segment_iters[max_union_pos]->Next();
        if (!union_segment_iters[max_union_pos]->Valid()) {
            union_segment_status[max_union_pos].MarkInValid();
        } else {
            union_segment_status[max_union_pos].set_key(
                union_segment_iters[max_union_pos]->GetKey());
        }
        // Pick new mininum union pos
        max_union_pos =
            IteratorStatus::PickIteratorWithMaximizeKey(&union_segment_status);
    }
    DLOG(INFO) << "REQUEST UNION cnt = " << window_table->GetCount();
    return window_table;
}

std::shared_ptr<DataHandler> PostRequestUnionRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    if (inputs.size() < 2u) {
        LOG(WARNING) << "inputs size < 2";
        return std::shared_ptr<DataHandler>();
    }
    auto left = inputs[0];
    auto right = inputs[1];
    if (!left || !right) {
        return nullptr;
    }
    auto request = std::dynamic_pointer_cast<RowHandler>(left);
    if (!request) {
        LOG(WARNING) << "Post request union left input is not valid";
        return nullptr;
    }
    const Row request_row = request->GetValue();
    int64_t request_key = request_ts_gen_.Gen(request_row);

    auto window_table = std::dynamic_pointer_cast<TableHandler>(right);
    if (!window_table) {
        LOG(WARNING) << "Post request union right input is not valid";
        return nullptr;
    }
    return std::make_shared<RequestUnionTableHandler>(request_key, request_row,
                                                      window_table);
}

std::shared_ptr<DataHandler> AggRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return std::shared_ptr<DataHandler>();
    }
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "input is empty";
        return std::shared_ptr<DataHandler>();
    }
    if (kTableHandler != input->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    auto row_handler = std::shared_ptr<RowHandler>(new MemRowHandler(
        agg_gen_.Gen(ctx.GetParameterRow(), std::dynamic_pointer_cast<TableHandler>(input))));
    return row_handler;
}
std::shared_ptr<DataHandlerList> ProxyRequestRunner::BatchRequestRun(
    RunnerContext& ctx) {
    if (need_cache_) {
        auto cached = ctx.GetBatchCache(id_);
        if (cached != nullptr) {
            DLOG(INFO) << "RUNNER ID " << id_ << " HIT CACHE!";
            return cached;
        }
    }
    std::shared_ptr<DataHandlerList> proxy_batch_input =
        producers_[0]->BatchRequestRun(ctx);
    std::shared_ptr<DataHandlerList> index_key_input =
        std::shared_ptr<DataHandlerList>();
    if (nullptr != index_input_) {
        index_key_input = index_input_->BatchRequestRun(ctx);
    }
    if (!proxy_batch_input || 0 == proxy_batch_input->GetSize()) {
        LOG(WARNING) << "proxy batch run input is empty";
        return std::shared_ptr<DataHandlerList>();
    }
    // if need batch cache_, we only need to compute the first line
    // and repeat the output
    if (need_batch_cache_) {
        std::shared_ptr<DataHandlerVector> proxy_one_row_batch_input =
            std::make_shared<DataHandlerVector>();
        proxy_one_row_batch_input->Add(proxy_batch_input->Get(0));

        std::shared_ptr<DataHandlerVector> one_index_key_input =
            std::shared_ptr<DataHandlerVector>();
        if (index_key_input) {
            std::shared_ptr<DataHandlerVector> one_index_key_input =
                std::make_shared<DataHandlerVector>();
            one_index_key_input->Add(index_key_input->Get(0));
        }
        auto res =
            RunBatchInput(ctx, proxy_one_row_batch_input, one_index_key_input);

        if (ctx.is_debug()) {
            std::ostringstream oss;
            oss << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
                << " HIT BATCH CACHE!"
                << "\n";
            Runner::PrintData(oss, output_schemas_, res->Get(0));
            LOG(INFO) << oss.str();
        }
        auto repeated_data = std::shared_ptr<DataHandlerList>(
            new DataHandlerRepeater(res->Get(0), proxy_batch_input->GetSize()));
        if (need_cache_) {
            ctx.SetBatchCache(id_, repeated_data);
        }
        return repeated_data;
    }

    // if not need batch cache
    // compute each line
    auto outputs = RunBatchInput(ctx, proxy_batch_input, index_key_input);
    if (ctx.is_debug()) {
        std::ostringstream oss;
        oss << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
            << "\n";
        for (size_t idx = 0; idx < outputs->GetSize(); idx++) {
            if (idx >= MAX_DEBUG_BATCH_SiZE) {
                oss << ">= MAX_DEBUG_BATCH_SiZE...\n";
                break;
            }
            Runner::PrintData(oss, output_schemas_, outputs->Get(idx));
        }
        LOG(INFO) << oss.str();
    }
    if (need_cache_) {
        ctx.SetBatchCache(id_, outputs);
    }
    return outputs;
}

// run each line of request
std::shared_ptr<DataHandler> ProxyRequestRunner::Run(
    RunnerContext& ctx,
    const std::vector<std::shared_ptr<DataHandler>>& inputs) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    // proxy input, can be row or rows
    if (inputs.size() < 1u) {
        LOG(WARNING) << "inputs size < 1";
        return fail_ptr;
    }
    auto input = inputs[0];
    if (!input) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    std::shared_ptr<DataHandler> index_input = std::shared_ptr<DataHandler>();
    if (nullptr != index_input_) {
        index_input = index_input_->RunWithCache(ctx);
    }
    switch (input->GetHanlderType()) {
        case kRowHandler: {
            auto row = std::dynamic_pointer_cast<RowHandler>(input)->GetValue();
            if (index_input) {
                auto index_row =
                    std::dynamic_pointer_cast<RowHandler>(index_input)
                        ->GetValue();
                return RunWithRowInput(ctx, row, index_row);

            } else {
                return RunWithRowInput(ctx, row, row);
            }
        }
        case kTableHandler: {
            auto iter =
                std::dynamic_pointer_cast<TableHandler>(input)->GetIterator();
            if (!iter) {
                LOG(WARNING)
                    << "fail to run proxy runner with rows: table iter null"
                    << task_id_;
                return fail_ptr;
            }
            iter->SeekToFirst();
            std::vector<Row> rows;
            while (iter->Valid()) {
                rows.push_back(iter->GetValue());
                iter->Next();
            }
            if (index_input) {
                std::vector<Row> index_rows;
                if (!ExtractRows(index_input, index_rows)) {
                    LOG(WARNING) << "run proxy runner extract rows fail";
                    return fail_ptr;
                }
                return RunWithRowsInput(ctx, rows, index_rows,
                                        producers_[0]->need_batch_cache());
            } else {
                return RunWithRowsInput(ctx, rows, rows,
                                        producers_[0]->need_batch_cache());
            }
        }
        default: {
            LOG(WARNING)
                << "fail to run proxy runner: handler type unsupported";
            return fail_ptr;
        }
    }
    return fail_ptr;
}
// outs = Proxy(in_rows),  remote batch request
// out_tables = Proxy(in_tables), remote batch request
std::shared_ptr<DataHandlerList> ProxyRequestRunner::RunBatchInput(
    RunnerContext& ctx,  // NOLINT
    std::shared_ptr<DataHandlerList> batch_input,
    std::shared_ptr<DataHandlerList> batch_index_input) {
    auto fail_ptr = std::shared_ptr<DataHandlerList>();
    // proxy input, can be row or rows
    if (!batch_input || 0 == batch_input->GetSize()) {
        LOG(WARNING) << "input is empty";
        return fail_ptr;
    }
    switch (batch_input->Get(0)->GetHanlderType()) {
        case kRowHandler: {
            bool input_batch_is_common = producers_[0]->need_batch_cache();
            if (input_batch_is_common || 1 == batch_input->GetSize()) {
                Row row;
                if (!ExtractRow(batch_input->Get(0), &row)) {
                    LOG(WARNING) << "run proxy runner with rows fail, batch "
                                    "rows is empty";
                    return fail_ptr;
                }
                std::vector<Row> rows({row});
                std::shared_ptr<TableHandler> table =
                    std::shared_ptr<TableHandler>();
                Row index_row;
                if (batch_index_input) {
                    if (!ExtractRow(batch_index_input->Get(0), &index_row)) {
                        LOG(WARNING)
                            << "run proxy runner extract index rows fail";
                        return fail_ptr;
                    }
                    table = RunWithRowsInput(ctx, rows,
                                             std::vector<Row>({index_row}),
                                             input_batch_is_common);
                } else {
                    index_row = row;
                    table = RunWithRowsInput(ctx, rows, rows,
                                             input_batch_is_common);
                }
                if (!table) {
                    LOG(WARNING) << "run proxy runner with rows fail, result "
                                    "table is null";
                    return fail_ptr;
                }

                std::shared_ptr<DataHandlerRepeater> outputs =
                    std::make_shared<DataHandlerRepeater>(
                        std::make_shared<AysncRowHandler>(0, table),
                        batch_input->GetSize());
                return outputs;
            } else {
                std::vector<Row> rows;

                if (!ExtractRows(batch_input, rows)) {
                    LOG(WARNING) << "run proxy runner with rows fail, batch "
                                    "rows is empty";
                    return fail_ptr;
                }
                std::shared_ptr<TableHandler> table =
                    std::shared_ptr<TableHandler>();
                if (batch_index_input) {
                    std::vector<Row> index_rows;
                    if (!ExtractRows(batch_index_input, index_rows)) {
                        LOG(WARNING) << "run proxy runner extract index rows";
                        return fail_ptr;
                    }
                    table = RunWithRowsInput(ctx, rows, index_rows,
                                             input_batch_is_common);
                } else {
                    table = RunWithRowsInput(ctx, rows, rows,
                                             input_batch_is_common);
                }

                if (!table) {
                    LOG(WARNING) << "run proxy runner with rows fail, result "
                                    "table is null";
                    return fail_ptr;
                }

                std::shared_ptr<DataHandlerVector> outputs =
                    std::make_shared<DataHandlerVector>();
                for (size_t idx = 0; idx < rows.size(); idx++) {
                    outputs->Add(std::make_shared<AysncRowHandler>(idx, table));
                }
                return outputs;
            }
        }
        case kTableHandler: {
            std::shared_ptr<DataHandlerVector> outputs =
                std::make_shared<DataHandlerVector>();
            for (size_t idx = 0; idx < batch_input->GetSize(); idx++) {
                std::vector<Row> rows;
                if (!ExtractRows(batch_input->Get(idx), rows)) {
                    LOG(WARNING) << "run proxy runner with rows fail, batch "
                                    "rows is empty";
                    return fail_ptr;
                }
                if (batch_index_input) {
                    std::vector<Row> index_rows;
                    if (!ExtractRows(batch_index_input->Get(idx), index_rows)) {
                        LOG(WARNING)
                            << "run proxy runner extract index rows fail";
                        return fail_ptr;
                    }
                    outputs->Add(
                        RunWithRowsInput(ctx, rows, index_rows, false));
                } else {
                    outputs->Add(RunWithRowsInput(ctx, rows, rows, false));
                }
            }
            return outputs;
        }
        default: {
            LOG(WARNING)
                << "fail to run proxy runner: handler type unsupported";
            return fail_ptr;
        }
    }
    return fail_ptr;
}

// out = Proxy(in_row)
// out_table = Proxy(in_table) , remote table left join
std::shared_ptr<DataHandler> ProxyRequestRunner::RunWithRowInput(
    RunnerContext& ctx,  // NOLINT
    const Row& row, const Row& index_row) {
    auto fail_ptr = std::shared_ptr<DataHandler>();
    auto cluster_job = ctx.cluster_job();
    if (nullptr == cluster_job) {
        LOG(WARNING) << "fail to run proxy runner: invalid cluster job ptr";
        return fail_ptr;
    }
    auto task = cluster_job->GetTask(task_id_);
    if (!task.IsValid()) {
        LOG(WARNING) << "fail to run proxy runner: invalid task of taskid "
                     << task_id_;
        return fail_ptr;
    }
    std::string pk = "";
    if (!task.GetIndexKey().ValidKey()) {
        LOG(WARNING) << "can't pick tablet to subquery without index";
        return std::shared_ptr<DataHandler>();
    }
    KeyGenerator generator(task.GetIndexKey().fn_info());
    pk = generator.Gen(index_row, ctx.GetParameterRow());
    if (pk.empty()) {
        // local mode
        LOG(WARNING) << "can't pick tablet to subquery with empty pk";
        return std::shared_ptr<DataHandler>();
    }
    DLOG(INFO) << "pick tablet with given index_name " << task.index() << " pk "
               << pk;
    auto table_handler = task.table_handler();
    if (!table_handler) {
        LOG(WARNING) << "remote task related table handler is null";
        return std::shared_ptr<DataHandler>();
    }
    auto tablet = table_handler->GetTablet(task.index(), pk);
    if (!tablet) {
        LOG(WARNING) << "fail to run proxy runner with row: tablet is null";
        return std::shared_ptr<DataHandler>();
    } else {
        if (row.GetRowPtrCnt() > 1) {
            LOG(WARNING) << "subquery with multi slice row is "
                            "unsupported currently";
            return std::shared_ptr<DataHandler>();
        }
        if (ctx.sp_name().empty()) {
            return tablet->SubQuery(task_id_, table_handler->GetDatabase(),
                                    cluster_job->sql(), row, false,
                                    ctx.is_debug());
        } else {
            return tablet->SubQuery(task_id_, table_handler->GetDatabase(),
                                    ctx.sp_name(), row, true, ctx.is_debug());
        }
    }
}
// out_table = Proxy(in_table) , remote table left join
std::shared_ptr<TableHandler> ProxyRequestRunner::RunWithRowsInput(
    RunnerContext& ctx,  // NOLINT
    const std::vector<Row>& rows, const std::vector<Row>& index_rows,
    const bool request_is_common) {
    // basic cluster task validate
    auto fail_ptr = std::shared_ptr<TableHandler>();

    auto cluster_job = ctx.cluster_job();
    if (nullptr == cluster_job) {
        LOG(WARNING) << "fail to run proxy runner: invalid cluster job ptr";
        return fail_ptr;
    }
    auto task = cluster_job->GetTask(task_id_);
    if (!task.IsValid()) {
        LOG(WARNING)
            << "fail to run proxy runner with rows: invalid task of taskid "
            << task_id_;
        return fail_ptr;
    }
    auto table_handler = task.table_handler();
    if (!table_handler) {
        LOG(WARNING) << "table handler is null";
        return fail_ptr;
    }
    auto &parameter = ctx.GetParameterRow();
    // collect pk list from rows
    std::shared_ptr<Tablet> tablet = std::shared_ptr<Tablet>();
    KeyGenerator generator(task.GetIndexKey().fn_info());
    if (request_is_common) {
        std::string pk = generator.Gen(index_rows[0], ctx.GetParameterRow());
        tablet = table_handler->GetTablet(task.index(), pk);
    } else {
        std::vector<std::string> pks;
        for (auto& index_row : index_rows) {
            pks.push_back(generator.Gen(index_row, parameter));
        }
        tablet = table_handler->GetTablet(task.index(), pks);
    }
    if (!tablet) {
        LOG(WARNING)
            << "fail to run proxy runner with rows: subquery tablet is null";
        return fail_ptr;
    }
    if (ctx.sp_name().empty()) {
        return tablet->SubQuery(task_id_, table_handler->GetDatabase(),
                                cluster_job->sql(),
                                ctx.cluster_job()->common_column_indices(),
                                rows, request_is_common, false, ctx.is_debug());
    } else {
        return tablet->SubQuery(task_id_, table_handler->GetDatabase(),
                                ctx.sp_name(),
                                ctx.cluster_job()->common_column_indices(),
                                rows, request_is_common, true, ctx.is_debug());
    }
    return fail_ptr;
}

/**
 * TODO(chenjing): GenConst key during compile-time
 * @return
 */
const std::string KeyGenerator::GenConst(const Row& parameter) {
    Row key_row = CoreAPI::RowConstProject(fn_, parameter, true);
    RowView row_view(row_view_);
    if (!row_view.Reset(key_row.buf())) {
        LOG(WARNING) << "fail to gen key: row view reset fail";
        return "NA";
    }
    std::string keys = "";
    for (auto pos : idxs_) {
        std::string key =
            row_view.IsNULL(pos)
                ? codec::NONETOKEN
                : fn_schema_.Get(pos).type() == hybridse::type::kDate
                      ? std::to_string(row_view.GetDateUnsafe(pos))
                      : row_view.GetAsString(pos);
        if (key == "") {
            key = codec::EMPTY_STRING;
        }
        if (!keys.empty()) {
            keys.append("|");
        }
        keys.append(key);
    }
    return keys;
}
const std::string KeyGenerator::Gen(const Row& row, const Row& parameter) {
    // TODO(wtz) 避免不必要的row project
    if (row.size() == 0) {
        return codec::NONETOKEN;
    }
    Row key_row = CoreAPI::RowProject(fn_, row, parameter, true);
    std::string keys = "";
    for (auto pos : idxs_) {
        if (!keys.empty()) {
            keys.append("|");
        }
        if (row_view_.IsNULL(key_row.buf(), pos)) {
            keys.append(codec::NONETOKEN);
            continue;
        }
        ::hybridse::type::Type type = fn_schema_.Get(pos).type();
        switch (type) {
            case ::hybridse::type::kVarchar: {
                const char* buf = nullptr;
                uint32_t size = 0;
                if (row_view_.GetValue(key_row.buf(), pos, &buf, &size) == 0) {
                    if (size == 0) {
                        keys.append(codec::EMPTY_STRING.c_str(),
                                    codec::EMPTY_STRING.size());
                    } else {
                        keys.append(buf, size);
                    }
                }
                break;
            }
            case hybridse::type::kDate: {
                int32_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(std::to_string(buf));
                }
                break;
            }
            case hybridse::type::kBool: {
                bool buf = false;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0) {
                    keys.append(buf ? "true" : "false");
                }
                break;
            }
            case hybridse::type::kInt16: {
                int16_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0)
                    keys.append(std::to_string(buf));
                break;
            }
            case hybridse::type::kInt32: {
                int32_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0)
                    keys.append(std::to_string(buf));
                break;
            }
            case hybridse::type::kInt64:
            case hybridse::type::kTimestamp: {
                int64_t buf = 0;
                if (row_view_.GetValue(key_row.buf(), pos, type,
                                       reinterpret_cast<void*>(&buf)) == 0)
                    keys.append(std::to_string(buf));
                break;
            }
            default:
                continue;
        }
    }
    return keys;
}

const int64_t OrderGenerator::Gen(const Row& row) {
    Row order_row = CoreAPI::RowProject(fn_, row, Row(), true);
    return Runner::GetColumnInt64(order_row.buf(), &row_view_, idxs_[0],
                                  fn_schema_.Get(idxs_[0]).type());
}

const bool ConditionGenerator::Gen(const Row& row, const Row& parameter) const {
    return CoreAPI::ComputeCondition(fn_, row, parameter, &row_view_, idxs_[0]);
}
const Row ProjectGenerator::Gen(const Row& row, const Row& parameter) {
    return CoreAPI::RowProject(fn_, row, parameter, false);
}

const Row ConstProjectGenerator::Gen(const Row& parameter) {
    return CoreAPI::RowConstProject(fn_, parameter, false);
}

const Row AggGenerator::Gen(const codec::Row& parameter_row, std::shared_ptr<TableHandler> table) {
    return Runner::GroupbyProject(fn_, parameter_row, table.get());
}

Row Runner::GroupbyProject(const int8_t* fn, const codec::Row& parameter, TableHandler* table) {
    auto iter = table->GetIterator();
    if (!iter) {
        LOG(WARNING) << "Agg table is empty";
        return Row();
    }
    iter->SeekToFirst();
    if (!iter->Valid()) {
        return Row();
    }
    auto& row = iter->GetValue();
    auto& row_key = iter->GetKey();
    auto udf = reinterpret_cast<int32_t (*)(const int64_t, const int8_t*,
                                            const int8_t*, const int8_t*, int8_t**)>(
        const_cast<int8_t*>(fn));
    int8_t* buf = nullptr;

    auto row_ptr = reinterpret_cast<const int8_t*>(&row);
    auto parameter_ptr = reinterpret_cast<const int8_t*>(&parameter);

    codec::ListRef<Row> window_ref;
    window_ref.list = reinterpret_cast<int8_t*>(table);
    auto window_ptr = reinterpret_cast<const int8_t*>(&window_ref);

    uint32_t ret = udf(row_key, row_ptr, window_ptr, parameter_ptr, &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(
        base::RefCountedSlice::CreateManaged(buf, RowView::GetSize(buf)));
}

const Row WindowProjectGenerator::Gen(const uint64_t key, const Row row,
                                      const codec::Row& parameter,
                                      bool is_instance, size_t append_slices,
                                      Window* window) {
    return Runner::WindowProject(fn_, key, row, parameter, is_instance, append_slices,
                                 window);
}

std::vector<std::shared_ptr<DataHandler>> InputsGenerator::RunInputs(
    RunnerContext& ctx) {
    std::vector<std::shared_ptr<DataHandler>> union_inputs;
    if (!input_runners_.empty()) {
        for (auto runner : input_runners_) {
            union_inputs.push_back(runner->RunWithCache(ctx));
        }
    }
    return union_inputs;
}
std::vector<std::shared_ptr<PartitionHandler>>
WindowUnionGenerator::PartitionEach(
    std::vector<std::shared_ptr<DataHandler>> union_inputs,
    const Row& parameter) {
    std::vector<std::shared_ptr<PartitionHandler>> union_partitions;
    if (!windows_gen_.empty()) {
        union_partitions.reserve(windows_gen_.size());
        for (size_t i = 0; i < inputs_cnt_; i++) {
            union_partitions.push_back(
                windows_gen_[i].partition_gen_.Partition(union_inputs[i], parameter));
        }
    }
    return union_partitions;
}
int32_t IteratorStatus::PickIteratorWithMininumKey(
    std::vector<IteratorStatus>* status_list_ptr) {
    const auto& status_list = *status_list_ptr;
    int32_t min_union_pos = -1;
    uint64_t min_union_order = UINT64_MAX;
    for (size_t i = 0; i < status_list.size(); i++) {
        if (status_list[i].is_valid_ && status_list[i].key_ < min_union_order) {
            min_union_order = status_list[i].key_;
            min_union_pos = static_cast<int32_t>(i);
        }
    }
    return min_union_pos;
}
int32_t IteratorStatus::PickIteratorWithMaximizeKey(
    std::vector<IteratorStatus>* status_list_ptr) {
    const auto& status_list = *status_list_ptr;
    int32_t min_union_pos = -1;
    uint64_t min_union_order = 0;
    for (size_t i = 0; i < status_list.size(); i++) {
        if (status_list[i].is_valid_ &&
            status_list[i].key_ >= min_union_order) {
            min_union_order = status_list[i].key_;
            min_union_pos = static_cast<int32_t>(i);
        }
    }
    return min_union_pos;
}
std::vector<std::shared_ptr<DataHandler>> WindowJoinGenerator::RunInputs(
    RunnerContext& ctx) {
    std::vector<std::shared_ptr<DataHandler>> union_inputs;
    if (!input_runners_.empty()) {
        for (auto runner : input_runners_) {
            union_inputs.push_back(runner->RunWithCache(ctx));
        }
    }
    return union_inputs;
}
Row WindowJoinGenerator::Join(
    const Row& left_row,
    const std::vector<std::shared_ptr<DataHandler>>& join_right_tables,
    const Row& parameter) {
    Row row = left_row;
    for (size_t i = 0; i < join_right_tables.size(); i++) {
        row = joins_gen_[i].RowLastJoin(row, join_right_tables[i], parameter);
    }
    return row;
}

std::shared_ptr<TableHandler> IndexSeekGenerator::SegmnetOfConstKey(
    const Row& parameter,
    std::shared_ptr<DataHandler> input) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!input) {
        LOG(WARNING) << "fail to seek segment of key: input is empty";
        return fail_ptr;
    }
    if (!index_key_gen_.Valid()) {
        switch (input->GetHanlderType()) {
            case kPartitionHandler: {
                LOG(WARNING) << "fail to seek segment: index key is empty";
                return fail_ptr;
            }
            case kTableHandler: {
                return std::dynamic_pointer_cast<TableHandler>(input);
            }
            default: {
                LOG(WARNING) << "fail to seek segment when input is row";
                return fail_ptr;
            }
        }
    }

    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
            auto key = index_key_gen_.GenConst(parameter);
            return partition->GetSegment(key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}
std::shared_ptr<TableHandler> IndexSeekGenerator::SegmentOfKey(
    const Row& row, const Row& parameter, std::shared_ptr<DataHandler> input) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!input) {
        LOG(WARNING) << "fail to seek segment of key: input is empty";
        return fail_ptr;
    }
    if (row.empty()) {
        LOG(WARNING) << "fail to seek segment: key row is empty";
        return fail_ptr;
    }

    if (!index_key_gen_.Valid()) {
        switch (input->GetHanlderType()) {
            case kPartitionHandler: {
                LOG(WARNING) << "fail to seek segment: index key is empty";
                return fail_ptr;
            }
            case kTableHandler: {
                return std::dynamic_pointer_cast<TableHandler>(input);
            }
            default: {
                LOG(WARNING) << "fail to seek segment when input is row";
                return fail_ptr;
            }
        }
    }

    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(input);
            auto key = index_key_gen_.Gen(row, parameter);
            return partition->GetSegment(key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}

std::shared_ptr<TableHandler> FilterGenerator::Filter(
    std::shared_ptr<PartitionHandler> table, const Row& parameter) {
    return Filter(index_seek_gen_.SegmnetOfConstKey(parameter, table), parameter);
}
std::shared_ptr<TableHandler> FilterGenerator::Filter(
    std::shared_ptr<TableHandler> table,
    const Row& parameter) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!table) {
        LOG(WARNING) << "fail to filter table: input is empty";
        return fail_ptr;
    }

    if (!condition_gen_.Valid()) {
        return table;
    }
    return std::shared_ptr<TableHandler>(new TableFilterWrapper(table, parameter, this));
}

std::shared_ptr<DataHandlerList> RunnerContext::GetBatchCache(
    int64_t id) const {
    auto iter = batch_cache_.find(id);
    if (iter == batch_cache_.end()) {
        return std::shared_ptr<DataHandlerList>();
    } else {
        return iter->second;
    }
}

void RunnerContext::SetBatchCache(int64_t id,
                                  std::shared_ptr<DataHandlerList> data) {
    batch_cache_[id] = data;
}

std::shared_ptr<DataHandler> RunnerContext::GetCache(int64_t id) const {
    auto iter = cache_.find(id);
    if (iter == cache_.end()) {
        return std::shared_ptr<DataHandler>();
    } else {
        return iter->second;
    }
}

void RunnerContext::SetCache(int64_t id,
                             const std::shared_ptr<DataHandler> data) {
    cache_[id] = data;
}

void RunnerContext::SetRequest(const hybridse::codec::Row& request) {
    request_ = request;
}
void RunnerContext::SetRequests(
    const std::vector<hybridse::codec::Row>& requests) {
    requests_ = requests;
}
}  // namespace vm
}  // namespace hybridse
