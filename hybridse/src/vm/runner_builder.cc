/**
 * Copyright (c) 2023 OpenMLDB authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "vm/runner_builder.h"
#include "vm/physical_op.h"

namespace hybridse {
namespace vm {

static vm::PhysicalDataProviderNode* request_node(vm::PhysicalOpNode* n) {
    switch (n->GetOpType()) {
        case kPhysicalOpDataProvider:
            return dynamic_cast<vm::PhysicalDataProviderNode*>(n);
        default:
            return request_node(n->GetProducer(0));
    }
}

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
        status.code = common::kExecutionPlanError;
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
                    auto provider = dynamic_cast<const PhysicalTableProviderNode*>(node);
                    DataRunner* runner = CreateRunner<DataRunner>(id_++, node->schemas_ctx(), provider->table_handler_);
                    return RegisterTask(node, CommonTask(runner));
                }
                case kProviderTypePartition: {
                    auto provider = dynamic_cast<const PhysicalPartitionProviderNode*>(node);
                    DataRunner* runner = CreateRunner<DataRunner>(
                        id_++, node->schemas_ctx(), provider->table_handler_->GetPartition(provider->index_name_));
                    if (support_cluster_optimized_) {
                        return RegisterTask(
                            node, UnCompletedClusterTask(runner, provider->table_handler_, provider->index_name_));
                    } else {
                        return RegisterTask(node, CommonTask(runner));
                    }
                }
                case kProviderTypeRequest: {
                    RequestRunner* runner = CreateRunner<RequestRunner>(id_++, node->schemas_ctx());
                    return RegisterTask(node, BuildRequestTask(runner));
                }
                default: {
                    status.msg = "fail to support data provider type " + DataProviderTypeName(op->provider_type_);
                    status.code = common::kExecutionPlanError;
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpSimpleProject: {
            auto cluster_task = Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner for simple project:\n" + node->GetTreeString();
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalSimpleProjectNode*>(node);
            int select_slice = op->GetSelectSourceIndex();
            if (select_slice >= 0) {
                SelectSliceRunner* runner =
                    CreateRunner<SelectSliceRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), select_slice);
                return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
            } else {
                SimpleProjectRunner* runner = CreateRunner<SimpleProjectRunner>(
                    id_++, node->schemas_ctx(), op->GetLimitCnt(), op->project().fn_info());
                return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
            }
        }
        case kPhysicalOpConstProject: {
            auto op = dynamic_cast<const PhysicalConstProjectNode*>(node);
            ConstProjectRunner* runner = CreateRunner<ConstProjectRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(),
                                                                          op->project().fn_info());
            return RegisterTask(node, CommonTask(runner));
        }
        case kPhysicalOpProject: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto input = cluster_task.GetRoot();
            auto op = dynamic_cast<const PhysicalProjectNode*>(node);
            switch (op->project_type_) {
                case kTableProject: {
                    if (support_cluster_optimized_) {
                        // Non-support table join under distribution env
                        status.msg = "fail to build cluster with table project";
                        status.code = common::kExecutionPlanError;
                        LOG(WARNING) << status;
                        return fail;
                    }
                    TableProjectRunner* runner = CreateRunner<TableProjectRunner>(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(), op->project().fn_info());
                    return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
                }
                case kReduceAggregation: {
                    ReduceRunner* runner = CreateRunner<ReduceRunner>(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        dynamic_cast<const PhysicalReduceAggregationNode*>(node)->having_condition_,
                        op->project().fn_info());
                    return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
                }
                case kAggregation: {
                    auto agg_node = dynamic_cast<const PhysicalAggregationNode*>(node);
                    if (agg_node == nullptr) {
                        status.msg = "fail to build AggRunner: input node is not PhysicalAggregationNode";
                        status.code = common::kExecutionPlanError;
                        return fail;
                    }
                    AggRunner* runner = CreateRunner<AggRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(),
                                                                agg_node->having_condition_, op->project().fn_info());
                    return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
                }
                case kGroupAggregation: {
                    if (support_cluster_optimized_) {
                        // Non-support group aggregation under distribution env
                        status.msg = "fail to build cluster with group agg project";
                        status.code = common::kExecutionPlanError;
                        LOG(WARNING) << status;
                        return fail;
                    }
                    auto op = dynamic_cast<const PhysicalGroupAggrerationNode*>(node);
                    GroupAggRunner* runner =
                        CreateRunner<GroupAggRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), op->group_,
                                                     op->having_condition_, op->project().fn_info());
                    return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
                }
                case kWindowAggregation: {
                    if (support_cluster_optimized_) {
                        // Non-support table window aggregation join under distribution env
                        status.msg = "fail to build cluster with window agg project";
                        status.code = common::kExecutionPlanError;
                        LOG(WARNING) << status;
                        return fail;
                    }
                    auto op = dynamic_cast<const PhysicalWindowAggrerationNode*>(node);
                    WindowAggRunner* runner = CreateRunner<WindowAggRunner>(
                        id_++, op->schemas_ctx(), op->GetLimitCnt(), op->window_, op->project().fn_info(),
                        op->instance_not_in_window(), op->exclude_current_time(),
                        op->need_append_input() ? node->GetProducer(0)->schemas_ctx()->GetSchemaSourceSize() : 0);
                    size_t input_slices = input->output_schemas()->GetSchemaSourceSize();
                    if (!op->window_unions_.Empty()) {
                        for (auto window_union : op->window_unions_.window_unions_) {
                            auto union_task = Build(window_union.first, status);
                            auto union_table = union_task.GetRoot();
                            if (nullptr == union_table) {
                                return RegisterTask(node, fail);
                            }
                            runner->AddWindowUnion(window_union.second, union_table);
                        }
                    }
                    if (!op->window_joins_.Empty()) {
                        for (auto& window_join : op->window_joins_.window_joins_) {
                            auto join_task =  // NOLINT
                                Build(window_join.first, status);
                            auto join_right_runner = join_task.GetRoot();
                            if (nullptr == join_right_runner) {
                                return RegisterTask(node, fail);
                            }
                            runner->AddWindowJoin(window_join.second, input_slices, join_right_runner);
                        }
                    }
                    return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
                }
                case kRowProject: {
                    RowProjectRunner* runner = CreateRunner<RowProjectRunner>(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(), op->project().fn_info());
                    return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
                }
                default: {
                    status.msg = "fail to support project type " + ProjectTypeName(op->project_type_);
                    status.code = common::kExecutionPlanError;
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpRequestUnion: {
            auto left_task = Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right_task = Build(node->producers().at(1), status);
            auto right = right_task.GetRoot();
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalRequestUnionNode*>(node);
            RequestUnionRunner* runner =
                CreateRunner<RequestUnionRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), op->window().range_,
                                                 op->exclude_current_time(), op->output_request_row());
            Key index_key;
            if (!op->instance_not_in_window()) {
                runner->AddWindowUnion(op->window_, right);
                index_key = op->window_.index_key_;
            }
            if (!op->window_unions_.Empty()) {
                for (auto window_union : op->window_unions_.window_unions_) {
                    auto union_task = Build(window_union.first, status);
                    if (!status.isOK()) {
                        LOG(WARNING) << status;
                        return fail;
                    }
                    auto union_table = union_task.GetRoot();
                    if (nullptr == union_table) {
                        return RegisterTask(node, fail);
                    }
                    runner->AddWindowUnion(window_union.second, union_table);
                    if (!index_key.ValidKey()) {
                        index_key = window_union.second.index_key_;
                        right_task = union_task;
                        right_task.SetRoot(right);
                    }
                }
            }
            if (support_cluster_optimized_) {
                if (node->GetOutputType() == kSchemaTypeGroup) {
                    // route by index of the left source, and it should uncompleted
                    auto& route_info = left_task.GetRouteInfo();
                    runner->AddProducer(left_task.GetRoot());
                    runner->AddProducer(right_task.GetRoot());
                    return RegisterTask(node, ClusterTask(runner, {}, route_info));
                }
            }
            return RegisterTask(node, BinaryInherit(left_task, right_task, runner, index_key, kRightBias));
        }
        case kPhysicalOpRequestAggUnion: {
            return BuildRequestAggUnionTask(node, status);
        }
        case kPhysicalOpRequestJoin: {
            auto left_task = Build(node->GetProducer(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner for: " + node->GetProducer(0)->GetTreeString();
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto left = left_task.GetRoot();
            auto right_task = Build(node->GetProducer(1), status);
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner for: " + node->GetProducer(1)->GetTreeString();
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right = right_task.GetRoot();
            auto op = dynamic_cast<const PhysicalRequestJoinNode*>(node);
            switch (op->join().join_type()) {
                case node::kJoinTypeLast:
                case node::kJoinTypeLeft: {
                    RequestJoinRunner* runner = CreateRunner<RequestJoinRunner>(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(), op->join_,
                        left->output_schemas()->GetSchemaSourceSize(), right->output_schemas()->GetSchemaSourceSize(),
                        op->output_right_only());

                    if (support_cluster_optimized_) {
                        if (node->GetOutputType() == kSchemaTypeRow) {
                            // complete cluster task from right
                            if (op->join().index_key().ValidKey()) {
                                // optimize key in this node
                                return RegisterTask(node, BinaryInherit(left_task, right_task, runner,
                                                                        op->join().index_key(), kLeftBias));
                            } else {
                                // optimize happens before, in right node
                                auto right_route_info = right_task.GetRouteInfo();
                                runner->AddProducer(left_task.GetRoot());
                                runner->AddProducer(right_task.GetRoot());
                                return RegisterTask(node, ClusterTask(runner, {}, right_route_info));
                            }
                        } else {
                            // uncomplete/lazify cluster task from left
                            auto left_route_info = left_task.GetRouteInfo();
                            runner->AddProducer(left_task.GetRoot());
                            runner->AddProducer(right_task.GetRoot());
                            return RegisterTask(node, ClusterTask(runner, {}, left_route_info));
                        }
                    }

                    return RegisterTask(
                        node, BinaryInherit(left_task, right_task, runner, op->join().index_key(), kLeftBias));
                }
                case node::kJoinTypeConcat: {
                    ConcatRunner* runner = CreateRunner<ConcatRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt());
                    if (support_cluster_optimized_) {
                        if (right_task.IsCompletedClusterTask() && right_task.GetRouteInfo().lazy_route_ &&
                            !op->join_.index_key_.ValidKey()) {
                            // concat join (.., filter<optimized>)
                            runner->AddProducer(left_task.GetRoot());
                            runner->AddProducer(right_task.GetRoot());
                            return RegisterTask(node, ClusterTask(runner, {}, RouteInfo{}));
                        }

                        // concat join (any(tx), any(tx)), tx is not request table
                        if (node->GetOutputType() != kSchemaTypeRow) {
                            runner->AddProducer(left_task.GetRoot());
                            runner->AddProducer(right_task.GetRoot());
                            return RegisterTask(node, ClusterTask(runner, {}, left_task.GetRouteInfo()));
                        }
                    }
                    return RegisterTask(node, BinaryInherit(left_task, right_task, runner, Key(), kNoBias));
                }
                default: {
                    status.code = common::kExecutionPlanError;
                    status.msg = "can't handle join type " + node::JoinTypeName(op->join().join_type());
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpJoin: {
            auto left_task = Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto left = left_task.GetRoot();
            auto right_task = Build(node->producers().at(1), status);
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right = right_task.GetRoot();
            auto op = dynamic_cast<const PhysicalJoinNode*>(node);
            switch (op->join().join_type()) {
                case node::kJoinTypeLeft:
                case node::kJoinTypeLast: {
                    // TableLastJoin convert to Batch Request RequestLastJoin
                    if (support_cluster_optimized_) {
                        // looks strange, join op won't run for batch-cluster mode
                        RequestJoinRunner* runner = CreateRunner<RequestJoinRunner>(
                            id_++, node->schemas_ctx(), op->GetLimitCnt(), op->join_,
                            left->output_schemas()->GetSchemaSourceSize(),
                            right->output_schemas()->GetSchemaSourceSize(), op->output_right_only_);
                        return RegisterTask(
                            node, BinaryInherit(left_task, right_task, runner, op->join().index_key(), kLeftBias));
                    } else {
                        JoinRunner* runner =
                            CreateRunner<JoinRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), op->join_,
                                                         left->output_schemas()->GetSchemaSourceSize(),
                                                         right->output_schemas()->GetSchemaSourceSize());
                        return RegisterTask(node, BinaryInherit(left_task, right_task, runner, Key(), kLeftBias));
                    }
                }
                case node::kJoinTypeConcat: {
                    ConcatRunner* runner = CreateRunner<ConcatRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt());
                    return RegisterTask(node,
                                        BinaryInherit(left_task, right_task, runner, op->join().index_key(), kNoBias));
                }
                default: {
                    status.code = common::kExecutionPlanError;
                    status.msg = "can't handle join type " + node::JoinTypeName(op->join().join_type());
                    LOG(WARNING) << status;
                    return RegisterTask(node, fail);
                }
            }
        }
        case kPhysicalOpGroupBy: {
            if (support_cluster_optimized_) {
                // Non-support group by under distribution env
                status.msg = "fail to build cluster with group by node";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto cluster_task = Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            GroupRunner* runner = CreateRunner<GroupRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), op->group());
            return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
        }
        case kPhysicalOpFilter: {
            auto producer_task = Build(node->GetProducer(0), status);
            if (!producer_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalFilterNode*>(node);
            FilterRunner* runner =
                CreateRunner<FilterRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), op->filter_);
            // under cluster, filter task might be completed or uncompleted
            // based on whether filter node has the index_key underlaying DataTask requires
            ClusterTask out;
            if (support_cluster_optimized_) {
                auto& route_info_ref = producer_task.GetRouteInfo();
                if (runner->filter_gen_.ValidIndex()) {
                    // complete the route info
                    RouteInfo lazy_route_info(route_info_ref.index_, op->filter().index_key(),
                                              std::make_shared<ClusterTask>(producer_task),
                                              route_info_ref.table_handler_);
                    lazy_route_info.lazy_route_ = true;
                    runner->AddProducer(producer_task.GetRoot());
                    out = ClusterTask(runner, {}, lazy_route_info);
                } else {
                    runner->AddProducer(producer_task.GetRoot());
                    out = UnCompletedClusterTask(runner, route_info_ref.table_handler_, route_info_ref.index_);
                }
            } else {
                out = UnaryInheritTask(producer_task, runner);
            }
            return RegisterTask(node, out);
        }
        case kPhysicalOpLimit: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            if (!op->GetLimitCnt().has_value() || op->GetLimitOptimized()) {
                return RegisterTask(node, cluster_task);
            }
            // limit runner always expect limit not empty
            LimitRunner* runner = CreateRunner<LimitRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt().value());
            return RegisterTask(node, UnaryInheritTask(cluster_task, runner));
        }
        case kPhysicalOpRename: {
            return Build(node->producers().at(0), status);
        }
        case kPhysicalOpPostRequestUnion: {
            auto left_task = Build(node->producers().at(0), status);
            if (!left_task.IsValid()) {
                status.msg = "fail to build left input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto right_task = Build(node->producers().at(1), status);
            if (!right_task.IsValid()) {
                status.msg = "fail to build right input runner";
                status.code = common::kExecutionPlanError;
                LOG(WARNING) << status;
                return fail;
            }
            auto union_op = dynamic_cast<PhysicalPostRequestUnionNode*>(node);
            PostRequestUnionRunner* runner =
                CreateRunner<PostRequestUnionRunner>(id_++, node->schemas_ctx(), union_op->request_ts());
            return RegisterTask(node, BinaryInherit(left_task, right_task, runner, Key(), kRightBias));
        }
        case kPhysicalOpSetOperation: {
            auto set = dynamic_cast<vm::PhysicalSetOperationNode*>(node);
            if (set->distinct_) {
                status.msg = "online un-implemented: UNION DISTINCT";
                status.code = common::kExecutionPlanError;
                return fail;
            }
            auto set_runner =
                CreateRunner<SetOperationRunner>(id_++, node->schemas_ctx(), set->set_type_, set->distinct_);
            std::vector<ClusterTask> tasks;
            for (auto n : node->GetProducers()) {
                auto task = Build(n, status);
                if (!status.isOK()) {
                    LOG(WARNING) << status;
                    return fail;
                }
                set_runner->AddProducer(task.GetRoot());

                tasks.push_back(task);
            }
            if (support_cluster_optimized_) {
                // first cluster task
                for (auto & task : tasks) {
                    if (task.IsClusterTask()) {
                        return RegisterTask(node, ClusterTask(set_runner, {}, task.GetRouteInfo()));
                    }
                }
            }
            return RegisterTask(node, ClusterTask(set_runner));
        }
        default: {
            status.code = common::kExecutionPlanError;
            status.msg = absl::StrCat("Non-support node ", PhysicalOpTypeName(node->GetOpType()),
                                      " for OpenMLDB Online execute mode");
            LOG(WARNING) << status;
            return RegisterTask(node, fail);
        }
    }
}

ClusterTask RunnerBuilder::BuildRequestAggUnionTask(PhysicalOpNode* node, Status& status) {
    auto fail = InvalidTask();
    auto request_task = Build(node->producers().at(0), status);
    if (!request_task.IsValid()) {
        status.msg = "fail to build request input runner";
        status.code = common::kExecutionPlanError;
        LOG(WARNING) << status;
        return fail;
    }
    auto base_table_task = Build(node->producers().at(1), status);
    auto base_table = base_table_task.GetRoot();
    if (!base_table_task.IsValid()) {
        status.msg = "fail to build base_table input runner";
        status.code = common::kExecutionPlanError;
        LOG(WARNING) << status;
        return fail;
    }
    auto agg_table_task = Build(node->producers().at(2), status);
    auto agg_table = agg_table_task.GetRoot();
    if (!agg_table_task.IsValid()) {
        status.msg = "fail to build agg_table input runner";
        status.code = common::kExecutionPlanError;
        LOG(WARNING) << status;
        return fail;
    }
    auto op = dynamic_cast<const PhysicalRequestAggUnionNode*>(node);
    RequestAggUnionRunner* runner =
        CreateRunner<RequestAggUnionRunner>(id_++, node->schemas_ctx(), op->GetLimitCnt(), op->window().range_,
                                            op->exclude_current_time(), op->output_request_row(), op->project_);
    Key index_key;
    if (!op->instance_not_in_window()) {
        index_key = op->window_.index_key();
        runner->AddWindowUnion(op->window_, base_table);
        runner->AddWindowUnion(op->agg_window_, agg_table);
    }
    auto task = RegisterTask(
        node, MultipleInherit({&request_task, &base_table_task, &agg_table_task}, runner, index_key, kRightBias));
    if (!runner->InitAggregator()) {
        return fail;
    } else {
        return task;
    }
}

ClusterTask RunnerBuilder::BinaryInherit(const ClusterTask& left, const ClusterTask& right, Runner* runner,
                                         const Key& index_key, const TaskBiasType bias) {
    if (support_cluster_optimized_) {
        return BuildClusterTaskForBinaryRunner(left, right, runner, index_key, bias);
    } else {
        return BuildLocalTaskForBinaryRunner(left, right, runner);
    }
}

ClusterTask RunnerBuilder::MultipleInherit(const std::vector<const ClusterTask*>& children, Runner* runner,
                                           const Key& index_key, const TaskBiasType bias) {
    // TODO(zhanghao): currently only kRunnerRequestAggUnion uses MultipleInherit
    const ClusterTask* request = children[0];
    if (runner->type_ != kRunnerRequestAggUnion) {
        LOG(WARNING) << "MultipleInherit only support RequestAggUnionRunner";
        return ClusterTask();
    }

    if (children.size() < 3) {
        LOG(WARNING) << "MultipleInherit should be called for children size >= 3, but children.size() = "
                     << children.size();
        return ClusterTask();
    }

    for (const auto child : children) {
        if (child->IsClusterTask()) {
            if (index_key.ValidKey()) {
                for (size_t i = 1; i < children.size(); i++) {
                    if (!children[i]->IsClusterTask()) {
                        LOG(WARNING) << "Fail to build cluster task for "
                                     << "[" << runner->id_ << "]" << RunnerTypeName(runner->type_)
                                     << ": can't handler local task with index key";
                        return ClusterTask();
                    }
                    if (children[i]->IsCompletedClusterTask()) {
                        LOG(WARNING) << "Fail to complete cluster task for "
                                     << "[" << runner->id_ << "]" << RunnerTypeName(runner->type_)
                                     << ": task is completed already";
                        return ClusterTask();
                    }
                }
                for (size_t i = 0; i < children.size(); i++) {
                    runner->AddProducer(children[i]->GetRoot());
                }
                // build complete cluster task
                // TODO(zhanghao): assume all children can be handled with one single tablet
                const RouteInfo& route_info = children[1]->GetRouteInfo();
                ClusterTask cluster_task(runner, std::vector<Runner*>({runner}),
                                         RouteInfo(route_info.index_, index_key,
                                                   std::make_shared<ClusterTask>(*request), route_info.table_handler_));
                return cluster_task;
            }
        }
    }

    // if all are local tasks
    for (const auto child : children) {
        runner->AddProducer(child->GetRoot());
    }
    return ClusterTask(runner);
}

ClusterTask RunnerBuilder::BuildLocalTaskForBinaryRunner(const ClusterTask& left, const ClusterTask& right,
                                                         Runner* runner) {
    if (left.IsClusterTask() || right.IsClusterTask()) {
        LOG(WARNING) << "fail to build local task for binary runner";
        return ClusterTask();
    }
    runner->AddProducer(left.GetRoot());
    runner->AddProducer(right.GetRoot());
    return ClusterTask(runner);
}

ClusterTask RunnerBuilder::BuildClusterTaskForBinaryRunner(const ClusterTask& left, const ClusterTask& right,
                                                           Runner* runner, const Key& index_key,
                                                           const TaskBiasType bias) {
    if (nullptr == runner) {
        LOG(WARNING) << "Fail to build cluster task for null runner";
        return ClusterTask();
    }
    ClusterTask new_left = left;
    ClusterTask new_right = right;

    // if index key is valid, try to complete route info of right cluster task
    if (index_key.ValidKey()) {
        if (!right.IsClusterTask()) {
            LOG(WARNING) << "Fail to build cluster task for "
                         << "[" << runner->id_ << "]" << RunnerTypeName(runner->type_)
                         << ": can't handler local task with index key";
            return ClusterTask();
        }
        if (right.IsCompletedClusterTask()) {
            // completed with same index key
            std::stringstream ss;
            right.Print(ss, " ");
            LOG(WARNING) << "Fail to complete cluster task for "
                         << "[" << runner->id_ << "]" << RunnerTypeName(runner->type_)
                         << ": task is completed already:\n"
                         << ss.str();
            LOG(WARNING) << "index key is " << index_key.ToString();
            return ClusterTask();
        }
        RequestRunner* request_runner = CreateRunner<RequestRunner>(id_++, new_left.GetRoot()->output_schemas());
        runner->AddProducer(request_runner);
        runner->AddProducer(new_right.GetRoot());

        const RouteInfo& right_route_info = new_right.GetRouteInfo();
        ClusterTask cluster_task(runner, std::vector<Runner*>({runner}),
                                 RouteInfo(right_route_info.index_, index_key, std::make_shared<ClusterTask>(new_left),
                                           right_route_info.table_handler_));

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
    while (new_left.IsCompletedClusterTask() && new_right.IsCompletedClusterTask()) {
        // merge left and right task if tasks can be merged
        if (ClusterTask::TaskCanBeMerge(new_left, new_right)) {
            ClusterTask task = ClusterTask::TaskMerge(runner, new_left, new_right);
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
    if (new_left.IsUnCompletedClusterTask()) {
        LOG(WARNING) << "can't handler uncompleted cluster task from left:" << new_left;
        return ClusterTask();
    }
    if (new_right.IsUnCompletedClusterTask()) {
        LOG(WARNING) << "can't handler uncompleted cluster task from right:" << new_right;
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
                return ClusterTask::TaskMergeToLeft(runner, new_left, new_right);
            }
            case kRightBias: {
                auto new_left_root_input = ClusterTask::GetRequestInput(new_left);
                auto new_right_root_input = ClusterTask::GetRequestInput(new_right);
                // task can be merge simply when their inputs are the same
                if (new_right_root_input == new_left_root_input) {
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToRight(runner, new_left, new_right);
                } else if (new_left_root_input == nullptr) {
                    // reset replace inputs as request runner
                    new_right.ResetInputs(nullptr);
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToRight(runner, new_left, new_right);
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
                return ClusterTask::TaskMergeToRight(runner, new_left, new_right);
            }
            case kLeftBias: {
                auto new_left_root_input = ClusterTask::GetRequestInput(new_right);
                auto new_right_root_input = ClusterTask::GetRequestInput(new_right);
                // task can be merge simply
                if (new_right_root_input == new_left_root_input) {
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToLeft(runner, new_left, new_right);
                } else if (new_right_root_input == nullptr) {
                    // reset replace inputs as request runner
                    new_left.ResetInputs(nullptr);
                    runner->AddProducer(new_left.GetRoot());
                    runner->AddProducer(new_right.GetRoot());
                    return ClusterTask::TaskMergeToLeft(runner, new_left, new_right);
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
ClusterTask RunnerBuilder::BuildProxyRunnerForClusterTask(const ClusterTask& task) {
    if (!task.IsCompletedClusterTask()) {
        LOG(WARNING) << "Fail to build proxy runner, cluster task is uncompleted";
        return ClusterTask();
    }
    // return cached proxy runner
    Runner* proxy_runner = nullptr;
    auto find_iter = proxy_runner_map_.find(task.GetRoot());
    if (find_iter != proxy_runner_map_.cend()) {
        proxy_runner = find_iter->second;
        proxy_runner->EnableCache();
    } else {
        uint32_t remote_task_id = cluster_job_.AddTask(task);
        ProxyRequestRunner* new_proxy_runner = CreateRunner<ProxyRequestRunner>(
            id_++, remote_task_id, task.GetIndexKeyInput(), task.GetRoot()->output_schemas());
        if (nullptr != task.GetIndexKeyInput()) {
            task.GetIndexKeyInput()->EnableCache();
        }
        if (task.GetRoot()->need_batch_cache()) {
            new_proxy_runner->EnableBatchCache();
        }
        proxy_runner_map_.insert(std::make_pair(task.GetRoot(), new_proxy_runner));
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

ClusterTask RunnerBuilder::UnCompletedClusterTask(Runner* runner, const std::shared_ptr<TableHandler> table_handler,
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
ClusterTask RunnerBuilder::UnaryInheritTask(const ClusterTask& input, Runner* runner) {
    ClusterTask task = input;
    runner->AddProducer(task.GetRoot());
    task.SetRoot(runner);
    return task;
}

ClusterTask RunnerBuilder::RegisterTask(PhysicalOpNode* node, ClusterTask task) {
    task_map_[node] = task;
    if (batch_common_node_set_.find(node->node_id()) != batch_common_node_set_.end()) {
        task.GetRoot()->EnableBatchCache();
    }
    return task;
}
ClusterJob RunnerBuilder::BuildClusterJob(PhysicalOpNode* node, Status& status) {
    id_ = 0;
    cluster_job_.Reset();
    auto task = Build(node, status);
    if (!status.isOK()) {
        return cluster_job_;
    }

    if (task.IsCompletedClusterTask()) {
        auto proxy_task = BuildProxyRunnerForClusterTask(task);
        if (!proxy_task.IsValid()) {
            status.code = common::kExecutionPlanError;
            status.msg = "Fail to build proxy cluster task";
            LOG(WARNING) << status;
            return cluster_job_;
        }
        cluster_job_.AddMainTask(proxy_task);
    } else if (task.IsUnCompletedClusterTask()) {
        status.code = common::kExecutionPlanError;
        status.msg =
            "Fail to build main task, can't handler "
            "uncompleted cluster task";
        LOG(WARNING) << status;
        return cluster_job_;
    } else {
        cluster_job_.AddMainTask(task);
    }
    return cluster_job_;
}

}  // namespace vm
}  // namespace hybridse
