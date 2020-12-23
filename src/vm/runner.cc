/*------------------------------------------------------------------------- *
 *Copyright (C) 2020, 4paradigm
 *
 * runner.cc
 *
 * Author: chenjing
 * Date: 2020/4/3
 *--------------------------------------------------------------------------
 **/
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

namespace fesql {
namespace vm {
#define MAX_DEBUG_LINES_CNT 20
#define MAX_DEBUG_COLUMN_MAX 20

ClusterTask RunnerBuilder::Build(PhysicalOpNode* node, Status& status) {
    auto fail = ClusterTask();
    if (nullptr == node) {
        status.msg = "fail to build runner : physical node is null";
        status.code = common::kOpGenError;
        LOG(WARNING) << status;
        return fail;
    }
    auto iter = task_map_.find(node);
    if (iter != task_map_.cend()) {
        iter->second.GetRoot()->EnableCache();
        return ClusterTask((iter->second));
    }
    switch (node->GetOpType()) {
        case kPhysicalOpDataProvider: {
            auto op = dynamic_cast<const PhysicalDataProviderNode*>(node);
            switch (op->provider_type_) {
                case kProviderTypeTable: {
                    auto provider =
                        dynamic_cast<const PhysicalTableProviderNode*>(node);
                    auto runner = nm_->RegisterNode(new DataRunner(
                        id_++, node->schemas_ctx(), provider->table_handler_));
                    return RegisterTask(node, ClusterTask(runner));
                }
                case kProviderTypePartition: {
                    auto provider =
                        dynamic_cast<const PhysicalPartitionProviderNode*>(
                            node);
                    auto runner = nm_->RegisterNode(
                        new DataRunner(id_++, node->schemas_ctx(),
                                       provider->table_handler_->GetPartition(
                                           provider->index_name_)));
                    if (support_cluster_optimized_) {
                        return RegisterTask(
                            node, ClusterTask(runner, provider->table_handler_,
                                              provider->index_name_));
                    } else {
                        return RegisterTask(node, ClusterTask(runner));
                    }
                }
                case kProviderTypeRequest: {
                    auto runner = nm_->RegisterNode(
                        new RequestRunner(id_++, node->schemas_ctx()));
                    return RegisterTask(node, ClusterTask(runner));
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
            auto input = cluster_task.GetRoot();
            auto op = dynamic_cast<const PhysicalSimpleProjectNode*>(node);
            auto runner = new SimpleProjectRunner(id_++, node->schemas_ctx(),
                                                  op->GetLimitCnt(),
                                                  op->project().fn_info());
            if (kRunnerRequestRunProxy == input->type_ &&
                !input->need_cache()) {
                cluster_job_.AddRunnerToTask(
                    nm_->RegisterNode(runner),
                    dynamic_cast<ProxyRequestRunner*>(input)->task_id());
                input->set_output_schemas(runner->output_schemas());
                return RegisterTask(node, cluster_task);
            }
            runner->AddProducer(input);
            cluster_task.SetRoot(runner);
            return RegisterTask(node, cluster_task);
        }
        case kPhysicalOpConstProject: {
            auto op = dynamic_cast<const PhysicalConstProjectNode*>(node);
            auto runner = new ConstProjectRunner(id_++, node->schemas_ctx(),
                                                 op->GetLimitCnt(),
                                                 op->project().fn_info());
            return RegisterTask(node, ClusterTask(nm_->RegisterNode(runner)));
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
                    auto runner = new TableProjectRunner(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->project().fn_info());
                    runner->AddProducer(input);
                    cluster_task.SetRoot(nm_->RegisterNode(runner));
                    return RegisterTask(node, cluster_task);
                }
                case kAggregation: {
                    auto runner = new AggRunner(id_++, node->schemas_ctx(),
                                                op->GetLimitCnt(),
                                                op->project().fn_info());
                    if (kRunnerRequestRunProxy == input->type_ &&
                        !input->need_cache()) {
                        cluster_job_.AddRunnerToTask(
                            nm_->RegisterNode(runner),
                            dynamic_cast<ProxyRequestRunner*>(input)
                                ->task_id());
                        input->set_output_schemas(runner->output_schemas());
                        return RegisterTask(node, cluster_task);
                    }
                    runner->AddProducer(input);
                    cluster_task.SetRoot(nm_->RegisterNode(runner));
                    return RegisterTask(node, cluster_task);
                }
                case kGroupAggregation: {
                    auto op =
                        dynamic_cast<const PhysicalGroupAggrerationNode*>(node);
                    auto runner = new GroupAggRunner(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->group_, op->project().fn_info());
                    runner->AddProducer(input);
                    cluster_task.SetRoot(nm_->RegisterNode(runner));
                    return RegisterTask(node, cluster_task);
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
                    auto runner = new WindowAggRunner(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->window_, op->project().fn_info(),
                        op->instance_not_in_window(), op->need_append_input());
                    runner->AddProducer(input);
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
                            if (!cluster_task.IsClusterTask()) {
                                cluster_task = union_task;
                                cluster_task.SetIndexKey(
                                    window_union.second.partition());
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

                            // set runner partition id, if main table depend on
                            // no window(partition)
                            if (!cluster_task.IsClusterTask()) {
                                cluster_task = join_task;
                                cluster_task.SetIndexKey(
                                    window_join.second.index_key_);
                            }
                            runner->AddWindowJoin(window_join.second,
                                                  input_slices,
                                                  join_right_runner);
                        }
                    }

                    cluster_task.SetRoot(nm_->RegisterNode(runner));
                    return RegisterTask(node, cluster_task);
                }
                case kRowProject: {
                    auto runner = new RowProjectRunner(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->project().fn_info());
                    if (kRunnerRequestRunProxy == input->type_) {
                        cluster_job_.AddRunnerToTask(
                            nm_->RegisterNode(runner),
                            dynamic_cast<ProxyRequestRunner*>(input)
                                ->task_id());
                        input->set_output_schemas(runner->output_schemas());
                        return RegisterTask(node, cluster_task);
                    } else {
                        runner->AddProducer(input);
                        cluster_task.SetRoot(nm_->RegisterNode(runner));
                        return RegisterTask(node, cluster_task);
                    }
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
            auto runner = new RequestUnionRunner(
                id_++, node->schemas_ctx(), op->GetLimitCnt(),
                op->window().range_, op->output_request_row());
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
                    }
                }
            }
            auto cluster_task =
                BuildRunnerWithProxy(nm_->RegisterNode(runner), left_task,
                                     right_task, index_key, status);
            return RegisterTask(node, cluster_task);
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
                    auto runner = new RequestLastJoinRunner(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->join_,
                        left->output_schemas()->GetSchemaSourceSize(),
                        right->output_schemas()->GetSchemaSourceSize(),
                        op->output_right_only());

                    auto cluster_task = BuildRunnerWithProxy(
                        nm_->RegisterNode(runner), left_task, right_task,
                        op->join().index_key(), status);
                    return RegisterTask(node, cluster_task);
                }
                case node::kJoinTypeConcat: {
                    auto runner = new ConcatRunner(id_++, node->schemas_ctx(),
                                                   op->GetLimitCnt());
                    auto cluster_task = BuildRunnerWithProxy(
                        nm_->RegisterNode(runner), left_task, right_task, Key(),
                        status);
                    return RegisterTask(node, cluster_task);
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
            if (support_cluster_optimized_) {
                // 边界检查, 分布式计划暂时不支持表拼接
                status.msg = "fail to build cluster with table join node";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
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
                    auto runner = new LastJoinRunner(
                        id_++, node->schemas_ctx(), op->GetLimitCnt(),
                        op->join_,
                        left->output_schemas()->GetSchemaSourceSize(),
                        right->output_schemas()->GetSchemaSourceSize());
                    runner->AddProducer(left);
                    runner->AddProducer(right);
                    // TODO(chenjing): support join+window
                    left_task.SetRoot(nm_->RegisterNode(runner));
                    return RegisterTask(node, left_task);
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
            auto input = cluster_task.GetRoot();
            auto op = dynamic_cast<const PhysicalGroupNode*>(node);
            auto runner = new GroupRunner(id_++, node->schemas_ctx(),
                                          op->GetLimitCnt(), op->group());
            runner->AddProducer(input);
            cluster_task.SetRoot(nm_->RegisterNode(runner));
            return RegisterTask(node, cluster_task);
        }
        case kPhysicalOpFilter: {
            auto cluster_task =  // NOLINT
                Build(node->producers().at(0), status);
            auto input = cluster_task.GetRoot();
            if (!cluster_task.IsValid()) {
                status.msg = "fail to build input runner";
                status.code = common::kOpGenError;
                LOG(WARNING) << status;
                return fail;
            }
            auto op = dynamic_cast<const PhysicalFilterNode*>(node);
            auto runner = new FilterRunner(id_++, node->schemas_ctx(),
                                           op->GetLimitCnt(), op->filter_);
            runner->AddProducer(input);
            cluster_task.SetRoot(nm_->RegisterNode(runner));
            return RegisterTask(node, cluster_task);
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
            auto input = cluster_task.GetRoot();
            auto op = dynamic_cast<const PhysicalLimitNode*>(node);
            if (op->GetLimitCnt() == 0 || op->GetLimitOptimized()) {
                return RegisterTask(node, cluster_task);
            }
            auto runner =
                new LimitRunner(id_++, node->schemas_ctx(), op->GetLimitCnt());
            runner->AddProducer(input);
            cluster_task.SetRoot(nm_->RegisterNode(runner));
            return RegisterTask(node, cluster_task);
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
            auto runner = nm_->RegisterNode(new PostRequestUnionRunner(
                id_++, node->schemas_ctx(), union_op->request_ts()));
            runner->AddProducer(left_task.GetRoot());
            runner->AddProducer(right_task.GetRoot());
            left_task.SetRoot(runner);
            return RegisterTask(node, left_task);
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
// runner(left,right) -->
// proxy_request_run(left, task_id) : task[runner(request, right)]
ClusterTask RunnerBuilder::BuildRunnerWithProxy(Runner* runner,
                                                const ClusterTask& left_task,
                                                const ClusterTask& right_task,
                                                const Key& index_key,
                                                Status& status) {
    auto left = left_task.GetRoot();
    auto right = right_task.GetRoot();
    auto task = left_task;
    if (support_cluster_optimized_ && right_task.IsClusterTask()) {
        runner->AddProducer(nm_->RegisterNode(
            new RequestRunner(id_++, left->output_schemas())));
        runner->AddProducer(right);
        auto remote_task = right_task;
        remote_task.SetRoot(runner);
        remote_task.SetIndexKey(index_key);
        auto remote_task_id = cluster_job_.AddTask(remote_task);
        if (-1 == remote_task_id) {
            LOG(WARNING) << "fail to add remote task id";
            return ClusterTask();
        }
        auto proxy_runner = nm_->RegisterNode(
            new ProxyRequestRunner(id_++, static_cast<uint32_t>(remote_task_id),
                                   runner->output_schemas()));
        proxy_runner->AddProducer(left);
        return ClusterTask(proxy_runner);
    } else {
        runner->AddProducer(left);
        runner->AddProducer(right);
        task.SetRoot(runner);
    }
    return task;
}

bool Runner::GetColumnBool(RowView* row_view, int idx, type::Type type) {
    bool key = false;
    switch (type) {
        case fesql::type::kInt32: {
            int32_t value;
            if (0 == row_view->GetInt32(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kInt64: {
            int64_t value;
            if (0 == row_view->GetInt64(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetInt16(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kFloat: {
            float value;
            if (0 == row_view->GetFloat(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kDouble: {
            double value;
            if (0 == row_view->GetDouble(idx, &value)) {
                return value == 0 ? false : true;
            }
            break;
        }
        case fesql::type::kBool: {
            bool value;
            if (0 == row_view->GetBool(idx, &value)) {
                return value;
            }
        }
        default: {
            LOG(WARNING) << "fail to get bool for "
                            "current row";
            break;
        }
    }
    return key;
}

Row Runner::WindowProject(const int8_t* fn, const uint64_t key, const Row row,
                          const bool is_instance, size_t append_slices,
                          Window* window) {
    if (row.empty()) {
        return row;
    }
    window->BufferData(key, row);
    if (!is_instance) {
        return Row();
    }

    // Init current run step runtime
    JITRuntime::get()->InitRunStep();

    auto udf =
        reinterpret_cast<int32_t (*)(const int8_t*, const int8_t*, int8_t**)>(
            const_cast<int8_t*>(fn));
    int8_t* out_buf = nullptr;

    codec::ListRef<Row> window_ref;
    window_ref.list = reinterpret_cast<int8_t*>(window);
    auto window_ptr = reinterpret_cast<const int8_t*>(&window_ref);
    auto row_ptr = reinterpret_cast<const int8_t*>(&row);

    uint32_t ret = udf(row_ptr, window_ptr, &out_buf);

    // Release current run step resources
    JITRuntime::get()->ReleaseRunStep();

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

int64_t Runner::GetColumnInt64(RowView* row_view, int key_idx,
                               type::Type key_type) {
    int64_t key = -1;
    switch (key_type) {
        case fesql::type::kInt32: {
            int32_t value;
            if (0 == row_view->GetInt32(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kInt64: {
            int64_t value;
            if (0 == row_view->GetInt64(key_idx, &value)) {
                return value;
            }
            break;
        }
        case fesql::type::kInt16: {
            int16_t value;
            if (0 == row_view->GetInt16(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kFloat: {
            float value;
            if (0 == row_view->GetFloat(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kDouble: {
            double value;
            if (0 == row_view->GetDouble(key_idx, &value)) {
                return static_cast<int64_t>(value);
            }
            break;
        }
        case fesql::type::kTimestamp: {
            int64_t value;
            if (0 == row_view->GetTimestamp(key_idx, &value)) {
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

    if (ctx.is_debug()) {
        LOG(INFO) << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
                  << "\n";
    }
    for (size_t idx = 0; idx < ctx.GetRequestSize(); idx++) {
        inputs.clear();
        for (size_t producer_idx = 0; producer_idx < producers_.size();
             producer_idx++) {
            inputs.push_back(batch_inputs[producer_idx]->Get(idx));
        }
        auto res = Run(ctx, inputs);
        if (ctx.is_debug()) {
            Runner::PrintData(output_schemas_, res);
        }
        if (need_batch_cache_) {
            if (ctx.is_debug()) {
                LOG(INFO) << "RUNNER TYPE: " << RunnerTypeName(type_)
                          << "RUNNER ID " << id_ << " HIT BATCH CACHE!";
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
        LOG(INFO) << "RUNNER TYPE: " << RunnerTypeName(type_)
                  << ", ID: " << id_;
        Runner::PrintData(output_schemas_, res);
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
    auto res = std::shared_ptr<DataHandlerList>(
        new DataHandlerRepeater(data_handler_, ctx.GetRequestSize()));

    if (ctx.is_debug()) {
        LOG(INFO) << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
                  << ", Repeated " << ctx.GetRequestSize();
        Runner::PrintData(output_schemas_, res->Get(0));
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
    std::shared_ptr<DataHandlerVector> res =
        std::shared_ptr<DataHandlerVector>(new DataHandlerVector());
    for (size_t idx = 0; idx < ctx.GetRequestSize(); idx++) {
        res->Add(std::shared_ptr<MemRowHandler>(
            new MemRowHandler(ctx.GetRequest(idx))));
    }
    if (ctx.is_debug()) {
        LOG(INFO) << "RUNNER TYPE: " << RunnerTypeName(type_) << ", ID: " << id_
                  << "\n";
        for (size_t idx = 0; idx < res->GetSize(); idx++) {
            Runner::PrintData(output_schemas_, res->Get(idx));
        }
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
    return partition_gen_.Partition(input);
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
    output_table->AddRow(project_gen_.Gen());
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
    iter->SeekToFirst();
    int32_t cnt = 0;
    while (iter->Valid()) {
        if (limit_cnt_ > 0 && cnt++ >= limit_cnt_) {
            break;
        }
        output_table->AddRow(project_gen_.Gen(iter->GetValue()));
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
        new MemRowHandler(project_gen_.Gen(row->GetValue())));
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

    switch (input->GetHanlderType()) {
        case kTableHandler: {
            return std::shared_ptr<TableHandler>(new TableProjectWrapper(
                std::dynamic_pointer_cast<TableHandler>(input),
                &project_gen_.fun_));
        }
        case kPartitionHandler: {
            return std::shared_ptr<TableHandler>(new PartitionProjectWrapper(
                std::dynamic_pointer_cast<PartitionHandler>(input),
                &project_gen_.fun_));
        }
        case kRowHandler: {
            return std::shared_ptr<RowHandler>(new RowProjectWrapper(
                std::dynamic_pointer_cast<RowHandler>(input),
                &project_gen_.fun_));
        }
        default: {
            LOG(WARNING) << "Fail run simple project, invalid handler type "
                         << input->GetHandlerTypeName();
        }
    }

    return std::shared_ptr<DataHandler>();
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

    // Partition Instance Table
    auto instance_partition =
        instance_window_gen_.partition_gen_.Partition(input);
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
    auto union_partitions = windows_union_gen_.PartitionEach(union_inpus);
    // Prepare Join Tables
    auto join_right_tables = windows_join_gen_.RunInputs(ctx);

    // Compute output
    std::shared_ptr<MemTableHandler> output_table =
        std::shared_ptr<MemTableHandler>(new MemTableHandler());
    while (instance_partition_iter->Valid()) {
        auto key = instance_partition_iter->GetKey().ToString();
        RunWindowAggOnKey(instance_partition, union_partitions,
                          join_right_tables, key, output_table);
        instance_partition_iter->Next();
    }
    return output_table;
}

// Run Window Aggeregation on given key
void WindowAggRunner::RunWindowAggOnKey(
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
    CurrentHistoryWindow window(instance_window_gen_.range_gen_.start_offset_);
    window.set_instance_not_in_window(instance_not_in_window_);
    window.set_rows_preceding(instance_window_gen_.range_gen_.start_row_);

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
                row = windows_join_gen_.Join(row, join_right_tables);
            }
            window_project_gen_.Gen(
                union_segment_iters[min_union_pos]->GetKey(), row, false,
                append_slices_, &window);

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
            row = windows_join_gen_.Join(instance_row, join_right_tables);
            output_table->AddRow(
                window_project_gen_.Gen(instance_segment_iter->GetKey(), row,
                                        true, append_slices_, &window));
        } else {
            output_table->AddRow(window_project_gen_.Gen(
                instance_segment_iter->GetKey(), instance_row, true,
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
    if (output_right_only_) {
        return std::shared_ptr<RowHandler>(new MemRowHandler(
            join_gen_.RowLastJoinDropLeftSlices(left_row, right)));
    } else {
        return std::shared_ptr<RowHandler>(
            new MemRowHandler(join_gen_.RowLastJoin(left_row, right)));
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

    switch (left->GetHanlderType()) {
        case kTableHandler: {
            if (join_gen_.right_group_gen_.Valid()) {
                right = join_gen_.right_group_gen_.Partition(right);
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
                        output_table)) {
                    return fail_ptr;
                }
            } else {
                if (!join_gen_.TableJoin(
                        left_table,
                        std::dynamic_pointer_cast<TableHandler>(right),
                        output_table)) {
                    return fail_ptr;
                }
            }
            return output_table;
        }
        case kPartitionHandler: {
            if (join_gen_.right_group_gen_.Valid()) {
                right = join_gen_.right_group_gen_.Partition(right);
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
                        output_partition)) {
                    return fail_ptr;
                }

            } else {
                if (!join_gen_.PartitionJoin(
                        left_partition,
                        std::dynamic_pointer_cast<TableHandler>(right),
                        output_partition)) {
                    return fail_ptr;
                }
            }
            return output_partition;
        }
        case kRowHandler: {
            auto left_row = std::dynamic_pointer_cast<RowHandler>(left);
            return std::make_shared<MemRowHandler>(
                join_gen_.RowLastJoin(left_row->GetValue(), right));
        }
        default:
            return fail_ptr;
    }
}

std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<DataHandler> input) {
    switch (input->GetHanlderType()) {
        case kPartitionHandler: {
            return Partition(
                std::dynamic_pointer_cast<PartitionHandler>(input));
        }
        case kTableHandler: {
            return Partition(std::dynamic_pointer_cast<TableHandler>(input));
        }
        default: {
            LOG(WARNING) << "Partition Fail: input isn't partition or table";
            return std::shared_ptr<PartitionHandler>();
        }
    }
}
std::shared_ptr<PartitionHandler> PartitionGenerator::Partition(
    std::shared_ptr<PartitionHandler> table) {
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
            std::string keys = key_gen_.Gen(segment_iter->GetValue());
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
    std::shared_ptr<TableHandler> table) {
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
        std::string keys = key_gen_.Gen(iter->GetValue());
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
    const Row& left_row, std::shared_ptr<DataHandler> right) {
    Row joined = RowLastJoin(left_row, right);
    Row right_row(joined.GetSlice(left_slices_));
    for (size_t offset = 0; offset < right_slices_; offset++) {
        right_row.Append(joined.GetSlice(left_slices_ + offset));
    }
    return right_row;
}
Row JoinGenerator::RowLastJoin(const Row& left_row,
                               std::shared_ptr<DataHandler> right) {
    switch (right->GetHanlderType()) {
        case kPartitionHandler: {
            return RowLastJoinPartition(
                left_row, std::dynamic_pointer_cast<PartitionHandler>(right));
        }
        case kTableHandler: {
            return RowLastJoinTable(
                left_row, std::dynamic_pointer_cast<TableHandler>(right));
        }
        case kRowHandler: {
            auto right_table =
                std::shared_ptr<MemTableHandler>(new MemTableHandler());
            right_table->AddRow(
                std::dynamic_pointer_cast<RowHandler>(right)->GetValue());
            return RowLastJoinTable(left_row, right_table);
        }
        default: {
            LOG(WARNING) << "Last Join right isn't row or table or partition";
            return Row(left_slices_, left_row, right_slices_, Row());
        }
    }
}
Row JoinGenerator::RowLastJoinPartition(
    const Row& left_row, std::shared_ptr<PartitionHandler> partition) {
    if (!index_key_gen_.Valid()) {
        LOG(WARNING) << "can't join right partition table when partition "
                        "keys is empty";
        return Row();
    }
    std::string partition_key = index_key_gen_.Gen(left_row);
    auto right_table = partition->GetSegment(partition_key);
    return RowLastJoinTable(left_row, right_table);
}
Row JoinGenerator::RowLastJoinTable(const Row& left_row,
                                    std::shared_ptr<TableHandler> table) {
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
        left_key_str = left_key_gen_.Gen(left_row);
    }
    while (right_iter->Valid()) {
        if (right_group_gen_.Valid()) {
            auto right_key_str =
                right_group_gen_.GetKey(right_iter->GetValue());
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
        if (condition_gen_.Gen(joined_row)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices_, left_row, right_slices_, Row());
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<TableHandler> right,
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
                                     right, right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::TableJoin(std::shared_ptr<TableHandler> left,
                              std::shared_ptr<PartitionHandler> right,
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
            index_key_gen_.Valid() ? index_key_gen_.Gen(left_row) : "";
        if (left_key_gen_.Valid()) {
            key_str = key_str.empty()
                          ? left_key_gen_.Gen(left_row)
                          : key_str + "|" + left_key_gen_.Gen(left_row);
        }
        DLOG(INFO) << "key_str " << key_str;
        auto right_table = right->GetSegment(key_str);
        output->AddRow(left_iter->GetKey(),
                       Runner::RowLastJoinTable(
                           left_slices_, left_row, right_slices_, right_table,
                           right_sort_gen_, condition_gen_));
        left_iter->Next();
    }
    return true;
}

bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<TableHandler> right,
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
                               right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_window_iter->Next();
    }
    return true;
}
bool JoinGenerator::PartitionJoin(std::shared_ptr<PartitionHandler> left,
                                  std::shared_ptr<PartitionHandler> right,
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
                index_key_gen_.Valid() ? index_key_gen_.Gen(left_row) + "|" +
                                             left_key_gen_.Gen(left_row)
                                       : left_key_gen_.Gen(left_row);
            auto right_table = right->GetSegment(key_str);
            auto left_key_str = std::string(
                reinterpret_cast<const char*>(left_key.buf()), left_key.size());
            output->AddRow(left_key_str, left_iter->GetKey(),
                           Runner::RowLastJoinTable(
                               left_slices_, left_row, right_slices_,
                               right_table, right_sort_gen_, condition_gen_));
            left_iter->Next();
        }
        left_partition_iter->Next();
    }
    return true;
}
const Row Runner::RowLastJoinTable(size_t left_slices, const Row& left_row,
                                   size_t right_slices,
                                   std::shared_ptr<TableHandler> right_table,
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
        if (cond_gen.Gen(joined_row)) {
            return joined_row;
        }
        right_iter->Next();
    }
    return Row(left_slices, left_row, right_slices, Row());
}
void Runner::PrintData(const vm::SchemasContext* schema_list,
                       std::shared_ptr<DataHandler> data) {
    std::ostringstream oss;
    std::vector<RowView> row_view_list;
    ::fesql::base::TextTable t('-', '|', '+');
    // Add Header
    t.add("Order");
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

    t.endOfRow();
    if (!data) {
        t.add("Empty set");
        t.endOfRow();
        oss << t << std::endl;
        LOG(INFO) << "\n" << oss.str();
        return;
    }

    switch (data->GetHanlderType()) {
        case kRowHandler: {
            auto row_handler = std::dynamic_pointer_cast<RowHandler>(data);
            if (!row_handler) {
                t.add("NULL Row");
                t.endOfRow();
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

            t.endOfRow();
            break;
        }
        case kTableHandler: {
            auto table_handler = std::dynamic_pointer_cast<TableHandler>(data);
            if (!table_handler) {
                t.add("Empty set");
                t.endOfRow();
                break;
            }
            auto iter = table_handler->GetIterator();
            if (!iter) {
                t.add("Empty set");
                t.endOfRow();
                break;
            }
            iter->SeekToFirst();
            if (!iter->Valid()) {
                t.add("Empty set");
                t.endOfRow();
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
                    t.endOfRow();
                }
            }

            break;
        }
        case kPartitionHandler: {
            auto partition = std::dynamic_pointer_cast<PartitionHandler>(data);
            if (!partition) {
                t.add("Empty set");
                t.endOfRow();
                break;
            }
            auto iter = partition->GetWindowIterator();
            int cnt = 0;
            if (!iter) {
                t.add("Empty set");
                t.endOfRow();
                break;
            }
            iter->SeekToFirst();
            if (!iter->Valid()) {
                t.add("Empty set");
                t.endOfRow();
                break;
            }
            while (iter->Valid() && cnt++ < MAX_DEBUG_LINES_CNT) {
                t.add("KEY: " + std::string(reinterpret_cast<const char*>(
                                                iter->GetKey().buf()),
                                            iter->GetKey().size()));
                t.endOfRow();
                auto segment_iter = iter->GetValue();
                if (!segment_iter) {
                    t.add("Empty set");
                    t.endOfRow();
                    break;
                }
                segment_iter->SeekToFirst();
                if (!segment_iter->Valid()) {
                    t.add("Empty set");
                    t.endOfRow();
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
                        t.endOfRow();
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
    oss << t << std::endl;
    LOG(INFO) << data->GetHandlerTypeName() << " RESULT:\n" << oss.str();
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
    if (!left || !right) {
        return std::shared_ptr<DataHandler>();
    }
    if (kRowHandler != left->GetHanlderType()) {
        return std::shared_ptr<DataHandler>();
    }
    return std::shared_ptr<RowHandler>(new RowCombineWrapper(
        std::dynamic_pointer_cast<RowHandler>(left), left_slices,
        std::dynamic_pointer_cast<RowHandler>(right), right_slices));
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
    // build window with start and end offset
    switch (input->GetHanlderType()) {
        case kTableHandler: {
            return filter_gen_.Filter(
                std::dynamic_pointer_cast<TableHandler>(input));
        }
        case kPartitionHandler: {
            return filter_gen_.Filter(
                std::dynamic_pointer_cast<PartitionHandler>(input));
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
        output_table->AddRow(agg_gen_.Gen(segment));
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
    // build window with start and end offset
    auto window_table =
        std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
    uint64_t start = 0;
    uint64_t end = UINT64_MAX;
    uint64_t rows_start_preceding = 0;
    int64_t request_key = range_gen_.ts_gen_.Gen(request);
    if (range_gen_.Valid()) {
        start = (request_key + range_gen_.start_offset_) < 0
                    ? 0
                    : (request_key + range_gen_.start_offset_);
        end = (request_key + range_gen_.end_offset_) < 0
                  ? 0
                  : (request_key + range_gen_.end_offset_);
        rows_start_preceding = range_gen_.start_row_;
    }
    if (output_request_row_) {
        window_table->AddRow(request_key, request);
    }
    // Prepare Union Window
    auto union_inputs = windows_union_gen_.RunInputs(ctx);
    auto union_segments =
        windows_union_gen_.GetRequestWindows(request, union_inputs);
    // Prepare Union Segment Iterators
    size_t unions_cnt = windows_union_gen_.inputs_cnt_;

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
    uint64_t cnt = 1;
    while (-1 != max_union_pos) {
        if (cnt > rows_start_preceding &&
            union_segment_status[max_union_pos].key_ < start) {
            break;
        }

        window_table->AddRow(union_segment_status[max_union_pos].key_,
                             union_segment_iters[max_union_pos]->GetValue());
        cnt++;
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
    DLOG(INFO) << "REQUEST UNION cnt = " << cnt;
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
    if (kRowHandler != left->GetHanlderType()) {
        return nullptr;
    }

    auto request = std::dynamic_pointer_cast<RowHandler>(left)->GetValue();
    int64_t request_key = request_ts_gen_.Gen(request);

    auto result_table =
        std::shared_ptr<MemTimeTableHandler>(new MemTimeTableHandler());
    result_table->AddRow(request_key, request);

    auto window_table = std::dynamic_pointer_cast<MemTimeTableHandler>(right);
    auto window_iter = window_table->GetIterator();
    while (window_iter->Valid()) {
        result_table->AddRow(window_iter->GetKey(), window_iter->GetValue());
        window_iter->Next();
    }
    return result_table;
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
        agg_gen_.Gen(std::dynamic_pointer_cast<TableHandler>(input))));
    return row_handler;
}

std::shared_ptr<DataHandler> ProxyRequestRunner::Run(
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

    auto cluster_job = ctx.cluster_job();
    auto fail_ptr = std::shared_ptr<DataHandler>();
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
    switch (input->GetHanlderType()) {
        case kRowHandler: {
            auto row = std::dynamic_pointer_cast<RowHandler>(input)->GetValue();
            if (!task.GetIndexKey().ValidKey()) {
                LOG(WARNING) << "can't pick tablet to subquery without index";
                return std::shared_ptr<DataHandler>();
            }
            KeyGenerator generator(task.GetIndexKey().fn_info());
            pk = generator.Gen(row);
            if (pk.empty()) {
                // local mode
                LOG(WARNING) << "can't pick tablet to subquery with empty pk";
                return std::shared_ptr<DataHandler>();
            } else {
                DLOG(INFO) << "pick tablet with given index_name "
                           << task.index() << " pk " << pk;
                auto table_handler = task.table_handler();
                if (!table_handler) {
                    LOG(WARNING) << "table handler is null";
                    return std::shared_ptr<DataHandler>();
                }
                auto tablet = table_handler->GetTablet(task.index(), pk);
                if (!tablet) {
                    DLOG(INFO) << "tablet is null, run in local mode";
                    return std::shared_ptr<DataHandler>();
                } else {
                    if (row.GetRowPtrCnt() > 1) {
                        LOG(WARNING) << "subquery with multi slice row is "
                                        "unsupported currently";
                        return std::shared_ptr<DataHandler>();
                    }
                    if (ctx.sp_name().empty()) {
                        return tablet->SubQuery(
                            task_id_, table_handler->GetDatabase(),
                            cluster_job->sql(), row, false, ctx.is_debug());
                    } else {
                        return tablet->SubQuery(
                            task_id_, table_handler->GetDatabase(),
                            ctx.sp_name(), row, true, ctx.is_debug());
                    }
                }
            }
        }
        default: {
            LOG(WARNING) << "invalid key row, key generator input type is "
                         << input->GetHandlerTypeName();
            return fail_ptr;
        }
    }
}
/**
 * TODO(chenjing): GenConst key during compile-time
 * @return
 */
const std::string KeyGenerator::GenConst() {
    Row key_row = CoreAPI::RowConstProject(fn_, true);
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
                : fn_schema_.Get(pos).type() == fesql::type::kDate
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
const std::string KeyGenerator::Gen(const Row& row) {
    Row key_row = CoreAPI::RowProject(fn_, row, true);
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
                : fn_schema_.Get(pos).type() == fesql::type::kDate
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
const int64_t OrderGenerator::Gen(const Row& row) {
    Row order_row = CoreAPI::RowProject(fn_, row, true);
    RowView row_view(row_view_);
    row_view.Reset(order_row.buf());
    return Runner::GetColumnInt64(&row_view, idxs_[0],
                                  fn_schema_.Get(idxs_[0]).type());
}
const bool ConditionGenerator::Gen(const Row& row) const {
    RowView row_view(row_view_);
    return CoreAPI::ComputeCondition(fn_, row, &row_view, idxs_[0]);
}
const Row ProjectGenerator::Gen(const Row& row) {
    return CoreAPI::RowProject(fn_, row, false);
}

const Row ConstProjectGenerator::Gen() {
    return CoreAPI::RowConstProject(fn_, false);
}

const Row AggGenerator::Gen(std::shared_ptr<TableHandler> table) {
    return Runner::GroupbyProject(fn_, table.get());
}

Row Runner::GroupbyProject(const int8_t* fn, TableHandler* table) {
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
    auto udf =
        reinterpret_cast<int32_t (*)(const int8_t*, const int8_t*, int8_t**)>(
            const_cast<int8_t*>(fn));
    int8_t* buf = nullptr;

    auto row_ptr = reinterpret_cast<const int8_t*>(&row);

    codec::ListRef<Row> window_ref;
    window_ref.list = reinterpret_cast<int8_t*>(table);
    auto window_ptr = reinterpret_cast<const int8_t*>(&window_ref);

    uint32_t ret = udf(row_ptr, window_ptr, &buf);
    if (ret != 0) {
        LOG(WARNING) << "fail to run udf " << ret;
        return Row();
    }
    return Row(
        base::RefCountedSlice::CreateManaged(buf, RowView::GetSize(buf)));
}

const Row WindowProjectGenerator::Gen(const uint64_t key, const Row row,
                                      bool is_instance, size_t append_slices,
                                      Window* window) {
    return Runner::WindowProject(fn_, key, row, is_instance, append_slices,
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
    std::vector<std::shared_ptr<DataHandler>> union_inputs) {
    std::vector<std::shared_ptr<PartitionHandler>> union_partitions;
    if (!windows_gen_.empty()) {
        union_partitions.reserve(windows_gen_.size());
        for (size_t i = 0; i < inputs_cnt_; i++) {
            union_partitions.push_back(
                windows_gen_[i].partition_gen_.Partition(union_inputs[i]));
        }
    }
    return union_partitions;
}
int32_t IteratorStatus::PickIteratorWithMininumKey(
    std::vector<IteratorStatus>* status_list_ptr) {
    auto status_list = *status_list_ptr;
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
    auto status_list = *status_list_ptr;
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
    const std::vector<std::shared_ptr<DataHandler>>& join_right_tables) {
    Row row = left_row;
    for (size_t i = 0; i < join_right_tables.size(); i++) {
        row = joins_gen_[i].RowLastJoin(row, join_right_tables[i]);
    }
    return row;
}

std::shared_ptr<TableHandler> IndexSeekGenerator::SegmnetOfConstKey(
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
            auto key = index_key_gen_.GenConst();
            return partition->GetSegment(key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}
std::shared_ptr<TableHandler> IndexSeekGenerator::SegmentOfKey(
    const Row& row, std::shared_ptr<DataHandler> input) {
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
            auto key = index_key_gen_.Gen(row);
            return partition->GetSegment(key);
        }
        default: {
            LOG(WARNING) << "fail to seek segment when input isn't partition";
            return fail_ptr;
        }
    }
}

std::shared_ptr<TableHandler> FilterGenerator::Filter(
    std::shared_ptr<PartitionHandler> table) {
    return Filter(index_seek_gen_.SegmnetOfConstKey(table));
}
std::shared_ptr<TableHandler> FilterGenerator::Filter(
    std::shared_ptr<TableHandler> table) {
    auto fail_ptr = std::shared_ptr<TableHandler>();
    if (!table) {
        LOG(WARNING) << "fail to filter table: input is empty";
        return fail_ptr;
    }

    if (!condition_gen_.Valid()) {
        return table;
    }
    return std::shared_ptr<TableHandler>(new TableFilterWrapper(table, this));
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

void RunnerContext::SetRequest(const fesql::codec::Row& request) {
    request_ = request;
}
void RunnerContext::SetRequests(
    const std::vector<fesql::codec::Row>& requests) {
    requests_ = requests;
}
}  // namespace vm
}  // namespace fesql
