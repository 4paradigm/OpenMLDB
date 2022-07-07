/*
 * Copyright 2021 4Paradigm
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

#include "sdk/tablet_sdk.h"
#include <map>
#include <memory>
#include <string>
#include <utility>
#include "base/fe_strings.h"
#include "brpc/channel.h"
#include "codec/fe_row_codec.h"
#include "codec/fe_schema_codec.h"
#include "glog/logging.h"
#include "node/node_enum.h"
#include "plan/plan_api.h"
#include "proto/fe_tablet.pb.h"
#include "sdk/result_set_impl.h"

namespace hybridse {
namespace sdk {

static const std::string EMPTY_STR;  // NOLINT

class ExplainInfoImpl : public ExplainInfo {
 public:
    ExplainInfoImpl(const SchemaImpl& input_schema,
                    const SchemaImpl& output_schema,
                    const std::string& logical_plan,
                    const std::string& physical_plan, const std::string& ir)
        : input_schema_(input_schema),
          output_schema_(output_schema),
          logical_plan_(logical_plan),
          physical_plan_(physical_plan),
          ir_(ir) {}
    ~ExplainInfoImpl() {}

    const Schema& GetInputSchema() { return input_schema_; }

    const Schema& GetOutputSchema() { return output_schema_; }

    const std::string& GetLogicalPlan() { return logical_plan_; }

    const std::string& GetPhysicalPlan() { return physical_plan_; }

    const std::string& GetIR() { return ir_; }

 private:
    SchemaImpl input_schema_;
    SchemaImpl output_schema_;
    std::string logical_plan_;
    std::string physical_plan_;
    std::string ir_;
};

class TabletSdkImpl : public TabletSdk {
 public:
    TabletSdkImpl() {}

    explicit TabletSdkImpl(const std::string& endpoint)
        : endpoint_(endpoint), channel_(NULL) {}

    ~TabletSdkImpl() { delete channel_; }

    bool Init();

    std::shared_ptr<ResultSet> Query(const std::string& db,
                                     const std::string& sql,
                                     sdk::Status* status) {
        return Query(db, sql, EMPTY_STR, true, status);
    }

    std::shared_ptr<ResultSet> Query(const std::string& db,
                                     const std::string& sql,
                                     const std::string& row,
                                     sdk::Status* status) {
        return Query(db, sql, row, false, status);
    }

    void Insert(const std::string& db, const std::string& sql,
                sdk::Status* status);

    std::shared_ptr<ExplainInfo> Explain(const std::string& db,
                                         const std::string& sql,
                                         sdk::Status* status);

 private:
    void BuildInsertRequest(const std::string& db, const std::string& table,
                            const std::vector<std::string>& columns,
                            node::ExprListNode* insert_value,
                            tablet::InsertRequest* request,
                            sdk::Status* status);

    void Insert(const tablet::InsertRequest& request, sdk::Status* status);

    bool GetSchema(const std::string& db, const std::string& table,
                   type::TableDef* schema, sdk::Status* status);

    void GetSqlPlan(
        const std::string& db, const std::string& sql,
        node::NodeManager& node_manager,  // NOLINT (runtime/references)
        node::PlanNodeList& plan_trees,   // NOLINT (runtime/references)
        sdk::Status& status);             // NOLINT (runtime/references)

    std::shared_ptr<ResultSet> Query(const std::string& db,
                                     const std::string& sql,
                                     const std::string& row, bool is_batch,
                                     sdk::Status* status);

 private:
    std::string endpoint_;
    brpc::Channel* channel_;
};

bool TabletSdkImpl::Init() {
    channel_ = new ::brpc::Channel();
    brpc::ChannelOptions options;
    int ret = channel_->Init(endpoint_.c_str(), &options);
    if (ret != 0) {
        return false;
    }
    return true;
}

void TabletSdkImpl::Insert(const tablet::InsertRequest& request,
                           sdk::Status* status) {
    ::hybridse::tablet::TabletServer_Stub stub(channel_);
    ::hybridse::tablet::InsertResponse response;
    brpc::Controller cntl;
    stub.Insert(&cntl, &request, &response, NULL);
    if (cntl.Failed()) {
        status->code = -1;
        status->msg = "Rpc Control error";
    }
    status->code = 0;
}

std::shared_ptr<ResultSet> TabletSdkImpl::Query(const std::string& db,
                                                const std::string& sql,
                                                const std::string& row,
                                                bool is_batch,
                                                sdk::Status* status) {
    if (status == NULL) {
        return std::shared_ptr<ResultSet>();
    }

    ::hybridse::tablet::TabletServer_Stub stub(channel_);
    ::hybridse::tablet::QueryRequest request;
    std::unique_ptr<tablet::QueryResponse> response(
        new tablet::QueryResponse());
    request.set_sql(sql);
    request.set_db(db);
    request.set_is_batch(is_batch);
    if (!is_batch) request.set_row(row);
    std::unique_ptr<brpc::Controller> cntl(new brpc::Controller());
    cntl->set_timeout_ms(10000);
    stub.Query(cntl.get(), &request, response.get(), NULL);
    if (cntl->Failed()) {
        status->code = common::kConnError;
        status->msg = "Rpc control error";
        LOG(WARNING) << "SyncQuery fail: " << status->msg;
        return std::shared_ptr<ResultSet>();
    }
    if (response->status().code() != common::kOk) {
        status->code = response->status().code();
        status->msg = response->status().msg();
        LOG(WARNING) << "SyncQuery fail: " << status->msg;
        return std::shared_ptr<ResultSet>();
    }
    status->code = 0;
    std::shared_ptr<ResultSetImpl> impl(
        new ResultSetImpl(std::move(response), std::move(cntl)));
    impl->Init();
    return impl;
}

bool TabletSdkImpl::GetSchema(const std::string& db, const std::string& table,
                              type::TableDef* schema, sdk::Status* status) {
    if (schema == NULL || status == NULL) return false;
    ::hybridse::tablet::TabletServer_Stub stub(channel_);
    ::hybridse::tablet::GetTablesSchemaRequest request;
    ::hybridse::tablet::GetTableSchemaReponse response;
    request.set_db(db);
    request.set_name(table);
    brpc::Controller cntl;
    stub.GetTableSchema(&cntl, &request, &response, NULL);
    if (response.status().code() != common::kOk) {
        status->code = response.status().code();
        status->msg = response.status().msg();
        return false;
    }
    if (cntl.Failed()) {
        status->code = (common::kConnError);
        status->msg = ("rpc controll error");
        return false;
    }
    schema->CopyFrom(response.schema());
    return true;
}

std::shared_ptr<ExplainInfo> TabletSdkImpl::Explain(const std::string& db,
                                                    const std::string& sql,
                                                    sdk::Status* status) {
    if (status == NULL) return std::shared_ptr<ExplainInfo>();
    ::hybridse::tablet::TabletServer_Stub stub(channel_);
    ::hybridse::tablet::ExplainRequest request;
    request.set_sql(sql);
    request.set_db(db);
    ::hybridse::tablet::ExplainResponse response;
    brpc::Controller cntl;
    stub.Explain(&cntl, &request, &response, NULL);

    if (cntl.Failed()) {
        status->code = (common::kConnError);
        status->msg = "rpc controller error " + cntl.ErrorText();
        return std::shared_ptr<ExplainInfo>();
    }

    if (response.status().code() != common::kOk) {
        status->code = response.status().code();
        status->msg = response.status().msg();
        status->trace = response.status().trace();
        return std::shared_ptr<ExplainInfo>();
    }

    vm::Schema internal_input_schema;
    bool ok = codec::SchemaCodec::Decode(response.input_schema(),
                                         &internal_input_schema);
    if (!ok) {
        status->msg = "fail to decode input schema";
        status->code = common::kSchemaCodecError;
        return std::shared_ptr<ExplainInfo>();
    }
    SchemaImpl input_schema(internal_input_schema);
    vm::Schema internal_output_schema;
    ok = codec::SchemaCodec::Decode(response.output_schema(),
                                    &internal_output_schema);
    if (!ok) {
        status->msg = "fail to decode output  schema";
        status->code = common::kSchemaCodecError;
        return std::shared_ptr<ExplainInfo>();
    }
    SchemaImpl output_schema(internal_output_schema);
    std::shared_ptr<ExplainInfoImpl> impl(new ExplainInfoImpl(
        input_schema, output_schema, response.logical_plan(),
        response.physical_plan(), response.ir()));
    status->code = common::kOk;
    return impl;
}

void TabletSdkImpl::GetSqlPlan(const std::string& db, const std::string& sql,
                               node::NodeManager& node_manager,
                               node::PlanNodeList& plan_trees,
                               sdk::Status& status) {
    base::Status sql_status;
    plan::PlanAPI::CreatePlanTreeFromScript(sql, plan_trees, &node_manager, sql_status);

    if (0 != sql_status.code) {
        status.code = sql_status.code;
        status.msg = sql_status.msg;
        status.trace = sql_status.GetTraces();
        return;
    }
}
void TabletSdkImpl::BuildInsertRequest(const std::string& db,
                                       const std::string& table_name,
                                       const std::vector<std::string>& columns,
                                       node::ExprListNode* values,
                                       tablet::InsertRequest* request,
                                       sdk::Status* status) {
    if (nullptr == values) {
        status->code = -1;
        status->msg = "Insert Values Is Null";
        return;
    }
    type::TableDef schema;
    if (!GetSchema(db, table_name, &schema, status)) {
        if (0 == status->code) {
            status->code = -1;
            status->msg = "Table Not Exist";
        }
        return;
    }
    request->set_table(table_name);
    request->set_db(db);

    std::unordered_set<std::string> column_set;
    for (int i = 0; i < schema.columns().size(); i++) {
        column_set.insert(schema.columns(i).name());
    }
    std::map<std::string, node::ConstNode*> column_value_map;
    if (!columns.empty()) {
        if (columns.size() != values->children_.size()) {
            status->msg = "Fail Build Request: insert column size != value size";
            status->code = -1;
            LOG(WARNING) << status->msg;
            return;
        }
        for (size_t i = 0; i < columns.size(); i++) {
            if (node::kExprPrimary != values->children_[i]->GetExprType()) {
                status->code = common::kTypeError;
                status->msg =
                    "Fail insert value with type " + node::ExprTypeName(values->children_[i]->GetExprType());
                return;
            }
            if (column_set.find(columns[i]) == column_set.end()) {
                status->code = common::kTypeError;
                status->msg =
                    "Fail insert: column " + columns[i] + "not exist";
                return;
            }
            column_value_map.insert(std::make_pair(columns[i], dynamic_cast<node::ConstNode*>(values->children_[i])));
        }
    } else {
        if (schema.columns().size() != static_cast<int32_t>(values->children_.size())) {
            status->msg = "Fail Build Request: insert column size != value size";
            status->code = -1;
            LOG(WARNING) << status->msg;
            return;
        }
        for (int i = 0; i < schema.columns().size(); i++) {
            if (node::kExprPrimary != values->children_[i]->GetExprType()) {
                status->code = common::kTypeError;
                status->msg = "Fail insert value with type " + node::ExprTypeName(values->children_[i]->GetExprType());
                return;
            }
            column_value_map.insert(
                std::make_pair(schema.columns(i).name(), dynamic_cast<node::ConstNode*>(values->children_[i])));
        }
    }
    uint32_t str_size = 0;
    for (auto column : schema.columns()) {
        std::string column_name = column.name();
        auto expr_node_it = column_value_map.find(column_name);
        if (expr_node_it == column_value_map.cend()) {
            continue;
        }
        if (type::kVarchar == column.type() && !expr_node_it->second->IsNull()) {
            str_size += strlen(expr_node_it->second->GetStr());
        }
    }

    codec::RowBuilder rb(schema.columns());
    uint32_t row_size = rb.CalTotalLength(str_size);
    std::string* row = request->mutable_row();
    row->resize(row_size);
    char* buf = reinterpret_cast<char*>(&(row->at(0)));
    rb.SetBuffer(reinterpret_cast<int8_t*>(buf), row_size);
    auto it = schema.columns().begin();
    uint32_t index = 0;
    for (; it != schema.columns().end(); ++it) {
        index++;
        std::string column_name = it->name();
        auto expr_node_it = column_value_map.find(column_name);
        if (expr_node_it == column_value_map.cend()) {
            if (it->is_not_null()) {
                status->code = common::kTypeError;
                status->msg = "Un-support insert null into NOT NULL column " + it->name();
                LOG(WARNING) << status->msg;
                return;
            }
            rb.AppendNULL();
            continue;
        }
        const node::ConstNode* primary = expr_node_it->second;
        if (primary->IsNull()) {
            if (it->is_not_null()) {
                status->code = common::kTypeError;
                status->msg = "Un-support insert null into NOT NULL column " + it->name();
                return;
            }
            rb.AppendNULL();
            continue;
        }
        bool ok = false;
        switch (it->type()) {
            case type::kNull: {
                ok = rb.AppendNULL();
                break;
            }
            case type::kBool: {
                ok = rb.AppendBool(primary->GetBool());
                break;
            }
            case type::kInt16: {
                ok = rb.AppendInt16(primary->GetAsInt16());
                break;
            }
            case type::kInt32: {
                ok = rb.AppendInt32(primary->GetAsInt32());
                break;
            }
            case type::kInt64: {
                ok = rb.AppendInt64(primary->GetAsInt64());
                break;
            }
            case type::kFloat: {
                ok = rb.AppendFloat(primary->GetAsFloat());
                break;
            }
            case type::kDouble: {
                ok = rb.AppendDouble(primary->GetAsDouble());
                break;
            }
            case type::kVarchar: {
                ok = rb.AppendString(primary->GetStr(), strlen(primary->GetStr()));
                break;
            }
            case type::kTimestamp: {
                ok = rb.AppendTimestamp(primary->GetAsInt64());
                break;
            }
            case type::kDate: {
                int32_t year;
                int32_t month;
                int32_t day;
                if (!primary->GetAsDate(&year, &month, &day)) {
                    ok = false;
                } else {
                    ok = rb.AppendDate(year, month, day);
                }
                break;
            }
            default: {
                status->code = common::kTypeError;
                status->msg = "can not handle data type " + node::DataTypeName(primary->GetDataType());
                return;
            }
        }
        if (!ok) {
            status->code = common::kTypeError;
            status->msg = "can not handle data type " + node::DataTypeName(primary->GetDataType());
            return;
        }
    }
    status->code = 0;
}

void TabletSdkImpl::Insert(const std::string& db, const std::string& sql, sdk::Status* status) {
    if (status == NULL) {
        LOG(WARNING) << "status is null";
        return;
    }
    node::PlanNodeList plan_trees;
    node::NodeManager node_manager;
    GetSqlPlan(db, sql, node_manager, plan_trees, *status);
    if (0 != status->code) {
        return;
    }
    if (plan_trees.empty() || nullptr == plan_trees[0]) {
        status->msg = "fail to execute plan : plan tree is empty or null";
        status->code = common::kPlanError;
        return;
    }
    node::PlanNode* plan = plan_trees[0];
    switch (plan->GetType()) {
        case node::kPlanTypeInsert: {
            node::InsertPlanNode* insert_plan = dynamic_cast<node::InsertPlanNode*>(plan);

            const node::InsertStmt* insert_stmt = insert_plan->GetInsertNode();
            if (nullptr == insert_stmt) {
                status->code = common::kNullPointer;
                status->msg = "fail to execute insert statement with null node";
                return;
            }
            for (auto iter = insert_stmt->values_.cbegin(); iter != insert_stmt->values_.cend(); iter++) {
                tablet::InsertRequest request;
                BuildInsertRequest(db, insert_stmt->table_name_, insert_stmt->columns_,
                                   dynamic_cast<node::ExprListNode*>(*iter), &request, status);
                if (status->code != 0) {
                    return;
                }
                Insert(request, status);
                if (common::kOk != status->code) {
                    return;
                }
            }

            return;
        }
        default: {
            status->code = common::kUnSupport;
            status->msg = "can not execute plan type " + node::NameOfPlanNodeType(plan->GetType());
            return;
        }
    }
}

std::shared_ptr<TabletSdk> CreateTabletSdk(const std::string& endpoint) {
    TabletSdkImpl* sdk = new TabletSdkImpl(endpoint);
    bool ok = sdk->Init();
    if (!ok) {
        delete sdk;
        return std::shared_ptr<TabletSdk>();
    }
    return std::shared_ptr<TabletSdk>(sdk);
}

}  // namespace sdk
}  // namespace hybridse
