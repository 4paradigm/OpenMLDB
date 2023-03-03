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

#include "vm/schemas_context.h"
#include <set>
#include "passes/physical/physical_pass.h"
#include "vm/physical_op.h"

DECLARE_bool(enable_spark_unsaferow_format);

namespace hybridse {
namespace vm {

using hybridse::base::Status;
using hybridse::common::kColumnNotFound;

size_t SchemaSource::GetColumnID(size_t idx) const { return column_ids_[idx]; }

const std::string& SchemaSource::GetColumnName(size_t idx) const {
    return schema_->Get(idx).name();
}

const hybridse::type::Type SchemaSource::GetColumnType(size_t idx) const {
    return schema_->Get(idx).type();
}

bool SchemaSource::IsSourceColumn(size_t idx) const {
    return GetSourceChildIdx(idx) >= 0;
}

bool SchemaSource::IsStrictSourceColumn(size_t idx) const {
    size_t column_id = source_child_column_ids_[idx];
    return GetSourceChildIdx(idx) >= 0 && column_id > 0 &&
           column_id == GetColumnID(idx);
}

int SchemaSource::GetSourceChildIdx(size_t idx) const {
    return source_child_idxs_[idx];
}

int SchemaSource::GetSourceColumnID(size_t idx) const {
    return source_child_idxs_[idx] >= 0 ? source_child_column_ids_[idx] : 0;
}

bool SchemaSource::CheckSourceSetIndex(size_t idx) const {
    if (schema_ == nullptr) {
        LOG(WARNING) << "Can not set column before init schema";
        return false;
    } else if (idx >= static_cast<size_t>(schema_->size())) {
        LOG(WARNING) << "Column index out of bound: " << idx;
        return false;
    }
    return true;
}

void SchemaSource::SetSchema(const codec::Schema* schema) {
    if (schema == nullptr) {
        LOG(WARNING) << "Set source with null schema";
        return;
    }
    schema_ = schema;
    column_ids_.resize(schema->size(), 0);
    source_child_idxs_ = std::vector<int>(schema->size(), -1);
    source_child_column_ids_ = std::vector<size_t>(schema->size(), 0);
}

void SchemaSource::SetSourceDBAndTableName(const std::string& db, const std::string& name) {
    source_db_ = db;
    source_name_ = name;
}

const std::string& SchemaSource::GetSourceName() const { return source_name_; }
const std::string& SchemaSource::GetSourceDB() const { return source_db_; }

void SchemaSource::SetSource(size_t idx, size_t child_idx,
                             size_t child_column_id) {
    if (!CheckSourceSetIndex(idx)) {
        return;
    }
    source_child_idxs_[idx] = child_idx;
    source_child_column_ids_[idx] = child_column_id;
}

void SchemaSource::SetNonSource(size_t idx) {
    if (!CheckSourceSetIndex(idx)) {
        return;
    }
    source_child_idxs_[idx] = -1;
    source_child_column_ids_[idx] = 0;
}

void SchemaSource::SetColumnID(size_t idx, size_t column_id) {
    if (!CheckSourceSetIndex(idx)) {
        return;
    }
    column_ids_[idx] = column_id;
}

void SchemaSource::Clear() {
    schema_ = nullptr;
    source_name_ = "";
    column_ids_.clear();
    source_child_idxs_.clear();
    source_child_column_ids_.clear();
}

size_t SchemaSource::size() const {
    return schema_ == nullptr ? 0 : schema_->size();
}

std::string SchemaSource::ToString() const {
    std::stringstream ss;
    for (size_t i = 0; i < column_ids_.size(); ++i) {
        ss << "#" << std::to_string(column_ids_[i]);
        if (i < column_ids_.size() - 1) {
            ss << ", ";
        }
    }
    return ss.str();
}

void SchemasContext::Clear() {
    root_db_name_ = "";
    root_relation_name_ = "";
    default_db_name_ = "";
    column_id_map_.clear();
    column_name_map_.clear();
    child_source_map_.clear();
    for (auto ptr : schema_sources_) {
        delete ptr;
    }
    schema_sources_.clear();
    if (row_format_) {
        delete row_format_;
        row_format_ = nullptr;
    }
    owned_concat_output_schema_.Clear();
}

void SchemasContext::SetDBAndRelationName(const std::string& db, const std::string& relation_name) {
    root_db_name_ = db.empty() ? default_db_name_ : db;
    root_relation_name_ = relation_name;
}
void SchemasContext::SetDefaultDBName(const std::string& default_db_name) {
    default_db_name_ = default_db_name;
}
SchemaSource* SchemasContext::AddSource() {
    schema_sources_.push_back(new SchemaSource());
    return schema_sources_.back();
}

void SchemasContext::Merge(size_t child_idx, const SchemasContext* child) {
    for (size_t i = 0; i < child->GetSchemaSourceSize(); ++i) {
        auto source = child->GetSchemaSource(i);
        auto new_source = this->AddSource();
        new_source->SetSchema(source->GetSchema());
        // source can take the child name for detail showing
        std::string db_name = child->GetDBName();
        if (db_name.empty() && !source->GetSourceDB().empty()) {
            db_name = source->GetSourceDB();
        }
        std::string rel_name = child->GetName();
        if (rel_name.empty()&& !source->GetSourceName().empty()) {
            rel_name = source->GetSourceName();
        }
        new_source->SetSourceDBAndTableName(db_name, rel_name);
        for (size_t j = 0; j < source->size(); ++j) {
            // inherit child column id
            new_source->SetColumnID(j, source->GetColumnID(j));
            new_source->SetSource(j, child_idx, source->GetColumnID(j));
        }
    }
}

void SchemasContext::MergeWithNewID(size_t child_idx,
                                    const SchemasContext* child,
                                    PhysicalPlanContext* plan_ctx) {
    for (size_t i = 0; i < child->GetSchemaSourceSize(); ++i) {
        auto source = child->GetSchemaSource(i);
        auto new_source = this->AddSource();
        new_source->SetSchema(source->GetSchema());
        // source can take the child name for detail showing
        // take the first one db/relation name from SchemasContext & SchemaSource,
        // SchemasContext has higher priority
        std::string db_name = child->GetDBName();
        if (db_name.empty() && !source->GetSourceDB().empty()) {
            db_name = source->GetSourceDB();
        }
        std::string rel_name = child->GetName();
        if (rel_name.empty()&& !source->GetSourceName().empty()) {
            rel_name = source->GetSourceName();
        }
        new_source->SetSourceDBAndTableName(db_name, rel_name);
        for (size_t j = 0; j < source->size(); ++j) {
            // use new column id but record source child column id
            new_source->SetColumnID(j, plan_ctx->GetNewColumnID());
            new_source->SetSource(j, child_idx, source->GetColumnID(j));
        }
    }
}

SchemasContext::~SchemasContext() { Clear(); }

size_t SchemasContext::GetColumnNum() const {
    return GetOutputSchema()->size();
}

Status SchemasContext::ResolveColumnIndexByName(
    const std::string& db_name,
    const std::string& relation_name, const std::string& column_name,
    size_t* schema_idx, size_t* col_idx) const {
    CHECK_TRUE(this->CheckBuild(), kColumnNotFound,
               "Schemas context is not fully build");

    if (relation_name.empty()) {
        // if relation name not specified, resolve in current context only
        auto iter = column_name_map_.find(column_name);
        CHECK_TRUE(iter != column_name_map_.end(), kColumnNotFound,
                   "Fail to find column ", column_name);
        if (iter->second.size() > 1) {
            CHECK_TRUE(!IsColumnAmbiguous(column_name), common::kColumnAmbiguous,
                       "Ambiguous column name ", column_name);
        }
        auto pair = iter->second[0];
        *schema_idx = pair.first;
        *col_idx = pair.second;
        return Status::OK();
    } else if (root_ == nullptr) {
        // fallback logic if this is not a schema context bind to plan node
        auto iter = column_name_map_.find(column_name);
        CHECK_TRUE(iter != column_name_map_.end(), kColumnNotFound,
                   "Fail to find column ", column_name);
        bool found = false;
        size_t cur_column_id = 0;
        size_t cur_col_idx = 0;
        size_t cur_schema_idx = 0;
        for (auto& pair : iter->second) {
            auto source = GetSchemaSource(pair.first);
            if (source->GetSourceDB() == (db_name.empty() ? default_db_name_ : db_name) &&
                (!source->GetSourceName().empty() &&
                 source->GetSourceName() == (relation_name.empty() ? root_relation_name_ : relation_name))) {
                if (!found) {
                    found = true;
                    cur_column_id = source->GetColumnID(pair.second);
                    cur_col_idx = pair.second;
                    cur_schema_idx = pair.first;
                } else {
                    CHECK_TRUE(cur_column_id == source->GetColumnID(pair.second), common::kColumnAmbiguous,
                               "Ambiguous column name ", db_name, ".", relation_name, ".", column_name);
                }
            }
        }
        CHECK_TRUE(found, kColumnNotFound, "Fail to find column ",
                   relation_name, ".", column_name);
        *schema_idx = cur_schema_idx;
        *col_idx = cur_col_idx;
        return Status::OK();
    } else {
        // relation name specified, resolve with column unique id
        size_t column_id;
        int child_idx = -1;
        size_t child_column_id;
        size_t source_column_id;
        const PhysicalOpNode* source_node = nullptr;
        CHECK_STATUS(
            ResolveColumnID(db_name, relation_name, column_name, &column_id, &child_idx,
                            &child_column_id, &source_column_id, &source_node),
            "Fail to resolve column ", db_name, ".", relation_name, ".", column_name);

        // compute index under current context
        return ResolveColumnIndexByID(column_id, schema_idx, col_idx);
    }
}

Status SchemasContext::ResolveColumnIndexByID(size_t column_id,
                                              size_t* schema_idx,
                                              size_t* index) const {
    CHECK_TRUE(this->CheckBuild(), kColumnNotFound,
               "Schemas context is not fully build");
    auto find_iter = column_id_map_.find(column_id);
    CHECK_TRUE(find_iter != column_id_map_.end(), kColumnNotFound, "Fail to find column id #", column_id,
               " in current schema context")
    *schema_idx = find_iter->second.first;
    *index = find_iter->second.second;
    return Status::OK();
}
Status SchemasContext::ResolveDbTableColumnByID(size_t column_id,
                                   std::string*db, std::string *table, std::string* column) const {
    CHECK_TRUE(this->CheckBuild(), kColumnNotFound,
               "Schemas context is not fully build");
    auto iter = column_id_map_.find(column_id);
    CHECK_TRUE(iter != column_id_map_.end(), kColumnNotFound,
               "Fail to find column id #", column_id,
               " in current schema context");
    auto sc = GetSchemaSource(iter->second.first);

    CHECK_TRUE(sc != nullptr, kColumnNotFound, iter->second.first,
               "th schema source not found");
    *db = sc->GetSourceDB();
    *table = sc->GetSourceName();
    auto schema = sc->GetSchema();
    CHECK_TRUE(schema != nullptr, kColumnNotFound, iter->second.first,
               "th schema not found");
    *column = schema->Get(iter->second.second).name();
    return Status::OK();
}
Status SchemasContext::ResolveColumnNameByID(size_t column_id,
                                             std::string* name) const {
    CHECK_TRUE(this->CheckBuild(), kColumnNotFound,
               "Schemas context is not fully build");
    auto iter = column_id_map_.find(column_id);
    CHECK_TRUE(iter != column_id_map_.end(), kColumnNotFound,
               "Fail to find column id #", column_id,
               " in current schema context");
    auto sc = GetSchema(iter->second.first);
    CHECK_TRUE(sc != nullptr, kColumnNotFound, iter->second.first,
               "th schema not found");
    *name = sc->Get(iter->second.second).name();
    return Status::OK();
}

Status SchemasContext::ResolveColumnRefIndex(
    const node::ColumnRefNode* column_ref, size_t* schema_idx,
    size_t* col_idx) const {
    CHECK_TRUE(this->CheckBuild(), kColumnNotFound,
               "Schemas context is not fully build");
    CHECK_TRUE(column_ref != nullptr, kColumnNotFound);
    return ResolveColumnIndexByName(column_ref->GetDBName(),
                                    column_ref->GetRelationName(),
                                    column_ref->GetColumnName(), schema_idx,
                                    col_idx);
}

Status SchemasContext::ResolveColumnID(const std::string& db_name,
                                       const std::string& relation_name,
                                       const std::string& column_name,
                                       size_t* column_id) const {
    CHECK_TRUE(this->CheckBuild(), kColumnNotFound,
               "Schemas context is not fully build");
    size_t schema_idx;
    size_t col_idx;
    CHECK_STATUS(ResolveColumnIndexByName(db_name, relation_name, column_name,
                                          &schema_idx, &col_idx));
    *column_id = GetSchemaSource(schema_idx)->GetColumnID(col_idx);
    return Status::OK();
}

Status DoSearchExprDependentColumns(const node::ExprNode* expr, std::vector<const node::ExprNode*>* columns) {
    if (expr == nullptr) {
        return Status::OK();
    }
    for (size_t i = 0; i < expr->GetChildNum(); ++i) {
        CHECK_STATUS(
            DoSearchExprDependentColumns(expr->GetChild(i), columns));
    }
    switch (expr->expr_type_) {
        case node::kExprColumnRef: {
            columns->push_back(expr);
            break;
        }
        case node::kExprColumnId: {
            columns->push_back(expr);
            break;
        }
        case node::kExprBetween: {
            std::vector<node::ExprNode*> expr_list;
            auto between_expr = dynamic_cast<const node::BetweenExpr*>(expr);
            CHECK_STATUS(DoSearchExprDependentColumns(between_expr->GetLow(),
                                                      columns));
            CHECK_STATUS(DoSearchExprDependentColumns(between_expr->GetHigh(),
                                                      columns));
            CHECK_STATUS(DoSearchExprDependentColumns(between_expr->GetLhs(),
                                                      columns));
            break;
        }
        case node::kExprCall: {
            auto call_expr = dynamic_cast<const node::CallExprNode*>(expr);
            if (nullptr != call_expr->GetOver()) {
                auto orders = call_expr->GetOver()->GetOrders();
                if (nullptr != orders) {
                    CHECK_STATUS(
                        DoSearchExprDependentColumns(orders, columns));
                }
                auto partitions = call_expr->GetOver()->GetPartitions();
                if (nullptr != partitions) {
                    CHECK_STATUS(
                        DoSearchExprDependentColumns(partitions, columns));
                }
            }
            break;
        }
        case node::kExprOrderExpression: {
            auto refx = dynamic_cast<const node::OrderExpression*>(expr);
            CHECK_TRUE(refx != nullptr, common::kTypeError);
            CHECK_STATUS(DoSearchExprDependentColumns(refx->expr(), columns));

            break;
        }
        default:
            break;
    }
    return Status::OK();
}

Status SchemasContext::ResolveExprDependentColumns(
    const node::ExprNode* expr, std::set<size_t>* column_ids) const {
    std::vector<const node::ExprNode*> columns;
    CHECK_STATUS(DoSearchExprDependentColumns(expr, &columns));

    column_ids->clear();
    for (auto col_expr : columns) {
        switch (col_expr->GetExprType()) {
            case node::kExprColumnRef: {
                auto column_ref =
                    dynamic_cast<const node::ColumnRefNode*>(col_expr);
                size_t schema_idx;
                size_t col_idx;
                CHECK_STATUS(
                    ResolveColumnRefIndex(column_ref, &schema_idx, &col_idx));
                column_ids->insert(
                    GetSchemaSource(schema_idx)->GetColumnID(col_idx));
                break;
            }
            case node::kExprColumnId: {
                auto column_id =
                    dynamic_cast<const node::ColumnIdNode*>(col_expr);
                size_t schema_idx;
                size_t col_idx;
                CHECK_STATUS(ResolveColumnIndexByID(column_id->GetColumnID(),
                                                    &schema_idx, &col_idx));
                column_ids->insert(column_id->GetColumnID());
                break;
            }
            default:
                break;
        }
    }
    return Status::OK();
}

Status SchemasContext::ResolveExprDependentColumns(
    const node::ExprNode* expr,
    std::vector<const node::ExprNode*>* columns) const {
    std::vector<const node::ExprNode*> search_columns;
    CHECK_STATUS(DoSearchExprDependentColumns(expr, &search_columns));

    std::set<size_t> column_id_set;
    std::set<std::string> column_name_set;
    columns->clear();
    for (auto col_expr : search_columns) {
        switch (col_expr->GetExprType()) {
            case node::kExprColumnRef: {
                auto column_ref =
                    dynamic_cast<const node::ColumnRefNode*>(col_expr);
                auto name = column_ref->GetExprString();
                auto iter = column_name_set.find(name);
                if (iter == column_name_set.end()) {
                    columns->push_back(column_ref);
                    column_name_set.insert(iter, name);
                }
                break;
            }
            case node::kExprColumnId: {
                auto column_id =
                    dynamic_cast<const node::ColumnIdNode*>(col_expr);
                auto iter = column_id_set.find(column_id->GetColumnID());
                if (iter == column_id_set.end()) {
                    columns->push_back(column_id);
                    column_id_set.insert(iter, column_id->GetColumnID());
                }
                break;
            }
            default:
                break;
        }
    }
    return Status::OK();
}

bool SchemasContext::IsColumnAmbiguous(const std::string& column_name) const {
    auto iter = column_name_map_.find(column_name);
    if (iter == column_name_map_.end()) {
        return true;  // not found is worse than ambiguous
    }
    std::set<size_t> column_id_set;
    for (auto& pair : iter->second) {
        column_id_set.insert(
            schema_sources_[pair.first]->GetColumnID(pair.second));
    }
    return column_id_set.size() != 1;
}

const codec::RowFormat* SchemasContext::GetRowFormat() const {
    return row_format_;
}

const std::string& SchemasContext::GetName() const {
    return root_relation_name_;
}
const std::string& SchemasContext::GetDBName() const {
    return root_db_name_;
}

const PhysicalOpNode* SchemasContext::GetRoot() const { return root_; }

const codec::Schema* SchemasContext::GetSchema(size_t idx) const {
    return idx < schema_sources_.size() ? schema_sources_[idx]->GetSchema()
                                        : nullptr;
}

const SchemaSource* SchemasContext::GetSchemaSource(size_t idx) const {
    return idx < schema_sources_.size() ? schema_sources_[idx] : nullptr;
}

size_t SchemasContext::GetSchemaSourceSize() const {
    return schema_sources_.size();
}

const codec::Schema* SchemasContext::GetOutputSchema() const {
    if (schema_sources_.size() == 1) {
        return schema_sources_[0]->GetSchema();
    } else {
        return &owned_concat_output_schema_;
    }
}

bool SchemasContext::CheckBuild() const {
    return row_format_ != nullptr;
}

void SchemasContext::Build() {
    // initialize detailed formats
    if (row_format_) {
        delete row_format_;
        row_format_ = nullptr;
    }
    std::vector<const hybridse::codec::Schema*> schemas;
    for (const auto& source : schema_sources_) {
        if (source->GetSchema() == nullptr) {
            LOG(WARNING) << "Source schema is null";
            return;
        }
        schemas.emplace_back(source->GetSchema());
    }

    if (FLAGS_enable_spark_unsaferow_format) {
        row_format_ = new codec::SingleSliceRowFormat(schemas);
    } else {
        row_format_ = new codec::MultiSlicesRowFormat(schemas);
    }

    // initialize mappings
    column_id_map_.clear();
    column_name_map_.clear();
    child_source_map_.clear();
    for (size_t i = 0; i < schema_sources_.size(); ++i) {
        const SchemaSource* source = schema_sources_[i];
        auto schema = source->GetSchema();
        for (auto j = 0; j < schema->size(); ++j) {
            column_name_map_[schema->Get(j).name()].emplace_back(i, j);
            size_t column_id = source->GetColumnID(j);

            // column id can be duplicate and
            // we do not care which one is resolved to.
            column_id_map_[column_id] = std::make_pair(i, j);

            // fill source mapping if it exists
            if (source->IsSourceColumn(j)) {
                child_source_map_[source->GetSourceChildIdx(j)]
                                 [source->GetSourceColumnID(j)] = column_id;
            }
        }
    }
    // initialize output schema
    if (schema_sources_.size() != 1) {
        owned_concat_output_schema_.Clear();
        for (size_t i = 0; i < schema_sources_.size(); ++i) {
            auto schema = schema_sources_[i]->GetSchema();
            owned_concat_output_schema_.MergeFrom(*schema);
        }
    }
}
bool SchemasContext::CheckDatabaseAndRelation(const std::string& db, const std::string& relation) const {
    if ((db.empty() ? default_db_name_ : db) == (root_db_name_.empty() ? default_db_name_ : root_db_name_)) {
        if (relation == "" || relation == root_relation_name_) {
            return true;
        } else {
            return false;
        }
    }
    return false;
}
Status SchemasContext::ResolveColumnID(
    const std::string& db_name,
    const std::string& relation_name, const std::string& column_name,
    size_t* column_id, int* child_path_idx, size_t* child_column_id,
    size_t* source_column_id, const PhysicalOpNode** source_node) const {
    // current context match relation name
    if (CheckDatabaseAndRelation(db_name, relation_name)) {
        auto iter = column_name_map_.find(column_name);
        if (iter != column_name_map_.end()) {
            // exit if find ambiguous match
            if (iter->second.size() > 1) {
                CHECK_TRUE(!IsColumnAmbiguous(column_name), common::kColumnAmbiguous,
                           "Ambiguous column name ", db_name, ".", relation_name, ".",
                           column_name);
            }

            // find non-ambiguous match column
            size_t schema_idx = iter->second[0].first;
            size_t col_idx = iter->second[0].second;
            const SchemaSource* source = schema_sources_[schema_idx];
            *column_id = source->GetColumnID(col_idx);
            *child_path_idx = source->GetSourceChildIdx(col_idx);
            *child_column_id = *column_id;

            // backtrace to the final source info
            size_t cur_column_id = *column_id;
            int child_col_id = source->GetSourceColumnID(col_idx);
            int path_idx = source->GetSourceChildIdx(col_idx);
            const PhysicalOpNode* cur_node = root_;
            while (path_idx >= 0 && child_col_id >= 0 && cur_node != nullptr) {
                cur_node = cur_node->GetProducer(path_idx);
                auto child_ctx = cur_node->schemas_ctx();
                size_t child_schema_idx;
                size_t child_col_idx;
                CHECK_STATUS(
                    child_ctx->ResolveColumnIndexByID(
                        child_col_id, &child_schema_idx, &child_col_idx),
                    "Illegal column id #", child_col_id,
                    " in schema context of\n", cur_node->GetTreeString());

                const SchemaSource* child_source =
                    child_ctx->GetSchemaSource(child_schema_idx);
                cur_column_id = child_source->GetColumnID(child_col_idx);
                child_col_id = child_source->GetSourceColumnID(child_col_idx);
                path_idx = child_source->GetSourceChildIdx(child_col_idx);
            }
            *source_column_id = cur_column_id;
            *source_node = cur_node;
            return Status::OK();
        }
    }

    // find recursively if node information is specified
    CHECK_TRUE(root_ != nullptr, kColumnNotFound, "Not found: ", db_name, ".", relation_name, ".", column_name);
    bool found = false;
    const auto& children = root_->GetProducers();
    for (size_t i = 0; i < children.size(); ++i) {
        const SchemasContext* child_ctx = children[i]->schemas_ctx();

        size_t cur_child_column_id;

        int sub_child_path_idx = -1;
        size_t sub_child_column_id;
        size_t sub_source_column_id;
        const PhysicalOpNode* sub_source_node = nullptr;

        Status status = child_ctx->ResolveColumnID(db_name,
            relation_name, column_name, &cur_child_column_id,
            &sub_child_path_idx, &sub_child_column_id, &sub_source_column_id,
            &sub_source_node);

        if (!status.isOK()) {
            if (common::kColumnAmbiguous == status.code) {
                CHECK_STATUS(status);
            }
            continue;
        }

        // found match in recursive child
        // try to mapping to column id in current context if possible
        size_t cand_column_id;
        if (column_id_map_.find(cur_child_column_id) != column_id_map_.end()) {
            cand_column_id = cur_child_column_id;
        } else {
            // use column id source mapping
            auto child_iter = child_source_map_.find(i);
            if (child_iter == child_source_map_.end()) {
                continue;
            }
            auto& child_dict = child_iter->second;
            auto id_iter = child_dict.find(cur_child_column_id);
            if (id_iter == child_dict.end()) {
                continue;
            }
            cand_column_id = id_iter->second;
        }

        // check if candidate is ambiguous
        if (found) {
            CHECK_TRUE(*column_id == cand_column_id, common::kColumnAmbiguous,
                       "Ambiguous column ", db_name, ".", relation_name, ".", column_name,
                       ": #", *column_id, " and #", cand_column_id);
        } else {
            found = true;
            *column_id = cand_column_id;
            *child_path_idx = i;
            *child_column_id = cur_child_column_id;
            *source_column_id = sub_source_column_id;
            *source_node = sub_source_node;
        }
    }
    CHECK_TRUE(found, kColumnNotFound, "Column Not found: ", db_name, ".", relation_name, ".", column_name);
    return base::Status::OK();
}

void SchemasContext::BuildTrivial(
    const std::vector<const codec::Schema*>& schemas) {
    size_t column_id = 1;
    for (auto schema : schemas) {
        auto source = this->AddSource();
        source->SetSourceDBAndTableName("", "");
        source->SetSchema(schema);
        for (int i = 0; i < schema->size(); ++i) {
            source->SetColumnID(i, column_id);
            column_id += 1;
        }
    }
    this->Build();
}

void SchemasContext::BuildTrivial(
    const std::string& default_db_name,
    const std::vector<const type::TableDef*>& tables) {
    default_db_name_ = default_db_name;
    size_t column_id = 1;
    for (auto table : tables) {
        auto schema = &table->columns();
        auto source = this->AddSource();
        source->SetSchema(schema);
        source->SetSourceDBAndTableName(table->catalog(), table->name());
        for (int i = 0; i < schema->size(); ++i) {
            source->SetColumnID(i, column_id);
            column_id += 1;
        }
    }
    this->Build();
}

RowParser::RowParser(const SchemasContext* schema_ctx) : schema_ctx_(schema_ctx) {
    for (size_t i = 0; i < schema_ctx_->GetSchemaSourceSize(); ++i) {
        auto source = schema_ctx_->GetSchemaSource(i);
        row_view_list_.push_back(codec::RowView(*source->GetSchema()));
    }
}

bool RowParser::IsNull(const Row& row, const node::ColumnRefNode& col) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnRefIndex(&col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    return row_view.IsNULL(row.buf(schema_idx), col_idx);
}

bool RowParser::IsNull(const Row& row, const std::string& col) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnIndexByName("", "", col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    return row_view.IsNULL(row.buf(schema_idx), col_idx);
}

int32_t RowParser::GetValue(const Row& row, const node::ColumnRefNode& col, ::hybridse::type::Type type,
                            void* val) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnRefIndex(&col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    return row_view.GetValue(row.buf(schema_idx), col_idx, type, val);
}

int32_t RowParser::GetValue(const Row& row, const node::ColumnRefNode& col, void* val) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnRefIndex(&col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    auto& col_def = row_view.GetSchema()->Get(col_idx);
    return row_view.GetValue(row.buf(schema_idx), col_idx, col_def.type(), val);
}

int32_t RowParser::GetValue(const Row& row, const std::string& col, ::hybridse::type::Type type, void* val) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnIndexByName("", "", col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    return row_view.GetValue(row.buf(schema_idx), col_idx, type, val);
}

int32_t RowParser::GetValue(const Row& row, const std::string& col, void* val) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnIndexByName("", "", col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    auto& col_def = row_view.GetSchema()->Get(col_idx);
    return row_view.GetValue(row.buf(schema_idx), col_idx, col_def.type(), val);
}

int32_t RowParser::GetString(const Row& row, const std::string& col, std::string* val) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnIndexByName("", "", col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    const char* ch = nullptr;
    uint32_t str_size;
    int ret = row_view.GetValue(row.buf(schema_idx), col_idx, &ch, &str_size);
    if (0 != ret) {
        return ret;
    }

    std::string tmp(ch, str_size);
    val->swap(tmp);
    return 0;
}

int32_t RowParser::GetString(const Row& row, const node::ColumnRefNode& col, std::string* val) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnRefIndex(&col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    const char* ch = nullptr;
    uint32_t str_size;
    row_view.GetValue(row.buf(schema_idx), col_idx, &ch, &str_size);

    std::string tmp(ch, str_size);
    val->swap(tmp);
    return 0;
}

type::Type RowParser::GetType(const std::string& col) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnIndexByName("", "", col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    auto& col_def = row_view.GetSchema()->Get(col_idx);
    return col_def.type();
}

type::Type RowParser::GetType(const node::ColumnRefNode& col) const {
    size_t schema_idx, col_idx;
    schema_ctx_->ResolveColumnRefIndex(&col, &schema_idx, &col_idx);
    const codec::RowView& row_view = row_view_list_[schema_idx];
    auto& col_def = row_view.GetSchema()->Get(col_idx);
    return col_def.type();
}

}  // namespace vm
}  // namespace hybridse
