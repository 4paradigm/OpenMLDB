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

#ifndef HYBRIDSE_INCLUDE_VM_SCHEMAS_CONTEXT_H_
#define HYBRIDSE_INCLUDE_VM_SCHEMAS_CONTEXT_H_
#include <map>
#include <set>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_status.h"
#include "codec/fe_row_codec.h"
#include "codec/row.h"
#include "node/sql_node.h"

namespace hybridse {
namespace vm {

// Forward decls
class PhysicalOpNode;
class PhysicalPlanContext;

class SchemaSource {
 public:
    const hybridse::codec::Schema* GetSchema() const { return schema_; }
    size_t GetColumnID(size_t idx) const;
    const std::string& GetColumnName(size_t idx) const;
    const hybridse::type::Type GetColumnType(size_t idx) const;
    const std::string& GetSourceName() const;
    const std::string& GetSourceDB() const;

    // build utility
    void SetSchema(const codec::Schema* schema);
    void SetSourceDBAndTableName(const std::string& db, const std::string& name);
    void SetColumnID(size_t idx, size_t column_id);
    void SetSource(size_t idx, size_t child_idx, size_t child_column_id);
    void SetNonSource(size_t idx);

    // source column tracking info
    int GetSourceColumnID(size_t idx) const;
    int GetSourceChildIdx(size_t idx) const;
    bool IsSourceColumn(size_t idx) const;
    bool IsStrictSourceColumn(size_t idx) const;

    size_t size() const;
    void Clear();

    std::string ToString() const;

 private:
    bool CheckSourceSetIndex(size_t idx) const;

    const codec::Schema* schema_;

    std::string source_name_ = "";
    std::string source_db_ = "";

    // column identifier of each output column
    std::vector<size_t> column_ids_;

    // trace which child and which column id each column come from
    // -1 means the column is created from current node
    std::vector<int> source_child_idxs_;
    std::vector<size_t> source_child_column_ids_;
};

/**
 * Utility context to resolve column spec into detailed column information.
 * This class should be explicitly initialized with schema source list info
 * or some physical node with schema intiailized.
 * If initialized by physical node, current context will take a relation name
 * used when column search is assiociated with a relation name. and the node
 * graph can be traversed to resolve column inherited from input nodes.
 */
class SchemasContext {
 public:
    SchemasContext() : root_(nullptr) {}
    explicit SchemasContext(const PhysicalOpNode* root) : root_(root) {}
    ~SchemasContext();

    /**
     * Given relation name and column name, return schema slice
     * index and column index within current context.
     */
    base::Status ResolveColumnIndexByName(const std::string& db_name,
                                          const std::string& relation_name,
                                          const std::string& column_name,
                                          size_t* schema_idx,
                                          size_t* col_idx) const;

    /**
     * Given unique column id, return schema slice index and
     * column index within schema slice within current context.
     */
    base::Status ResolveColumnIndexByID(size_t column_id, size_t* schema_idx,
                                        size_t* col_idx) const;

    /**
     * Given unique column id, return column name.
     */
    base::Status ResolveColumnNameByID(size_t column_id,
                                       std::string* name) const;

    /**
    * Given unique column id, return db, table, column name.
    */
    base::Status ResolveDbTableColumnByID(size_t column_id,
                                       std::string*db, std::string *table, std::string* column) const;

    /**
     * Resolve index for column reference expression
     */
    base::Status ResolveColumnRefIndex(const node::ColumnRefNode* column_ref,
                                       size_t* schema_idx,
                                       size_t* col_idx) const;
    /**
     * Resolve column id with given column expression [ColumnRefNode, ColumnId]
     */
    base::Status ResolveColumnID(const node::ExprNode* column, size_t* column_id) const;

    /**
     * Given relation name and column name, return column unique id
     * under current context.
     */
    base::Status ResolveColumnID(const std::string& db_name,
                                 const std::string& relation_name,
                                 const std::string& column_name,
                                 size_t* column_id) const;

    /**
     * Resolve source column by relation name and column name recursively.
     * If it can be resolved in current node, `child_path_id` is -1,
     * else `child_path_id` is the index of the child which the column
     * is resolved from.
     */
    base::Status ResolveColumnID(const std::string& db_name,
                                 const std::string& relation_name,
                                 const std::string& column_name,
                                 size_t* column_id, int* child_path_idx,
                                 size_t* child_column_id,
                                 size_t* source_column_id,
                                 const PhysicalOpNode** source_node) const;

    /**
     * Resolve all columns input expression will depend on.
     * Return column id list.
     */
    base::Status ResolveExprDependentColumns(const node::ExprNode* expr,
                                             std::set<size_t>* column_ids) const;
    base::Status ResolveExprDependentColumns(
        const node::ExprNode* expr,
        std::vector<const node::ExprNode*>* columns) const;

    /**
     * Get the relation name for this schema context, default ""
     */
    const std::string& GetName() const;
    const std::string& GetDBName() const;

    const PhysicalOpNode* GetRoot() const;

    /**
     * Get detailed format.
     */
    const codec::RowFormat* GetRowFormat() const;

    /**
     * Get `idx`th schema source.
     */
    const SchemaSource* GetSchemaSource(size_t idx) const;

    /**
     * Get raw schema for `idx`th schema source.
     */
    const codec::Schema* GetSchema(size_t idx) const;

    /**
     * Get num of total schema sources.
     */
    size_t GetSchemaSourceSize() const;

    /**
     * Check database name and relation name match with current context
     */
    bool CheckDatabaseAndRelation(const std::string& db, const std::string& table) const;

    /**
     * Set the database name and relation name for this schema context.
     */
    void SetDBAndRelationName(const std::string& db, const std::string& relation_name);

    /**
     * Set the defautl database name
     */
    void SetDefaultDBName(const std::string& default_db_name);

    const std::string& GetDefaultDBName() const { return default_db_name_; }
    /**
     * Add new schema source and return the mutable instance of added source.
     */
    SchemaSource* AddSource();

    /**
     * Add schema sources from child and inherit column identifiers.
     * New source is appended to the back.
     */
    void Merge(size_t child_idx, const SchemasContext* child);

    /**
     * Add schema sources from child with new column identifiers.
     * The source informations are set to traceback which child column
     * the new column is from.
     */
    void MergeWithNewID(size_t child_idx, const SchemasContext* child,
                        PhysicalPlanContext* plan_ctx);

    void Clear();
    void Build();

    bool Empty() const { return schema_sources_.empty(); }

    /**
     * Get total column num of all schema sources.
     */
    size_t GetColumnNum() const;

    const codec::Schema* GetOutputSchema() const;

    /**
     * Helper method to init schemas context with trival schema sources
     * this can be commonly used when no plan node is provided.
     */
    void BuildTrivial(const std::vector<const codec::Schema*>& schemas);
    void BuildTrivial(const std::string& default_db, const std::vector<const type::TableDef*>& tables);

 private:
    bool IsColumnAmbiguous(const std::string& column_name) const;

    bool CheckBuild() const;

    // root node to search column id, can be null
    const PhysicalOpNode* root_ = nullptr;
    std::string default_db_name_ = "";
    std::string root_db_name_ = "";
    std::string root_relation_name_ = "";

    // column id -> (schema idx, column idx) mapping
    std::map<size_t, std::pair<size_t, size_t>> column_id_map_;

    // column name -> [(schema idx, column idx)] mapping
    std::map<std::string, std::vector<std::pair<size_t, size_t>>> column_name_map_;

    // child source mapping
    // child idx -> (child column id -> column idx)
    std::map<size_t, std::map<size_t, size_t>> child_source_map_;

    // schema source parts
    std::vector<SchemaSource*> schema_sources_;

    // detailed schema format info
    codec::RowFormat* row_format_ = nullptr;

    // owned schema object
    codec::Schema owned_concat_output_schema_;
};

class RowParser {
 public:
    using Row = codec::Row;

    explicit RowParser(const SchemasContext* schema_ctx);
    int32_t GetValue(const Row& row, const node::ColumnRefNode& col, type::Type type,
                     void* val) const;
    int32_t GetValue(const Row& row, const node::ColumnRefNode& col, void* val) const;
    int32_t GetValue(const Row& row, const std::string& col, type::Type type, void* val) const;
    int32_t GetValue(const Row& row, const std::string& col, void* val) const;
    int32_t GetString(const Row& row, const std::string& col, std::string* val) const;
    int32_t GetString(const Row& row, const node::ColumnRefNode& col, std::string* val) const;

    type::Type GetType(const node::ColumnRefNode& col) const;
    type::Type GetType(const std::string& col) const;

    bool IsNull(const Row& row, const node::ColumnRefNode& col) const;
    bool IsNull(const Row& row, const std::string& col) const;

    const SchemasContext* schema_ctx() const {
        return schema_ctx_;
    }

 private:
    const SchemasContext* schema_ctx_ = nullptr;
    std::vector<codec::RowView> row_view_list_;
};

}  // namespace vm
}  // namespace hybridse

#endif  // HYBRIDSE_INCLUDE_VM_SCHEMAS_CONTEXT_H_
