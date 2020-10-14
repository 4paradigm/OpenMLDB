/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * schema.h
 *
 * Author: chenjing
 * Date: 2020/4/20
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_SCHEMAS_CONTEXT_H_
#define SRC_VM_SCHEMAS_CONTEXT_H_
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "base/fe_status.h"
#include "node/sql_node.h"
#include "vm/catalog.h"
namespace fesql {
namespace vm {
struct RowSchemaInfo : public vm::SchemaSource {
 public:
    RowSchemaInfo(const uint32_t idx, const std::string& table,
                  const vm::Schema* schema, const vm::ColumnSourceList* sources)
        : SchemaSource(table, schema, sources), idx_(idx) {}
    const uint32_t idx_;
};

class SchemasContext {
 public:
    explicit SchemasContext(const vm::SchemaSourceList& table_schema_list);
    virtual ~SchemasContext() {}
    const bool Empty() const { return row_schema_info_list_.empty(); }
    bool ExprListResolvedFromSchema(
        const std::vector<node::ExprNode*>& expr_list,
        const RowSchemaInfo** info) const;
    bool ExprListResolved(
        const std::vector<node::ExprNode*>& expr_list,
        std::set<const RowSchemaInfo*>& infos) const;  // NOLINT
    bool ExprRefResolved(const node::ExprNode* expr,
                         const RowSchemaInfo** info) const;

    bool AllRefResolved(const std::string& relation_name,
                        const RowSchemaInfo** info) const;
    bool ColumnRefResolved(const std::string& relation_name,
                           const std::string& col_name,
                           const RowSchemaInfo** info) const;
    ColumnSource ColumnSourceResolved(node::ExprNode* expr);
    ColumnSource ColumnSourceResolved(const std::string& relation_name,
                                      const std::string& col_name) const;
    const std::string SourceColumnNameResolved(node::ExprNode* expr);
    base::Status ColumnTypeResolved(const std::string& relation_name,
                                    const std::string& col_name,
                                    fesql::type::Type* type);

    const codec::RowDecoder* GetDecoder(size_t slice_id) const;

 public:
    // row ir context list
    std::vector<RowSchemaInfo> row_schema_info_list_;
    // column_name -> [context_id1, context_id2]
    std::map<std::string, std::vector<uint32_t>> col_context_id_map_;
    // table_name -> context_id1
    std::map<std::string, uint32_t> table_context_id_map_;
    int32_t ColumnOffsetResolved(const std::string& relation_name,
                                 const std::string& col_name) const;
    int32_t ColumnOffsetResolved(const int32_t schema_idx,
                                 const int32_t column_idx) const;
    int32_t ColumnIdxResolved(const std::string& column,
                              const Schema* schema) const;

    std::vector<codec::RowDecoder> row_decoders_;
};
}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_SCHEMAS_CONTEXT_H_
