/*-------------------------------------------------------------------------
 * Copyright (C) 2020, 4paradigm
 * schema.h
 *
 * Author: chenjing
 * Date: 2020/4/20
 *--------------------------------------------------------------------------
 **/

#ifndef SRC_VM_SCHEMA_H_
#define SRC_VM_SCHEMA_H_
#include <map>
#include <string>
#include <utility>
#include <vector>
#include "node/sql_node.h"
namespace fesql {
namespace vm {
struct ColInfo {
    ::fesql::type::Type type;
    uint32_t pos;
    std::string name;
};

struct IndexSt {
    std::string name;
    uint32_t index;
    uint32_t ts_pos;
    std::vector<ColInfo> keys;
};

typedef ::google::protobuf::RepeatedPtrField<::fesql::type::ColumnDef> Schema;
typedef ::google::protobuf::RepeatedPtrField<::fesql::type::IndexDef> IndexList;
typedef std::map<std::string, ColInfo> Types;
typedef std::map<std::string, IndexSt> IndexHint;

struct RowSchemaInfo {
    const uint32_t idx;
    const std::string table_name_;
    const vm::Schema* schema_;
};

class SchemasContext {
 public:
    SchemasContext(
        const std::vector<std::pair<const std::string, const vm::Schema*>>&
            table_schema_list);

    bool ExprListResolved(std::vector<node::ExprNode*> expr_list,
                          const RowSchemaInfo** info) const;
    bool ExprRefResolved(const node::ExprNode* expr,
                         const RowSchemaInfo** info) const;

    bool AllRefResolved(const std::string& relation_name,
                        const RowSchemaInfo** info) const;
    bool ColumnRefResolved(const std::string& relation_name,
                           const std::string& col_name,
                           const RowSchemaInfo** info) const;

 public:
    // row ir context list
    std::vector<RowSchemaInfo> row_schema_info_list_;
    // column_name -> [context_id1, context_id2]
    std::map<std::string, std::vector<uint32_t>> col_context_id_map_;
    // table_name -> context_id1
    std::map<std::string, uint32_t> table_context_id_map_;
};
}  // namespace vm
}  // namespace fesql

#endif  // SRC_VM_SCHEMA_H_
