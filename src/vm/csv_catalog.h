/*
 * csv_catalog.h
 * Copyright (C) 4paradigm.com 2020 wangtaize <wangtaize@4paradigm.com>
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

#ifndef SRC_VM_CSV_CATALOG_H_
#define SRC_VM_CSV_CATALOG_H_

#include "vm/catalog.h"

#include "storage/codec.h"
#include "arrow/filesystem/api.h"
#include "arrow/io/api.h"
#include "arrow/csv/api.h"
#include "arrow/table.h"
#include "arrow/type_fwd.h"
#include "arrow/array.h"
#include "glog/logging.h"

namespace fesql {
namespace vm {

class SchemaParser {

 public:
    SchemaParser() {}
    ~SchemaParser() {}

    bool Parse(const std::string& path, 
            Schema* schema);

    bool Convert(const std::string& type,
            type::Type* db_type);
};

class ArrowTableIterator: public Iterator {
 public:
    ArrowTableIterator(const std::shared_ptr<arrow::Table>& table,
            const Schema& schema): table_(table), schema_(schema), 
    chunk_offset_(0), array_offset_(0),
    buf_(NULL), rb_(schema), buf_size_(0){}

    ~ArrowTableIterator() {
        delete buf_;
    }

    void Seek(uint64_t ts) {
        // not supported
    }

    void SeekToFirst() {}

    const uint64_t GetKey() {}

    const base::Slice GetValue() {
        return base::Slice(reinterpret_cast<char*>(buf_), buf_size_);
    }

    void Next() {
        if (table_->column(0)->chunk(chunk_offset_)->length() <= array_offset_ + 1) {
            chunk_offset_ += 1;
            array_offset_ = 0;
        }else {
            array_offset_ += 1;
        }
    }

    bool Valid() {
        if (table_->num_columns() <= 0) return false;
        if (table_->column(0)->num_chunks() <= chunk_offset_) return false;
        if (table_->column(0)->chunk(chunk_offset_)->length() <= array_offset_) return false;
        BuildRow();
        return true;
    }
 
 private:
    void BuildRow() {
        uint32_t str_size = 0;
        for (int32_t i = 0; i < schema_.size(); i++) {
            const type::ColumnDef& column = schema_.Get(i);
            if (column.type() == type::kVarchar) {
                auto chunked_array = table_->column(i);
                auto array = std::static_pointer_cast<arrow::StringArray>(chunked_array->chunk(chunk_offset_));
                str_size += array->GetView(array_offset_).size();
            }
        }
        uint32_t row_size = rb_.CalTotalLength(str_size);
        if (buf_ != NULL) {
            delete buf_;
        }
        buf_ = reinterpret_cast<int8_t*>(malloc(row_size));
        rb_.SetBuffer(buf_, row_size);
        buf_size_ = row_size;
        for (int32_t i = 0; i < schema_.size(); i++) {
            const type::ColumnDef& column = schema_.Get(i);
            auto chunked_array = table_->column(i);
            switch(column.type()) {
                case type::kInt16:
                    {
                        auto array = std::static_pointer_cast<arrow::Int16Array>(chunked_array->chunk(chunk_offset_));
                        int16_t value = array->Value(array_offset_);
                        rb_.AppendInt16(value);
                        break;
                    }
                case type::kInt32:
                    {
                        auto array = std::static_pointer_cast<arrow::Int32Array>(chunked_array->chunk(chunk_offset_));
                        int32_t value = array->Value(array_offset_);
                        rb_.AppendInt32(value);
                        break;
                    }
                case type::kInt64:
                    {
                        auto array = std::static_pointer_cast<arrow::Int64Array>(chunked_array->chunk(chunk_offset_));
                        int64_t value = array->Value(array_offset_);
                        rb_.AppendInt64(value);
                        break;
                    }
                case type::kFloat:
                    {
                        auto array = std::static_pointer_cast<arrow::FloatArray>(chunked_array->chunk(chunk_offset_));
                        float value = array->Value(array_offset_);
                        rb_.AppendFloat(value);
                        break;
                    }
                case type::kDouble:
                    {
                        auto array = std::static_pointer_cast<arrow::DoubleArray>(chunked_array->chunk(chunk_offset_));
                        double value = array->Value(array_offset_);
                        rb_.AppendDouble(value);
                        break;
                    }

                case type::kVarchar:
                    {
                        auto array = std::static_pointer_cast<arrow::StringArray>(chunked_array->chunk(chunk_offset_));
                        auto string_view = array->GetView(array_offset_);
                        rb_.AppendString(string_view.data(), string_view.size());
                        break;
                    }
                default :{
                    LOG(WARNING) << "type is not supported";
                }
            }
        }
    }

 private:
    const std::shared_ptr<arrow::Table> table_;
    const Schema schema_;
    uint64_t chunk_offset_;
    uint64_t array_offset_;
    int8_t* buf_;
    storage::RowBuilder rb_;
    uint32_t buf_size_;
};

class CSVTableHandler : public TableHandler {
 public:

    CSVTableHandler(const std::string& table_dir,
                    const std::string& table_name,
                    const std::string& db,
                    std::shared_ptr<::arrow::fs::FileSystem> fs);

    ~CSVTableHandler();

    bool Init();

    inline const Schema& GetSchema() {
        return schema_;
    }
    inline const std::string& GetName() {
        return table_name_;
    }
    inline const Types& GetTypes() {
        return types_;
    }
    inline const IndexList& GetIndex() {
        return index_list_;
    }

    std::unique_ptr<Iterator> GetIterator() {
        std::unique_ptr<ArrowTableIterator> it(new ArrowTableIterator(table_, schema_));
        return std::move(it);
    }

    inline const std::string& GetDatabase() {
        return db_;
    }

    std::unique_ptr<WindowIterator> GetWindowIterator(const std::string& idx_name) {}

 private:
    bool InitSchema();

    bool InitTable();

    bool InitOptions(arrow::csv::ConvertOptions* options);
 private:
    std::string table_dir_;
    std::string table_name_;
    std::string db_;
    Schema schema_;
    std::shared_ptr<arrow::Table> table_;
    std::shared_ptr<arrow::fs::FileSystem> fs_;
    Types types_;
    IndexList index_list_;
};

// csv catalog is local dbms
// the dir layout should be 
// root
//   |
//   db1
//    \t1
//      |
//       -schema
//       -data.csv
//       -index
//     t2

typedef std::map<std::string, std::map<std::string, std::shared_ptr<CSVTableHandler> > > CSVTables;
typedef std::map<std::string, std::shared_ptr<type::Database>> Databases;

class CSVCatalog : public Catalog {

 public:
    CSVCatalog(const std::string& root_dir);

    ~CSVCatalog();

    bool Init();

    std::shared_ptr<type::Database> GetDatabase(const std::string& db);
    std::shared_ptr<TableHandler> GetTable(const std::string& db, 
            const std::string& table_name);
 private:
    bool InitDatabase(const std::string& db);
 private:
    std::string root_dir_;
    CSVTables tables_;
    Databases dbs_;
    std::shared_ptr<::arrow::fs::FileSystem> fs_;
};

}  // namespace vm
}  // namespace fesql
#endif   // SRC_VM_CSV_CATALOG_H_
