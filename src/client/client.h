//
// Copyright 2020 4paradigm
//
#pragma once
#include <map>
#include <memory>
#include <set>
#include <string>
#include <vector>

#include "base/codec.h"
#include "base/schema_codec.h"
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "zk/zk_client.h"

const std::set<rtidb::type::DataType> pk_type_set = {
    rtidb::type::kSmallInt, rtidb::type::kInt, rtidb::type::kBigInt,
    rtidb::type::kVarchar};

struct WriteOption {
    WriteOption() {
        update_if_equal = true;
        update_if_exist = true;
    }

    bool update_if_exist;
    bool update_if_equal;
};

struct ReadFilter {
    std::string column;
    uint8_t type;
    std::string value;
};

struct PartitionInfo {
    std::string leader;
    std::vector<std::string> follower;
};

struct TableHandler {
    std::shared_ptr<rtidb::nameserver::TableInfo> table_info;
    std::shared_ptr<
        google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>
        columns;
    std::vector<PartitionInfo> partition;
    int pk_index;
    rtidb::type::DataType pk_type;
    std::string auto_gen_pk_;
};

struct GeneralResult {
    GeneralResult() : code(0), msg() {}

    explicit GeneralResult(int err_num) : code(err_num), msg() {}

    GeneralResult(int err_num, const std::string error_msg)
        : code(err_num), msg(error_msg) {}

    void SetError(int err_num, const std::string& error_msg) {
        code = err_num;
        msg = error_msg;
    }

    int code;
    std::string msg;
};

struct ReadOption {
    explicit ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    }

    ReadOption() : index(), read_filter(), col_set(), limit(0) {}

    ~ReadOption() {}

    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
};

class ViewResult {
public:
    bool GetBool(uint32_t idx) {
        bool val;
        rv_->GetBool(idx, &val);
        return val;
    }
    int16_t GetInt16(uint32_t idx) {
        int16_t val;
        rv_->GetInt16(idx, &val);
        return val;
    }

    int32_t GetInt32(uint32_t idx) {
        int32_t val;
        rv_->GetInt32(idx, &val);
        return val;
    }

    int64_t GetInt64(uint32_t idx) {
        int64_t val;
        rv_->GetInt64(idx, &val);
        return val;
    }

    uint64_t GetSchemaSize() {
        if (!initialed_) {
            return 0;
        }
        return columns_->size();
    }

    int32_t GetColumnType(int32_t idx) {
        if (!initialed_) {
            return -1;
        }
        if (idx >= columns_->size()) {
            return -1;
        }
        return columns_->Get(idx).data_type();
    }

    std::vector<std::string> GetColumnsName() {
        std::vector<std::string> result;
        if (!initialed_) {
            return result;
        }
        for (int i = 0; i < columns_->size(); i++) {
            result.push_back(columns_->Get(i).name());
        }
        return result;
    }

    float GetFloat(uint32_t idx) {
        float val;
        rv_->GetFloat(idx, &val);
        return val;
    }

    double GetDouble(uint32_t idx) {
        double val;
        rv_->GetDouble(idx, &val);
        return val;
    }

    double GetFloatNum(uint32_t idx) {
        double val;
        auto type = columns_->Get(idx).data_type();
        if (type == rtidb::type::kFloat) {
            float f_val;
            rv_->GetFloat(idx, &f_val);
            val = f_val;
        } else {
            rv_->GetDouble(idx, &val);
        }
        return val;
    }

    std::string GetString(uint32_t idx) {
        std::string col = "";
        char* ch = NULL;
        uint32_t length = 0;
        int ret = rv_->GetString(idx, &ch, &length);
        if (ret == 0) {
            col.assign(ch, length);
        }
        return col;
    }

    bool IsNULL(uint32_t idx) { return rv_->IsNULL(idx); }

    void SetRv(const std::shared_ptr<TableHandler>& th) {
        columns_ = th->columns;
        rv_ = std::make_shared<rtidb::base::RowView>(*columns_);
        pk_idx_ = th->pk_index;
        data_type_ = th->pk_type;
        initialed_ = true;
    }

    std::string GetKey() {
        std::string key = "";
        if (data_type_ == rtidb::type::kSmallInt) {
            int16_t val = GetInt16(pk_idx_);
            key = std::to_string(val);
        } else if (data_type_ == rtidb::type::kInt) {
            int32_t val = GetInt32(pk_idx_);
            key = std::to_string(val);
        } else if (data_type_ == rtidb::type::kBigInt) {
            int64_t val = GetInt64(pk_idx_);
            key = std::to_string(val);
        } else if (data_type_ == rtidb::type::kVarchar) {
            key = GetString(pk_idx_);
        }
        return key;
    }

    ViewResult() : rv_(), columns_(), initialed_(false) {}

    ~ViewResult() {}

    int64_t GetInt(uint32_t idx);

    std::shared_ptr<rtidb::base::RowView> rv_;

private:
    std::shared_ptr<
        google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>
        columns_;
    bool initialed_;
    int pk_idx_;
    rtidb::type::DataType data_type_;
};

class QueryResult : public ViewResult {
public:
    void SetError(int err_code, const std::string& err_msg) {
        code_ = err_code;
        msg_ = err_msg;
    }

    bool Next() {
        int size = values_->size();
        if (size < 1 || index_ >= size) {
            return false;
        }
        uint32_t old_index = index_;
        index_++;
        return rv_->Reset(
            reinterpret_cast<int8_t*>(&((*(*values_)[old_index])[0])),
            (*values_)[old_index]->size());
    }

    uint64_t ValueSize() { return values_->size(); }

    QueryResult() : code_(0), msg_(), index_(0) {
        values_ = std::make_shared<std::vector<std::shared_ptr<std::string>>>();
    }

    ~QueryResult() {}

    void AddValue(const std::shared_ptr<std::string>& value) {
        values_->push_back(value);
    }

    void CleanValue() {
        values_->clear();
        index_ = 0;
    }

    bool IsEnd() { return static_cast<uint64_t>(index_) >= values_->size(); }

public:
    int code_;
    std::string msg_;

private:
    int32_t index_;
    std::shared_ptr<std::vector<std::shared_ptr<std::string>>> values_;
};

class RtidbClient;

class TraverseResult : public ViewResult {
public:
    TraverseResult(): code_(0),
        msg_(), offset_(0), value_(),
        client_(nullptr), is_finish_(false),
        ro_(), table_name_(), count_(0),
        last_pk_() {
    }

    void SetError(int err_code, const std::string& err_msg) {
        code_ = err_code;
        msg_ = err_msg;
    }

    void Init(RtidbClient* client, std::string* table_name, struct ReadOption* ro, uint32_t count);

    ~TraverseResult();

    void SetValue(std::string* value, bool is_finish) {
        value_.reset(value);
        is_finish_ = is_finish;
    }

    bool Next();

 public:
    int code_;
    std::string msg_;

private:
    bool TraverseNext();

    uint32_t offset_;
    std::shared_ptr<std::string> value_;
    RtidbClient* client_;
    bool is_finish_;
    std::shared_ptr<ReadOption> ro_;
    std::shared_ptr<std::string> table_name_;
    uint32_t count_;
    std::string last_pk_;
};

class BatchQueryResult: public ViewResult {
public:
    BatchQueryResult(): code_(0),
    msg_(), offset_(0), value_(),
    client_(nullptr), is_finish_(false),
    keys_(), table_name_(), already_get_(),
    count_(0) {
    }

    void SetError(int err_code, const std::string& err_msg) {
        code_ = err_code;
        msg_ = err_msg;
    }

    void Init(RtidbClient* client, std::string* table_name, const std::vector<std::string>& keys, uint32_t count);

    ~BatchQueryResult();

    void SetValue(std::string* value, bool is_finish) {
        value_.reset(value);
        is_finish_ = is_finish;
    }

    bool Next();

public:
    int code_;
    std::string msg_;

private:
    bool BatchQueryNext(const std::vector<std::string>& get_keys);

    uint32_t offset_;
    std::shared_ptr<std::string> value_;
    RtidbClient* client_;
    bool is_finish_;
    std::shared_ptr<std::vector<std::string>> keys_;
    std::shared_ptr<std::string> table_name_;
    std::set<std::string> already_get_;
    uint32_t count_;
};

class RtidbClient {
public:
    RtidbClient();
    ~RtidbClient();
    GeneralResult Init(const std::string& zk_cluster, const std::string& zk_path);
    QueryResult Query(const std::string& name, const struct ReadOption& ro);
    GeneralResult Put(const std::string& name, const std::map<std::string, std::string>& values, const WriteOption& wo);
    GeneralResult Delete(const std::string& name, const std::map<std::string, std::string>& values);
    TraverseResult Traverse(const std::string& name, const struct ReadOption& ro);
    bool Traverse(const std::string& name, const struct ReadOption& ro, std::string* data, uint32_t* count,
                               const std::string& last_key, bool* is_finish);
    BatchQueryResult BatchQuery(const std::string& name, const std::vector<ReadOption>& ros);
    bool BatchQuery(const std::string& name, const std::vector<std::string>& keys, std::string* data, bool* is_finish, uint32_t* count);
    void SetZkCheckInterval(int32_t interval);
    GeneralResult Update(const std::string& table_name, 
            const std::map<std::string, std::string>& condition_columns_map,
            const std::map<std::string, std::string>& value_columns_map,
            const WriteOption& wo); 

private:
    std::shared_ptr<rtidb::client::TabletClient> GetTabletClient(const std::string& endpoint, std::string* msg);
    void CheckZkClient();
    void UpdateEndpoint(const std::set<std::string>& alive_endpoints);
    bool RefreshNodeList();
    void RefreshTable();
    std::shared_ptr<TableHandler> GetTableHandler(const std::string& name);
    void DoFresh(const std::vector<std::string>& events) {
        RefreshNodeList();
        RefreshTable();
    }

private:
    std::shared_ptr<rtidb::zk::ZkClient> zk_client_;
    std::shared_ptr<rtidb::client::NsClient> client_;
    std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>> tablets_;
    std::mutex mu_;
    std::string zk_cluster_;
    std::string zk_path_;
    std::string zk_table_data_path_;
    std::map<std::string, std::shared_ptr<TableHandler>> tables_;
    baidu::common::ThreadPool task_thread_pool_;
    int32_t zk_keep_alive_check_;
};
