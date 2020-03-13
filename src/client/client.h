#pragma once
#include "client/ns_client.h"
#include "client/tablet_client.h"
#include "zk/zk_client.h"
#include <string>
#include <set>
#include "base/codec.h"
#include "base/schema_codec.h"

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

struct GeneralResult {
    GeneralResult():code(0), msg() {}

    GeneralResult(int err_num):code(err_num), msg() {}

    GeneralResult(int err_num, const std::string error_msg):code(err_num), msg(error_msg) {}

    void SetError(int err_num, const std::string& error_msg) {
        code = err_num;
        msg = error_msg;
    }

    int code;
    std::string msg;
};

struct ReadOption {

    ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    };

    ReadOption(): index(), read_filter(), col_set(), limit(0) {
    }

    ~ReadOption() {
    }

    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
};

class QueryResult {
public:
    void SetError(int err_code, const std::string& err_msg) {
        code_ = err_code;
        msg_ = err_msg;
    }

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

    int64_t GetInt(uint32_t idx);

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
        char *ch = NULL;
        uint32_t length = 0;
        int ret = rv_->GetString(idx, &ch, &length);
        if (ret == 0) {
            col.assign(ch, length);
        }
        return col;
    }

    bool IsNULL(uint32_t idx) {
        return rv_->IsNULL(idx);
    }

    bool next() {
        int size = values_->size();
        if (size < 1 || index_ >= size) {
            return false;
        }
        uint32_t old_index = index_;
        index_++;
        return rv_->Reset(reinterpret_cast<int8_t*>(&((*(*values_)[old_index])[0])), (*values_)[old_index]->size());
    }

    uint64_t ValueSize() {
        return values_->size();
    }

    void SetRV(std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>>& schema) {
        columns_ = schema;
        rv_ = std::make_shared<rtidb::base::RowView>(*columns_);
    }

    QueryResult():code_(0), msg_(), index_(0), rv_(), columns_(), initialed_(false) {
        values_ = std::make_shared<std::vector<std::shared_ptr<std::string>>>();
    }

    ~QueryResult() {
    }

    std::map<std::string, std::string> DecodeData();

    void AddValue(std::shared_ptr<std::string>& value) {
        std::cout << *value << std::endl;
        values_->push_back(value);
    }
public:
    int code_;
    std::string msg_;
private:
    int index_;
    std::shared_ptr<std::vector<std::shared_ptr<std::string>>> values_;
    std::shared_ptr<rtidb::base::RowView> rv_;
    std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>> columns_;
    bool initialed_;
};

struct PartitionInfo {
    std::string leader;
    std::vector<std::string> follower;
};

struct TableHandler {
    std::shared_ptr<rtidb::nameserver::TableInfo>  table_info;
    std::shared_ptr<google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>> columns;
    std::vector<PartitionInfo> partition;
};

class RtidbClient {
public:
    RtidbClient();
    ~RtidbClient();
    GeneralResult Init(const std::string& zk_cluster, const std::string& zk_path);
    QueryResult Query(const std::string& name, const struct ReadOption& ro);
    GeneralResult Put(const std::string& name, const std::map<std::string, std::string>& values, const WriteOption& wo);
    GeneralResult Delete(const std::string& name, const std::map<std::string, std::string>& values);
    GeneralResult Update(const std::string& name, const std::map<std::string, std::string>& condition, const std::map<std::string, std::string> values, const WriteOption& wo);
    void SetZkCheckInterval(int32_t interval);

private:
    std::shared_ptr<rtidb::client::TabletClient> GetTabletClient(const std::string& endpoint, std::string& msg);
    void CheckZkClient();
    void UpdateEndpoint(const std::set<std::string>& alive_endpoints);
    bool RefreshNodeList();
    void RefreshTable();
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
