//
// Copyright 2020 4paradigm
//
#pragma once
#include "client/client_type.h"

#include <set>
#include <memory>
#include <map>
#include <string>
#include <vector>

#include "client/tablet_client.h"
#include "codec/codec.h"
#include "codec/schema_codec.h"
#include "zk/zk_client.h"

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
    std::shared_ptr<google::protobuf::RepeatedPtrField<
        rtidb::common::ColumnDesc>> columns;
    std::vector<PartitionInfo> partition;
    std::vector<PartitionInfo> blob_partition;
    std::string auto_gen_pk_;
    std::vector<int32_t> blobSuffix;
    std::vector<std::string> blobFieldNames;
    std::string auto_gen_pk;
    std::map<std::string, ::rtidb::type::DataType> name_type_map;
};

struct GeneralResult {
    GeneralResult() : code(0), msg() {}

    explicit GeneralResult(int err_num) : code(err_num), msg() {}

    GeneralResult(int err_num, const std::string& error_msg)
        : code(err_num), msg(error_msg) {}

    void SetError(int err_num, const std::string& error_msg) {
        code = err_num;
        msg = error_msg;
    }
    const char* GetMsg() { return msg.data(); }
    int code;
    std::string msg;
};

struct PutResult : public GeneralResult {
    void SetAutoGenPk(int64_t num) {
        has_auto_gen_pk = true;
        auto_gen_pk = num;
    }
    int64_t auto_gen_pk;
    bool has_auto_gen_pk = false;
};

struct UpdateResult : public GeneralResult {
    UpdateResult(): affected_count(0) {}
    void SetAffectedCount(uint32_t num) {
        affected_count = num;
    }
    uint32_t affected_count;
};

struct ReadOption {
    explicit ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    }

    ReadOption() : index(), read_filter(), col_set(), limit(0) {}

    ~ReadOption() = default;

    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
};


class RtidbClient;

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
        std::string col;
        char* ch = nullptr;
        uint32_t length = 0;
        int ret = rv_->GetString(idx, &ch, &length);
        if (ret == 0) {
            col.assign(ch, length);
        }
        return col;
    }

    int64_t GetTimestamp(uint32_t idx) {
        int64_t val;
        rv_->GetTimestamp(idx, &val);
        return val;
    }

    int32_t GetDate(uint32_t idx) {
        int32_t val;
        rv_->GetDate(idx, &val);
        return val;
    }

    int64_t GetBlob(uint32_t idx) {
        rv_->GetBlob(idx, &curr_blob_key_);
        return curr_blob_key_;
    }

    BlobInfoResult GetBlobInfo();

    bool IsNULL(uint32_t idx) { return rv_->IsNULL(idx); }

    void SetRv(const std::shared_ptr<TableHandler>& th) {
        columns_ = th->columns;
        rv_ = std::make_shared<rtidb::codec::RowView>(*columns_);
        initialed_ = true;
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

    ViewResult() : rv_(), columns_(), initialed_(false), curr_blob_key_(),
                    table_name_(), client_(nullptr) {}

    ~ViewResult() = default;

    int64_t GetInt(uint32_t idx);

    const std::string& GetTableName() { return table_name_; }

    void SetTable(const std::string& name) { table_name_ = name; }

    void SetClient(RtidbClient* client) { client_ = client; }

    void SetBlobIdxVec(const std::vector<int32_t>& vec) {
        blob_idx_vec_ = vec;
    }
    const std::vector<int32_t>& GetBlobIdxVec() { return blob_idx_vec_; }

    std::shared_ptr<rtidb::codec::RowView> rv_;

 private:
    std::shared_ptr<google::protobuf::RepeatedPtrField<
        rtidb::common::ColumnDesc>> columns_;
    bool initialed_;
    int64_t curr_blob_key_;
    std::string table_name_;
    RtidbClient* client_;
    std::vector<int32_t> blob_idx_vec_;
};

class TraverseResult : public ViewResult {
 public:
    TraverseResult()
        : code_(0),
          msg_(),
          offset_(0),
          value_(),
          client_(nullptr),
          is_finish_(false),
          ro_(),
          table_name_(),
          count_(0),
          last_pk_(),
          snapshot_id_() {}

    void SetError(int err_code, const std::string& err_msg) {
        code_ = err_code;
        msg_ = err_msg;
    }

    void Init(RtidbClient* client, std::string* table_name,
              struct ReadOption* ro, uint32_t count, uint64_t snapshot_id);

    ~TraverseResult() = default;

    void SetValue(std::string* value, bool is_finish, const std::string& pk) {
        value_.reset(value);
        is_finish_ = is_finish;
        last_pk_ = pk;
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
    uint64_t snapshot_id_;
};

class BatchQueryResult : public ViewResult {
 public:
    BatchQueryResult() : code_(0), msg_(), offset_(0), value_(), count_(0) {}

    void SetError(int err_code, const std::string& err_msg) {
        code_ = err_code;
        msg_ = err_msg;
    }

    ~BatchQueryResult() = default;

    void SetValue(std::string* value, uint32_t count) {
        value_.reset(value);
        count_ = count;
    }

    bool Next();
    uint64_t Count() const { return count_; }

 public:
    int code_;
    std::string msg_;

 private:
    uint32_t offset_;
    std::shared_ptr<std::string> value_;
    uint32_t count_;
};

class BaseClient {
 public:
    BaseClient(const std::string& zk_cluster, const std::string& zk_root_path,
               const std::string& endpoint, int32_t zk_session_timeout,
               int32_t zk_keep_alive_check)
        : mu_(),
          tablets_(),
          blobs_(),
          tables_(),
          zk_client_(nullptr),
          zk_cluster_(zk_cluster),
          zk_root_path_(zk_root_path),
          endpoint_(endpoint),
          zk_session_timeout_(zk_session_timeout),
          zk_keep_alive_check_(zk_keep_alive_check),
          zk_table_data_path_(),
          task_thread_pool_(1),
          zk_client_session_term_(0) {}
    explicit BaseClient(
        const std::map<std::string,
                       std::shared_ptr<rtidb::client::TabletClient>>& tablets);
    ~BaseClient();

    bool Init(std::string* msg);
    void CheckZkClient();
    bool RefreshNodeList();
    void UpdateEndpoint(const std::set<std::string>& alive_endpoints);
    void UpdateBlobEndpoint(const std::set<std::string>& alive_endpoints);
    void RefreshTable();
    void SetZkCheckInterval(int32_t interval);
    void DoFresh(const std::vector<std::string>& events);
    bool RegisterZK(std::string* msg);
    std::shared_ptr<rtidb::client::TabletClient> GetTabletClient(
        const std::string& endpoint, std::string* msg);
    std::shared_ptr<rtidb::client::BsClient> GetBlobClient(
        const std::string& endpoint, std::string* msg);
    std::shared_ptr<TableHandler> GetTableHandler(const std::string& name);

 private:
    std::mutex mu_;
    std::map<std::string, std::shared_ptr<rtidb::client::TabletClient>>
        tablets_;
    std::map<std::string, std::shared_ptr<rtidb::client::BsClient>> blobs_;
    std::map<std::string, std::shared_ptr<TableHandler>> tables_;
    rtidb::zk::ZkClient* zk_client_;
    std::string zk_cluster_;
    std::string zk_root_path_;
    std::string endpoint_;
    int32_t zk_session_timeout_;
    int32_t zk_keep_alive_check_;
    std::string zk_table_data_path_;
    baidu::common::ThreadPool task_thread_pool_;
    uint64_t zk_client_session_term_;
};

class RtidbClient {
 public:
    RtidbClient();
    ~RtidbClient();
    GeneralResult Init(const std::string& zk_cluster,
                       const std::string& zk_path);
    PutResult Put(
        const std::string& name,
        const std::map<std::string, std::string>& value,
        const WriteOption& wo);
    UpdateResult Delete(const std::string& name,
                         const std::map<std::string, std::string>& values);
    TraverseResult Traverse(const std::string& name,
                            const struct ReadOption& ro);
    bool Traverse(const std::string& name, const struct ReadOption& ro,
                  std::string* data, uint32_t* count, std::string* last_key,
                  bool* is_finish, uint64_t* snapshot_id_);
    BatchQueryResult BatchQuery(const std::string& name,
                                const std::vector<ReadOption>& ros);
    bool BatchQuery(const std::string& name,
                    const google::protobuf::RepeatedPtrField<
                        ::rtidb::api::ReadOption>& ros_pb,
                    std::string* data, uint32_t* count, std::string* msg);
    void SetZkCheckInterval(int32_t interval);
    UpdateResult Update(
        const std::string& table_name,
        const std::map<std::string, std::string>& condition_map,
        const std::map<std::string, std::string>& value_map,
        const WriteOption& wo);
    std::vector<std::string>& GetBlobSchema(const std::string& name);
    BlobInfoResult GetBlobInfo(const std::string& name);

 private:
    BaseClient* client_;
    std::vector<std::string> empty_vector_;
};
