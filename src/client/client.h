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
        updateIfEqual = true;
        updateIfEqual = true;
    }

    bool updateIfExist;
    bool updateIfEqual;
};

struct ReadFilter {
    std::string column;
    uint8_t type;
    std::string value;
};

struct GetColumn {
    uint8_t type;
    std::string buffer;
};

struct QueryRawResult {
    int code;
    std::string msg;
    std::map<std::string, std::string> values;
    QueryRawResult():code(0), msg() {
    };
    void SetError(int err_code, const std::string& err_msg) {
        code = err_code;
        msg = err_msg;
    }
};

struct GeneralResult {
    int code;
    std::string msg;
    GeneralResult():code(0), msg() {}
    GeneralResult(int err_num):code(err_num), msg() {}
    GeneralResult(int err_num, const std::string error_msg):code(err_num), msg(error_msg) {}
    void SetError(int err_num, const std::string& error_msg) {
        code = err_num;
        msg = error_msg;
    }
};

struct ReadOption {
    std::map<std::string, std::string> index;
    std::vector<ReadFilter> read_filter;
    std::set<std::string> col_set;
    uint64_t limit;
    ReadOption(const std::map<std::string, std::string>& indexs) {
        index.insert(indexs.begin(), indexs.end());
    };
    ReadOption(): index(), read_filter(), col_set(), limit(0) {
    }
    ~ReadOption() {
    }
};

struct RowViewResult {
    int code;
    std::string msg;
    std::vector<std::string> values;
    uint32_t index;
    std::shared_ptr<rtidb::base::RowView> rv;
    std::vector<rtidb::common::ColumnDesc> columns;

    void SetError(int err_code, const std::string& err_msg) {
        code = err_code;
        msg = err_msg;
    }

    int16_t GetInt16(uint32_t idx) {
        int16_t val;
        rv->GetInt16(idx, &val);
        return val;
    }

    int32_t GetInt32(uint32_t idx) {
        int32_t val;
        rv->GetInt32(idx, &val);
        return val;
    }

    int64_t GetInt64(uint32_t idx) {
        int64_t val;
        rv->GetInt64(idx, &val);
        return val;
    }

    int64_t GetInt(uint32_t idx) {
        int64_t val = 0;
        auto type = columns[idx].data_type();
        if (type == rtidb::type::kInt16) {
            int16_t st_val;
            rv->GetInt16(idx, &st_val);
            val = st_val;
        } else if (type == rtidb::type::kInt32) {
            int32_t tt_val;
            rv->GetInt32(idx, &tt_val);
            val = tt_val;
        } else {
            int64_t val;
            rv->GetInt64(idx, &val);
        }
        return val;
    }

    float GetFloat(uint32_t idx) {
        float val;
        rv->GetFloat(idx, &val);
        return val;
    }

    double GetDouble(uint32_t idx) {
        double val;
        rv->GetDouble(idx, &val);
        return val;
    }

    double GetFloatNum(uint32_t idx) {
        double val;
        auto type = columns[idx].data_type();
        if (type == rtidb::type::kFloat) {
            float f_val;
            rv->GetFloat(idx, &f_val);
            val = f_val;
        } else {
            rv->GetDouble(idx, &val);
        }
        return val;
    }

    std::string GetString(uint32_t idx) {
        std::string col = "";
        char *ch = NULL;
        uint32_t length = 0;
        int ret = rv->GetString(idx, &ch, &length);
        if (ret == 0) {
            col.assign(ch, length);
        }
        return col;
    }

    bool IsNULL(uint32_t idx) {
        return rv->IsNULL(idx);
    }

    bool next() {
        if (values.size() < 1 || index >= values.size()) {
            return false;
        }
        uint32_t old_index = index;
        index++;
        return rv->Reset(reinterpret_cast<int8_t*>(&values[old_index][0]), values[old_index].size());
    }

    std::map<std::string, std::string> DecodeData() {
        std::map<std::string, std::string> result;
        for (int64_t i = 0; i < columns.size(); i++) {
            std::string col_name = columns[i].name();
            if (rv->IsNULL(i)) {
                result.insert(std::make_pair(col_name, rtidb::base::NONETOKEN));
            }
            std::string col = "";
            auto type = columns[i].data_type();
            if (type == rtidb::type::kInt32) {
                int32_t val;
                int ret = rv->GetInt32(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kTimestamp) {
                int64_t val;
                int ret = rv->GetTimestamp(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kInt64) {
                int64_t val;
                int ret = rv->GetInt64(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kBool) {
                bool val;
                int ret = rv->GetBool(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kFloat) {
                float val;
                int ret = rv->GetFloat(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kInt16) {
                int16_t val;
                int ret = rv->GetInt16(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kDouble) {
                double val;
                int ret = rv->GetDouble(i, &val);
                if (ret == 0) {
                    col = std::to_string(val);
                }
            } else if (type == rtidb::type::kVarchar) {
                char *ch = NULL;
                uint32_t length = 0;
                int ret = rv->GetString(i, &ch, &length);
                if (ret == 0) {
                    col.assign(ch, length);
                }
            }
            result.insert(std::make_pair(col_name, col));
        }
        return result;
    }
    void SetRV(google::protobuf::RepeatedPtrField<rtidb::common::ColumnDesc>& schema) {
        columns.clear();
        for (auto col : schema) {
            columns.push_back(col);
        }
        rv = std::make_shared<rtidb::base::RowView>(schema);
    }

    RowViewResult():code(0), msg(), index(0) {
    }
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
    RowViewResult Query(const std::string& name, struct ReadOption& ro);
    GeneralResult Put(const std::string& name, const std::map<std::string, std::string>& values, const WriteOption& wo);
    GeneralResult Delete(const std::string& name, const std::map<std::string, std::string>& values);
    GeneralResult Update(const std::string& name, const std::map<std::string, std::string>& condition, const std::map<std::string, std::string> values, const WriteOption& wo);
    RowViewResult GetRowView(const std::string& name);

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
    std::string zk_table_notify_path_;
    std::map<std::string, std::shared_ptr<TableHandler>> tables_;
};
