enum DataType {
    kTypeBool = 0,
    kTypeInt16,
    kTypeInt32,
    kTypeInt64,
    kTypeFloat,
    kTypeDouble,
    kTypeString,
    kTypeDate,
    kTypeTimestamp,
    kTypeUnknow
};

class Schema {
 public:
    Schema() : empty() {}
    virtual ~Schema() {}
    virtual int32_t GetColumnCnt() const { return 0; }
    virtual const std::string& GetColumnName(uint32_t index) const {
        return empty;
    }
    virtual const DataType GetColumnType(uint32_t index) const {
        return kTypeUnknow;
    }
    virtual const bool IsColumnNotNull(uint32_t index) const { return false; }
    virtual const bool IsConstant(uint32_t index) const { return false; }

 private:
    std::string empty;
};
