package com._4paradigm.openmldb.memoryusagecompare;

enum OpenMLDBTableStatusField {
    TABLE_ID(1),
    TABLE_NAME(2),
    DATABASE_NAME(3),
    STORAGE_TYPE(4),
    ROWS(5),
    MEMORY_DATA_SiZE(6),
    DISK_DATA_SIZE(7),
    PARTITION(8),
    PARTITION_UNALIVE(9),
    REPLICA(10),
    OFFLINE_PATH(11),
    OFFLINE_FORMAT(12),
    OFFLINE_SYMBOLIC_PATHS(13),
    WARNINGS(14),
    ;

    private final int index;

    OpenMLDBTableStatusField(int index) {
        this.index = index;
    }

    public int getIndex() {
        return index;
    }
}