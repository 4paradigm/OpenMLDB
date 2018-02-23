package com._4paradigm.rtidb.client;

import java.util.List;

import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;

public interface TabletClient {

    boolean createTable(String name, int tid, int pid, long ttl, int segCnt);

    boolean createTable(String name, int tid, int pid, long ttl, TTLType type, int segCnt);

    boolean createTable(String name, int tid, int pid, long ttl, int segCnt, List<ColumnDesc> schema);

    boolean createTable(String name, int tid, int pid, long ttl, TTLType type, int segCnt, List<ColumnDesc> schema);

    boolean dropTable(int tid, int pid);
}
