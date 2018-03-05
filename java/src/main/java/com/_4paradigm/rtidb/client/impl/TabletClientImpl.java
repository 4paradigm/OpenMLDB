package com._4paradigm.rtidb.client.impl;

import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com._4paradigm.rtidb.client.TabletClient;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.impl.RTIDBSingleNodeClient;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.SchemaCodec;
import com._4paradigm.rtidb.tablet.Tablet;
import com._4paradigm.rtidb.tablet.Tablet.TTLType;
import com._4paradigm.rtidb.tablet.Tablet.TableStatus;
import com.google.protobuf.ByteString;

import rtidb.api.TabletServer;

public class TabletClientImpl implements TabletClient {
    private final static Logger logger = LoggerFactory.getLogger(TabletClientImpl.class);
    private final static int KEEP_LATEST_MAX_NUM = 1000;
    private RTIDBSingleNodeClient client;

    public TabletClientImpl(RTIDBSingleNodeClient client) {
        this.client = client;
    }

    @Override
    public boolean createTable(String name, int tid, int pid, long ttl, int segCnt) {
        return createTable(name, tid, pid, ttl, segCnt, null);
    }

    @Override
    public boolean createTable(String name, int tid, int pid, long ttl, TTLType type, int segCnt) {
        return createTable(name, tid, pid, ttl, type, segCnt, null);
    }

    @Override
    public boolean createTable(String name, int tid, int pid, long ttl, int segCnt, List<ColumnDesc> schema) {
        return createTable(name, tid, pid, ttl, null, segCnt, schema);
    }

    @Override
    public boolean createTable(String name, int tid, int pid, long ttl, TTLType type, int segCnt,
            List<ColumnDesc> schema) {
        if (ttl < 0) {
            return false;
        }
        if (null == name || "".equals(name.trim())) {
            return false;
        }
        if (type == TTLType.kLatestTime && (ttl > KEEP_LATEST_MAX_NUM || ttl < 0)) {
            return false;
        }
        Tablet.TableMeta.Builder builder = Tablet.TableMeta.newBuilder();
        Set<String> usedColumnName = new HashSet<String>();
        if (schema != null && schema.size() > 0) {
            for (ColumnDesc desc : schema) {
                if (null == desc.getName() || "".equals(desc.getName().trim())) {
                    return false;
                }
                if (usedColumnName.contains(desc.getName())) {
                    return false;
                }
                usedColumnName.add(desc.getName());
                if (desc.isAddTsIndex()) {
                    builder.addDimensions(desc.getName());
                }
            }
            try {
                ByteBuffer buffer = SchemaCodec.encode(schema);
                builder.setSchema(ByteString.copyFrom(buffer.array()));
            } catch (TabletException e) {
                logger.error("fail to decode schema");
                return false;
            }

        }
        if (type != null) {
            builder.setTtlType(type);
        }
        TabletServer tabletServer = client.getHandler(0).getHandler(0).getLeader();
        builder.setName(name).setTid(tid).setPid(pid).setTtl(ttl).setSegCnt(segCnt);
        Tablet.TableMeta meta = builder.build();
        Tablet.CreateTableRequest request = Tablet.CreateTableRequest.newBuilder().setTableMeta(meta).build();
        Tablet.CreateTableResponse response = tabletServer.createTable(request);
        if (response != null && response.getCode() == 0) {
            return true;
        }
        return false;
    }

    @Override
    public boolean dropTable(int tid, int pid) {
        Tablet.DropTableRequest.Builder builder = Tablet.DropTableRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(pid);
        Tablet.DropTableRequest request = builder.build();
        Tablet.DropTableResponse response = client.getHandler(0).getHandler(0).getLeader().dropTable(request);
        if (response != null && response.getCode() == 0) {
            logger.info("drop table tid {} pid {} ok", tid, pid);
            return true;
        }
        return false;
    }

    @Override
    public TableStatus getTableStatus(int tid, int pid) {
        Tablet.GetTableStatusRequest request = Tablet.GetTableStatusRequest.newBuilder().build();
        Tablet.GetTableStatusResponse response = client.getHandler(0).getHandler(0).getLeader().getTableStatus(request);
        for (TableStatus status : response.getAllTableStatusList()) {
            if (status.getTid() == tid && status.getPid() == pid) {
                return status;
            }
        }
        return null;
    }
    
    

}
