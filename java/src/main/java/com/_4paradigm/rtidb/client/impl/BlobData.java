package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.blobserver.OSS;
import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com.google.protobuf.ByteString;
import rtidb.blobserver.BlobServer;

import java.nio.ByteBuffer;

public class BlobData {
    private TableHandler th;
    private long key;

    public BlobData(TableHandler th, long key) {
        this.th = th;
        this.key = key;

    }

    public String getUrl() {
        StringBuilder sb = new StringBuilder("/v1/get/");
        sb.append(th.getTableInfo().getName());
        sb.append("/");
        String prefix = sb.toString();
        final String s;
        s = String.format("%s%s", prefix, this.key);
        return s;
    }

    public ByteBuffer getData() throws TabletException {
        BlobServer bs = th.getBlobServer();
        int tid = th.getTableInfo().getTid();

        if (bs == null) {
            throw new TabletException("can not found available blobserver with tid " + tid);
        }
        OSS.GetRequest.Builder builder = OSS.GetRequest.newBuilder();
        builder.setTid(tid);
        builder.setPid(0);
        builder.setKey(key);

        OSS.GetRequest request = builder.build();
        OSS.GetResponse response = bs.get(request);
        if (response != null && response.getCode() == 0) {
            ByteString data = response.getData();
            return (ByteBuffer) data.asReadOnlyByteBuffer().rewind();
        }
        throw new TabletException("get blob data failed " + key);
    }

    public long getKey() {
        return key;
    }
}
