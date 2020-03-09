package com._4paradigm.rtidb.client.impl;

import com._4paradigm.rtidb.client.TabletException;
import com._4paradigm.rtidb.client.ha.TableHandler;
import com._4paradigm.rtidb.client.schema.ColumnDesc;
import com._4paradigm.rtidb.client.schema.RowView;
import com._4paradigm.rtidb.ns.NS;
import com._4paradigm.rtidb.utils.Compress;
import com.google.protobuf.ByteString;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class RelationalIterator {

    private ByteString bs;
    private int offset;
    private ByteBuffer bb;
    // no copy
    private ByteBuffer slice;
    private int length;
    private int totalSize;
    private List<ColumnDesc> schema;
    private int count;
    private NS.CompressType compressType = NS.CompressType.kNoCompress;
    private TableHandler th;
    private Set<String> colSet;

    public RelationalIterator() {
    }

    public RelationalIterator(ByteString bs, TableHandler th, Set<String> colSet) {
        this.bs = bs;
        this.bb = this.bs.asReadOnlyByteBuffer();
        this.offset = 0;
        this.totalSize = this.bs.size();
        next();
        this.schema = th.getSchema();
        this.th = th;
        this.colSet = colSet;
    }


    public List<ColumnDesc> getSchema() {
        if (th != null && th.getSchemaMap().size() > 0) {
            return th.getSchemaMap().get(th.getSchema().size() + th.getSchemaMap().size());
        }
        return schema;
    }

    public boolean valid() {
        if (offset <= totalSize && totalSize != 0) {
            return true;
        }
        return false;
    }

    // no copy
    private ByteBuffer getValue() {
        if (compressType == NS.CompressType.kSnappy) {
            byte[] data = new byte[slice.remaining()];
            slice.get(data);
            byte[] uncompressed = Compress.snappyUnCompress(data);
            return ByteBuffer.wrap(uncompressed);
        } else {
            return slice;
        }
    }

    public Map<String, Object> getDecodedValue() throws TabletException {
        if (schema == null) {
            throw new TabletException("get decoded value is not supported");
        }
        RowView rowView;
        if (compressType == NS.CompressType.kSnappy) {
            byte[] data = new byte[slice.remaining()];
            slice.get(data);
            byte[] uncompressed = Compress.snappyUnCompress(data);
            if (uncompressed == null) {
                throw new TabletException("snappy uncompress error");
            }
            rowView = new RowView(th.getSchema(), ByteBuffer.wrap(uncompressed),
                    ByteBuffer.wrap(uncompressed).array().length);
        } else {
            rowView = new RowView(th.getSchema(), slice, bs.size());
        }
        return getInternel(rowView);
    }

    public void next() {
//        if (offset + 4 > totalSize) {
//            offset += 4;
//            return;
//        }
//        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
//        slice.position(offset);
//        length = slice.getInt();
//        if (length <= 0) {
//            throw new RuntimeException("bad frame data");
//        }
//        offset += (4 + length);
//        slice.limit(offset);

        slice = this.bb.slice().asReadOnlyBuffer().order(ByteOrder.LITTLE_ENDIAN);
        slice.position(0);
        slice.limit(bs.size());
    }

    private Map<String, Object> getInternel(RowView rowView) throws TabletException {
        Map<String, Object> map = new HashMap<>();
        List<ColumnDesc> schema = th.getSchema();
        Set<String> colSet = this.colSet;
        for (int i = 0; i < schema.size(); i++) {
            ColumnDesc columnDesc = schema.get(i);
            if (colSet == null || colSet.isEmpty() || colSet.contains(columnDesc.getName())) {
                switch (columnDesc.getDataType()) {
                    case kBool:
                        Boolean bool = rowView.getBool(i);
                        map.put(columnDesc.getName(), bool);
                        break;
                    case kSmallInt:
                        Short st = rowView.getInt16(i);
                        map.put(columnDesc.getName(), st);
                        break;
                    case kInt:
                        Integer itg = rowView.getInt32(i);
                        map.put(columnDesc.getName(), itg);
                        break;
                    case kTimestamp:
                        Long ts = rowView.getTimestamp(i);
                        map.put(columnDesc.getName(), ts);
                        break;
                    case kBigInt:
                        Long lg = rowView.getInt64(i);
                        map.put(columnDesc.getName(), lg);
                        break;
                    case kFloat:
                        Float ft = rowView.getFloat(i);
                        map.put(columnDesc.getName(), ft);
                        break;
                    case kDouble:
                        Double db = rowView.getDouble(i);
                        map.put(columnDesc.getName(), db);
                        break;
                    case kVarchar:
                        String str = rowView.getString(i);
                        map.put(columnDesc.getName(), str);
                        break;
                    default:
                        throw new TabletException("unsupported data type");
                }
            }
        }
        return map;
    }

}
