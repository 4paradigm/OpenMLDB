/*
 * Copyright 2021 4Paradigm
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com._4paradigm.openmldb.importer;

import com._4paradigm.openmldb.proto.Tablet;
import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.NS;
import com._4paradigm.openmldb.proto.Type;
import com._4paradigm.openmldb.common.codec.RowBuilder;
import com.baidu.brpc.RpcContext;
import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class BulkLoadGenerator implements Runnable {
    private static final Logger logger = LoggerFactory.getLogger(BulkLoadGenerator.class);

    public static class FeedItem {
        public final Map<Integer, List<Tablet.Dimension>> dims;
        public final List<Long> tsDims; // TODO(hw): how to use uint64? BigInteger is low-effective
        public final Map<String, String> valueMap;

        public FeedItem(Map<Integer, List<Tablet.Dimension>> dims, Set<Integer> tsSet, CSVRecord record) {
            this.dims = dims;
            this.valueMap = record.toMap();
            // TODO(hw): can't support no header csv now
            Preconditions.checkNotNull(valueMap);
            // TODO(hw): build tsDims here!! copy then improve
            tsDims = new ArrayList<>();
            for (Integer tsPos : tsSet) {
                // only kTimeStamp type
                tsDims.add((Long) buildTypedValues(record.get(tsPos), Type.DataType.kTimestamp));
            }
        }
    }

    private final int tid;
    private final int pid;
    private final BlockingQueue<FeedItem> queue;
    private final long pollTimeout;
    private final AtomicBoolean shutdown = new AtomicBoolean(false);
    private final AtomicBoolean internalErrorOcc = new AtomicBoolean(false);
    private final NS.TableInfo tableInfo;
    private final Tablet.BulkLoadInfoResponse indexInfoFromTablet; // TODO(hw): not a good name
    private final DataRegionBuilder dataRegionBuilder;
    private final IndexRegionBuilder indexRegionBuilder;
    private final TabletService service;

    private int statistics = 0;

    public BulkLoadGenerator(int tid, int pid, NS.TableInfo tableInfo, Tablet.BulkLoadInfoResponse indexInfo, TabletService service, int rpcSizeLimit) {
        this.tid = tid;
        this.pid = pid;
        this.queue = new ArrayBlockingQueue<>(1000);
        this.pollTimeout = 100;
        this.tableInfo = tableInfo;
        this.indexInfoFromTablet = indexInfo;
        this.dataRegionBuilder = new DataRegionBuilder(tid, pid, rpcSizeLimit);
        // TODO(hw): size limit improve
        this.indexRegionBuilder = new IndexRegionBuilder(tid, pid, indexInfoFromTablet, rpcSizeLimit); // built from BulkLoadInfoResponse
        this.service = service;
    }

    @Override
    public void run() {
        logger.info("Thread {} for MemTable(tid-pid {}-{})", Thread.currentThread().getId(), tid, pid);

        try {
            // exit statement: shutdown and no element in queue, or internal exit
            long startTime = System.currentTimeMillis();
            long realGenTime = 0;
            while (!shutdown.get() || !queue.isEmpty()) {
                FeedItem item = queue.poll(pollTimeout, TimeUnit.MILLISECONDS);
                if (item == null) {
                    // poll timeout, queue is still empty
                    continue;
                }
                long realStartTime = System.currentTimeMillis();

                // build data buffer
                ByteBuffer dataBuffer = buildData(item.valueMap);

                List<Tablet.Dimension> dimensions = item.dims.get(this.pid);

                // tsDimensions[idx] has 0 or 1 ts, we convert it to Tablet.TSDimensions for simplicity
                List<Tablet.TSDimension> tsDimensions = new ArrayList<>();
                for (int i = 0; i < item.tsDims.size(); i++) {
                    tsDimensions.add(Tablet.TSDimension.newBuilder().setIdx(i).setTs(item.tsDims.get(i)).build());
                }
                // If no ts, use current time
                long time = System.currentTimeMillis();

                Map<Integer, String> innerIndexKeyMap = new HashMap<>();
                for (Tablet.Dimension dim : dimensions) {
                    String key = dim.getKey();
                    long idx = dim.getIdx();
                    // TODO(hw): idx is uint32, but info size is int
                    Preconditions.checkElementIndex((int) idx, indexInfoFromTablet.getInnerIndexCount());
                    innerIndexKeyMap.put(indexInfoFromTablet.getInnerIndexPos((int) idx), key);
                }

                // we use ExecuteInsert logic, so won't call
                //  `table->Put(request->pk(), request->time(), request->value().c_str(), request->value().size());`
                // dimensions size must > 0
                Preconditions.checkState(!dimensions.isEmpty());

                // TODO(hw): CheckDimessionPut

                // set the dataBlockInfo's ref count when index region insertion
                AtomicInteger realRefCnt = new AtomicInteger();
                // 1. if tsDimensions is empty, we will put data into `ready Index` without checking.
                //      But we'll check the Index whether it has the ts column. Mismatch meta returns false.
                // 2. if tsDimensions is not empty, we will find the corresponding tsDimensions to put data. If it can't find, continue.
                innerIndexKeyMap.forEach((k, v) -> {
                    // TODO(hw): check idx valid
                    Tablet.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfoFromTablet.getInnerIndex(k);
                    for (Tablet.BulkLoadInfoResponse.InnerIndexSt.IndexDef indexDef : innerIndex.getIndexDefList()) {
                        //
                        if (tsDimensions.isEmpty() && indexDef.getTsIdx() != -1) {
                            throw new RuntimeException("IndexStatus has the ts column, but InsertRow doesn't have tsDimensions.");
                        }

                        if (!tsDimensions.isEmpty()) {
                            // just continue
                            if (indexDef.getTsIdx() == -1 || tsDimensions.stream().noneMatch(ts -> ts.getIdx() == indexDef.getTsIdx())) {
                                continue;
                            }
                            // TODO(hw): But there may be another question.
                            //  if we can't find here, but indexDef is ready, we may put in the next phase.
                            //  (foundTs is not corresponding to the put index, we can't ensure that?)
                        }

                        if (indexDef.getIsReady()) {
                            realRefCnt.incrementAndGet();
                        }
                    }
                });

                // if no tsDimensions, it's ok to warp the current time into tsDimensions.
                List<Tablet.TSDimension> tsDimsWrap = tsDimensions;
                if (tsDimensions.isEmpty()) {
                    tsDimsWrap = Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(time).build());
                }

                // Index Region insert, only use data block id
                int dataBlockId = dataRegionBuilder.nextId();
                for (Map.Entry<Integer, String> idx2key : innerIndexKeyMap.entrySet()) {
                    Integer innerIdx = idx2key.getKey();
                    String key = idx2key.getValue();
                    Tablet.BulkLoadInfoResponse.InnerIndexSt innerIndex = indexInfoFromTablet.getInnerIndex(innerIdx);
                    boolean needPut = innerIndex.getIndexDefList().stream().anyMatch(Tablet.BulkLoadInfoResponse.InnerIndexSt.IndexDef::getIsReady);
                    if (needPut) {
                        long segIdx = 0;
                        if (indexInfoFromTablet.getSegCnt() > 1) {
                            // hash get signed int, we treat is as unsigned
                            segIdx = Integer.toUnsignedLong(MurmurHash.hash32(key.getBytes(), key.length(), 0xe17a1465)) % indexInfoFromTablet.getSegCnt();
                        }
                        // segment[k][segIdx]->Put
                        IndexRegionBuilder.SegmentIndexRegion segment = indexRegionBuilder.getSegmentIndexRegion(innerIdx, (int) segIdx);
                        // void Segment::Put(const Slice& key, const TSDimensions& ts_dimension, DataBlock* row)
                        boolean put = segment.Put(key, tsDimsWrap, dataBlockId);
                        if (!put) {
                            // TODO(hw): for debug
                            logger.warn("segment.Put no put");
                        }
                    }
                }

                // If success, add block data & info
                dataRegionBuilder.addDataBlock(dataBuffer, realRefCnt.get());
                // add binlog too
                dataRegionBuilder.addBinlog(dimensions, tsDimensions, time, dataBlockId);

                // If reach limit, set part id, send data block infos & data, no index region.
                ByteArrayOutputStream attachmentStream = new ByteArrayOutputStream();
                Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(false, attachmentStream);
                if (request != null) {
                    sendRequest(request, attachmentStream);
                }

                long realEndTime = System.currentTimeMillis();
                realGenTime += (realEndTime - realStartTime);
            }
            // We try to send after add one block. So there is no more or only one request to build.
            ByteArrayOutputStream attachmentStream = new ByteArrayOutputStream();
            Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(true, attachmentStream);
            if (request != null) {
                logger.info("send data region part {}", request.getPartId());
                sendRequest(request, attachmentStream);
            }
            Preconditions.checkState(dataRegionBuilder.buildPartialRequest(true, attachmentStream)
                    == null, "shouldn't has more data to send");

            long generateTime = System.currentTimeMillis();
            logger.info("Thread {} for MemTable(tid-pid {}-{}), generate cost {} ms, real cost {} ms",
                    Thread.currentThread().getId(), tid, pid, generateTime - startTime, realGenTime);

            if (dataRegionBuilder.getNextPartId() == 0) {
                logger.info("no data sent, skip index region");
                return;
            }

            // IndexRegion may be big too. e.g. 40M index message for 1.4M rows
            // the last index rpc will set eof to true.
            indexRegionBuilder.setStartPartId(dataRegionBuilder.getNextPartId());
            Tablet.BulkLoadRequest req;
            while ((req = indexRegionBuilder.buildPartialRequest()) != null) {
                logger.info("send index region part {}, eof {}", req.getPartId(), req.getEof());
                if (logger.isDebugEnabled()) {
                    logger.debug("{}", req);
                }
                sendRequest(req, null);
            }
            long endTime = System.currentTimeMillis();
            logger.info("index region cost {} ms", endTime - generateTime);

            logger.info("total row count {}", statistics);
        } catch (Exception e) {
            logger.error("Thread {} for MemTable(tid-pid {}-{}) got err: {}. Exit...", Thread.currentThread().getId(), tid, pid, e.getMessage());
            internalErrorOcc.set(true);
            e.printStackTrace();
        }
    }

    private ByteBuffer buildData(Map<String, String> valueMap) throws Exception{
        List<Object> rowValues = new ArrayList<>();
        for (int j = 0; j < tableInfo.getColumnDescCount(); j++) {
            Common.ColumnDesc desc = tableInfo.getColumnDesc(j);
            String v = valueMap.get(desc.getName());
            Type.DataType type = desc.getDataType();
            Object obj = buildTypedValues(v, type);
            if (obj == null) {
                throw new RuntimeException("build value failed, raw " + v + ", col " + desc.getName() + ", type " + type.name());
            }
            rowValues.add(obj);
        }

        ByteBuffer dataBuffer;
        try{
            dataBuffer = RowBuilder.encode(rowValues.toArray(), tableInfo.getColumnDescList(), 1);
            Preconditions.checkState(tableInfo.getCompressType() == Type.CompressType.kNoCompress); // TODO(hw): support snappy later
        }catch (Exception e){
            System.out.println(e.getMessage());
            throw e;
        }
        return dataBuffer;
    }

    private void sendRequest(Tablet.BulkLoadRequest request, ByteArrayOutputStream attachmentStream) {
        byte[] attachment = null;
        if (attachmentStream != null) {
            attachment = attachmentStream.toByteArray();
        }
        RpcContext.getContext().setRequestBinaryAttachment(attachment);
        logger.info("send rpc, message size {}, attachment {}", request.getSerializedSize(),
                attachmentStream == null ? "empty" : attachmentStream.size());

        // com.baidu.brpc.exceptions.RpcException will be thrown up, generator run() will fail immediately
        Tablet.GeneralResponse response = service.bulkLoad(request);
        statistics += request.getBlockInfoCount();
        if (response.getCode() != 0) {
            throw new RuntimeException("bulk load rpc to " + request.getTid() + "-" + request.getPid()
                    + "(part " + request.getPartId() + ") failed" + ", " + response);
        }
    }

    public void feed(FeedItem item) throws InterruptedException {
        do {
            if (hasInternalError()) {
                throw new RuntimeException("generator has inter err");
            }
        } while (!queue.offer(item, 100, TimeUnit.MILLISECONDS));
        // offer() returns false if no space is available, waiting up to the specified wait time if necessary for space
        // to become available(and waiting up can avoid frequent switching).
        // If the generator works properly, we should keep retrying.
    }

    // Shutdown will wait for queue empty.
    public void shutdownGracefully() {
        this.shutdown.set(true);
    }

    public boolean hasInternalError() {
        return internalErrorOcc.get();
    }

    private static Object buildTypedValues(String v, Type.DataType type) {
        switch (type) {
            case kBool:
                return v.equals("true");
            case kSmallInt:
                return Short.parseShort(v);
            case kInt:
                return (Integer.parseInt(v));
            case kBigInt:
                return Long.parseLong(v);
            case kFloat:
                return Float.parseFloat(v);
            case kDouble:
                return Double.parseDouble(v);
            case kVarchar:
            case kString:
                return v;
            case kDate:
                return Date.valueOf(v);
            case kTimestamp:
                // TODO(hw): no need to support data time. Converting here is only for simplify. Should be deleted later.
                if (v.contains("-")) {
                    Timestamp ts = Timestamp.valueOf(v);
                    return ts.getTime(); // milliseconds
                }
                return Long.parseLong(v);
        }
        return null;
    }
}
