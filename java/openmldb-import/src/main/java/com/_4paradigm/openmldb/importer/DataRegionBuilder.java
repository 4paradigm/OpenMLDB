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
import com.google.common.base.Preconditions;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

public class DataRegionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(DataRegionBuilder.class);

    private final int tid;
    private final int pid;
    private final int rpcSizeLimit;

    private final List<Tablet.DataBlockInfo> dataBlockInfoList = new ArrayList<>(); // TODO(hw): to queue?
    // binlog is one part of data region. May need refactor if binlog load is time-consuming.
    private final List<Tablet.BinlogInfo> binlogInfoList = new ArrayList<>();
    private final List<ByteBuffer> dataList = new ArrayList<>();
    private int absoluteDataLength = 0;
    private int absoluteNextId = 0;
    private int estimatedDataAndInfoSize = 0;

    private int partId = 0;

    // RPC sizeLimit won't be changed frequently, so it could be final.
    public DataRegionBuilder(int tid, int pid, int rpcSizeLimit) {
        this.tid = tid;
        this.pid = pid;
        this.rpcSizeLimit = rpcSizeLimit;
    }

    // used by IndexRegion
    public int nextId() {
        return absoluteNextId;
    }

    // After data region all sent, index region RPCs need start after the last part, loader(TabletServer) will check the order.
    public int getNextPartId() {
        return partId;
    }

    public void addDataBlock(ByteBuffer data, int refCnt) {
        int head = absoluteDataLength;
        dataList.add(data);
        int length = data.limit();
        Tablet.DataBlockInfo info = Tablet.DataBlockInfo.newBuilder().setRefCnt(refCnt).setOffset(head).setLength(length).build();
        dataBlockInfoList.add(info);
        absoluteDataLength += length;
        absoluteNextId++;
        // TODO(hw): message size + attachment size v.s. max_body_size?
        estimatedDataAndInfoSize += BulkLoadRequestSize.estimateDataBlockInfoSize + length;
    }

    public void addBinlog(List<Tablet.Dimension> dimensions, List<Tablet.TSDimension> tsDimensions, long time, int dataBlockId) {
        Tablet.BinlogInfo info = Tablet.BinlogInfo.newBuilder().addAllDimensions(dimensions).
                addAllTsDimensions(tsDimensions).setTime(time).setBlockId(dataBlockId).build();
        binlogInfoList.add(info);
        // BinlogInfo size is hard to estimate, we use the real serialized size of each binlog info.
        // When it's added in bulk load request, it will add some extra size(TagSize & fieldLength's tag size, cuz it's repeated).
        // Add a tolerance size of TagSize & fieldLength.
        estimatedDataAndInfoSize += info.getSerializedSize() + BulkLoadRequestSize.tagAndFieldLengthTolerance;
    }

    // not force: if data size > limit, send x(x<=limit), remain size-x. If size <= limit, return null, needs to force build.
    // force: only send x(x<=limit) too. When data size > limit, you may need to force build more than once until returns null.
    public Tablet.BulkLoadRequest buildPartialRequest(boolean force, ByteArrayOutputStream attachmentStream) throws IOException {
        // if current size < sizeLimit, no need to send.
        if (!force && BulkLoadRequestSize.reqReservedSize + estimatedDataAndInfoSize <= rpcSizeLimit) {
            return null;
        }
        Preconditions.checkState(binlogInfoList.size() == dataBlockInfoList.size());

        // To limit the size properly, request message + attachment will be <= rpcSizeLimit
        // We need to add data blocks one by one.
        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(tid).setPid(pid).setPartId(partId);

        int shouldBeSentEnd = 0; // is block info size
        int sentTotalSize = BulkLoadRequestSize.reqReservedSize;
        attachmentStream.reset();
        for (int i = 0; i < dataBlockInfoList.size(); i++) {
            Tablet.DataBlockInfo dataBlockInfo = dataBlockInfoList.get(i);
            Tablet.BinlogInfo binlogInfo = binlogInfoList.get(i);
            ByteBuffer data = dataList.get(i);

            // add = info size + binlog size + data size
            int add = BulkLoadRequestSize.estimateDataBlockInfoSize
                    + binlogInfo.getSerializedSize() + BulkLoadRequestSize.tagAndFieldLengthTolerance
                    + dataBlockInfo.getLength();
            if (sentTotalSize + add > rpcSizeLimit) {
                break;
            }

            builder.addBlockInfo(dataBlockInfo);
            builder.addBinlogInfo(binlogInfo);
            attachmentStream.write(data.array());

            sentTotalSize += add;
            shouldBeSentEnd++;
        }

        if (shouldBeSentEnd == 0) {
            Preconditions.checkState(!force || dataBlockInfoList.isEmpty(),
                    "remain data can't be sent when force is true, " +
                            "maybe too big, estimatedDataAndInfoSize = ", estimatedDataAndInfoSize);
            return null;
        }

        // clear should-send data [0, End)
        logger.debug("should send {} data blocks, remain {} blocks", shouldBeSentEnd, dataBlockInfoList.size() - shouldBeSentEnd);
        dataBlockInfoList.subList(0, shouldBeSentEnd).clear();
        binlogInfoList.subList(0, shouldBeSentEnd).clear();
        dataList.subList(0, shouldBeSentEnd).clear();
        estimatedDataAndInfoSize -= sentTotalSize;

        partId++;
        Preconditions.checkState(builder.getBlockInfoCount() == builder.getBinlogInfoCount());
        Tablet.BulkLoadRequest req = builder.build();
        int realSize = req.getSerializedSize() + attachmentStream.size();

        logger.debug("estimate data rpc size = {}, real size = {}", sentTotalSize, realSize);
        Preconditions.checkState(realSize <= rpcSizeLimit,
                "data partial req real size %d should <= size limit %d", realSize, rpcSizeLimit);

        return req;
    }

    public static int sizeDebug(Tablet.BulkLoadRequest request) {
        int size;
        size = 0;
        if (request.hasTid()) {
            size += com.google.protobuf.CodedOutputStream
                    .computeUInt32Size(1, request.getTid());
            logger.info("size {}", size);
        }
        if (request.hasPid()) {
            size += com.google.protobuf.CodedOutputStream
                    .computeUInt32Size(2, request.getPid());
            logger.info("size {}", size);
        }
        if (request.hasPartId()) {
            size += com.google.protobuf.CodedOutputStream
                    .computeInt32Size(3, request.getPartId());
            logger.info("size {}", size);
        }
        for (int i = 0; i < request.getBlockInfoCount(); i++) {
            size += com.google.protobuf.CodedOutputStream
                    .computeMessageSize(4, request.getBlockInfo(i));
            logger.info("size {}", size);
        }
        for (int i = 0; i < request.getBinlogInfoCount(); i++) {
            size += com.google.protobuf.CodedOutputStream
                    .computeMessageSize(5, request.getBinlogInfo(i));
            logger.info("size {}", size);
        }
        for (int i = 0; i < request.getIndexRegionCount(); i++) {
            size += com.google.protobuf.CodedOutputStream
                    .computeMessageSize(6, request.getIndexRegion(i));
            logger.info("size {}", size);
        }
        if (request.hasEof()) {
            size += com.google.protobuf.CodedOutputStream
                    .computeBoolSize(7, request.getEof());
        }
        logger.info("size {}", size);
        size += request.getUnknownFields().getSerializedSize();
        logger.info("size {}", size);
        return size;
    }
}
