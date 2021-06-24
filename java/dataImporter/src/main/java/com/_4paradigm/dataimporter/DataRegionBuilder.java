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

package com._4paradigm.dataimporter;

import com._4paradigm.openmldb.api.Tablet;
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
    private final List<ByteBuffer> dataList = new ArrayList<>(); // TODO(hw): to queue?
    private int absoluteDataLength = 0;
    private int absoluteNextId = 0;
    private int estimatedTotalSize = 0;

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
        estimatedTotalSize += BulkLoadRequestSize.estimateInfoSize + length;
        logger.debug("after size {}(exclude header 6)", estimatedTotalSize);
    }

    // not force: if data size > limit, send x(x<=limit), remain size-x. If size <= limit, return null, needs to force build.
    // force: only send x(x<=limit) too. data size > limit is available, you may need to force build more than once until returns null.
    public Tablet.BulkLoadRequest buildPartialRequest(boolean force, ByteArrayOutputStream attachmentStream) throws IOException {
        // if current size < sizeLimit, no need to send.
        if (!force && BulkLoadRequestSize.reqReservedSize + estimatedTotalSize <= rpcSizeLimit) {
            return null;
        }
        // To limit the size properly, request message + attachment will be <= rpcSizeLimit
        // We need to add data blocks one by one.

        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(tid).setPid(pid).setPartId(partId);

        // TODO(hw): refactor this
        int shouldBeSentEnd = 0; // is block info size
        int sentTotalSize = BulkLoadRequestSize.reqReservedSize;
        attachmentStream.reset();
        // TODO(hw): builder can get serialized size in every loop? estimateInfoSize is too big. May use removeBlockInfo()
        for (int i = 0; i < dataBlockInfoList.size(); i++) {
            int add = dataBlockInfoList.get(i).getLength() + BulkLoadRequestSize.estimateInfoSize;
            if (sentTotalSize + add > rpcSizeLimit) {
                break;
            }
            builder.addBlockInfo(dataBlockInfoList.get(i));
            attachmentStream.write(dataList.get(i).array());
            sentTotalSize += add;
            shouldBeSentEnd++;
        }

        // clear sent data [0..End)
        logger.debug("sent {} data blocks, remain {} blocks", shouldBeSentEnd, dataBlockInfoList.size() - shouldBeSentEnd);
        dataBlockInfoList.subList(0, shouldBeSentEnd).clear();
        dataList.subList(0, shouldBeSentEnd).clear();
        estimatedTotalSize -= (BulkLoadRequestSize.estimateInfoSize * shouldBeSentEnd + attachmentStream.size());

        if (shouldBeSentEnd == 0) {
            Preconditions.checkState(estimatedTotalSize == 0 && dataList.isEmpty() && dataBlockInfoList.isEmpty(),
                    "remain data can't be sent, maybe too big, or builder has internal error");
            return null;
        }

        partId++;

        logger.debug("estimate data rpc size = {}, real size = {}",
                BulkLoadRequestSize.estimateInfoSize * shouldBeSentEnd + attachmentStream.size(),
                builder.build().getSerializedSize() + attachmentStream.size());
        Preconditions.checkState(builder.build().getSerializedSize() <= rpcSizeLimit);
        return builder.build();
    }
}
