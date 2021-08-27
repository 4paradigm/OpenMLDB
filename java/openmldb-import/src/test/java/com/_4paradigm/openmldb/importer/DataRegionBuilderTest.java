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
import junit.framework.TestCase;
import org.apache.commons.lang3.StringUtils;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collections;

public class DataRegionBuilderTest extends TestCase {
    private static final Logger logger = LoggerFactory.getLogger(DataRegionBuilderTest.class);

    public void testMessageSize() {
        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(Integer.MAX_VALUE).setPid(Integer.MAX_VALUE);
        builder.setPartId(Integer.MAX_VALUE);
        Assert.assertTrue(BulkLoadRequestSize.reqReservedSize >= builder.build().getSerializedSize());

        Tablet.DataBlockInfo dataBlockInfo = Tablet.DataBlockInfo.newBuilder().setRefCnt(Integer.MAX_VALUE).
                setOffset(Long.MAX_VALUE).setLength(Integer.MAX_VALUE).build();
        Assert.assertTrue(BulkLoadRequestSize.estimateDataBlockInfoSize >= dataBlockInfo.getSerializedSize());

        String sampleKey = StringUtils.repeat('1', 1000);
        Tablet.Dimension dim = Tablet.Dimension.newBuilder().setKey(sampleKey).setIdx(10).build();
        Tablet.TSDimension tsDim = Tablet.TSDimension.newBuilder().setTs(System.currentTimeMillis()).setIdx(10).build();
        Tablet.BinlogInfo binlogInfo = Tablet.BinlogInfo.newBuilder().addAllDimensions(Collections.nCopies(10, dim))
                .addAllTsDimensions(Collections.nCopies(100, tsDim)).setTime(System.currentTimeMillis())
                .setBlockId(Integer.MAX_VALUE).build();
        int binlogInfoSize = binlogInfo.getSerializedSize();

        for (int i = 0; i < 100; i++) {
            builder.addBlockInfo(dataBlockInfo);
            builder.addBinlogInfo(binlogInfo);
            int estimateSize = BulkLoadRequestSize.reqReservedSize
                    + (i + 1) * (BulkLoadRequestSize.estimateDataBlockInfoSize + binlogInfoSize + BulkLoadRequestSize.tagAndFieldLengthTolerance);
            int realSize = builder.build().getSerializedSize();
            Assert.assertTrue("The " + i + " time: estimate size " + estimateSize + " must >= real size " + realSize,
                    estimateSize >= realSize);
        }
    }

    public void testNormalBuild() throws IOException {
        // small size limit for testing, so we just set it to 64B.
        DataRegionBuilder dataRegionBuilder = new DataRegionBuilder(1, 1, 12000);
        ByteBuffer bufferSample = ByteBuffer.allocate(10);

        String sampleKey = StringUtils.repeat('1', 1000);
        Tablet.Dimension dim = Tablet.Dimension.newBuilder().setKey(sampleKey).setIdx(10).build();
        Tablet.TSDimension tsDim = Tablet.TSDimension.newBuilder().setTs(System.currentTimeMillis()).setIdx(10).build();

        for (int i = 0; i < 10; i++) {
            // one row add 11205B
            dataRegionBuilder.addDataBlock(bufferSample, 0);
            dataRegionBuilder.addBinlog(Collections.nCopies(10, dim), Collections.nCopies(100, tsDim), System.currentTimeMillis(), 0);

            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(false, stream);
            if (i == 0) {
                // dataRegionBuilder only has one row 11205 + some reserved size < 12000, won't build request.
                Assert.assertNull(request);
            } else {
                // there are two rows in builder, but we will get a request that has only one row cuz the size limit.
                // After sent, leave one row. So when i in [1,n), we'll get one-row request in one loop.
                Assert.assertNotNull(request);
                logger.info("request real size {}", request.getSerializedSize());
                Assert.assertEquals(1, request.getBlockInfoCount());
                Assert.assertEquals(bufferSample.limit(), stream.size());
            }
        }
        // remain 1 row
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(true, stream);
        logger.info("request real size {}", request.getSerializedSize());
        Assert.assertEquals(1, request.getBlockInfoCount());
        Assert.assertEquals(bufferSample.limit(), stream.size());
        // If we try to send after each addDataBlock, it shouldn't have more data after one force request building.
        Assert.assertNull(dataRegionBuilder.buildPartialRequest(true, stream));
    }

}