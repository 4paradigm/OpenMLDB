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

import com._4paradigm.openmldb.api.Tablet;
import junit.framework.TestCase;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class DataRegionBuilderTest extends TestCase {
    private static final Logger logger = LoggerFactory.getLogger(DataRegionBuilderTest.class);

    public void testRequestSize() {
        Tablet.BulkLoadRequest.Builder builder = Tablet.BulkLoadRequest.newBuilder();
        builder.setTid(Integer.MAX_VALUE).setPid(Integer.MAX_VALUE);
        builder.setPartId(Integer.MAX_VALUE);
        Assert.assertTrue(BulkLoadRequestSize.reqReservedSize >= builder.build().getSerializedSize());
        for (int i = 0; i < 10; i++) {
            Tablet.DataBlockInfo info = Tablet.DataBlockInfo.newBuilder().setRefCnt(Integer.MAX_VALUE).
                    setOffset(Long.MAX_VALUE).setLength(Integer.MAX_VALUE).build();
            Assert.assertTrue(BulkLoadRequestSize.estimateInfoSize >= info.getSerializedSize());
            builder.addBlockInfo(info);
            Assert.assertTrue(BulkLoadRequestSize.reqReservedSize + (i + 1) * BulkLoadRequestSize.estimateInfoSize >=
                    builder.build().getSerializedSize());
        }
    }

    public void testNormalSizeLimit() throws IOException {
        // small size limit for testing, so we just set it to 64B.
        DataRegionBuilder dataRegionBuilder = new DataRegionBuilder(1, 1, 64);
        ByteBuffer bufferSample = ByteBuffer.allocate(10);
        for (int i = 0; i < 10; i++) {
            dataRegionBuilder.addDataBlock(bufferSample, 0);
            ByteArrayOutputStream stream = new ByteArrayOutputStream();
            Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(false, stream);
            if (i == 0) {
                // dataRegionBuilder has one row 24(max info size+optional) + 10(data size), reqReservedSize(18B) + 34B = 52B < 64B, won't build request.
                Assert.assertNull(request);
            } else {
                // each row add 32B, 52 + 32 = 84 > 64, so we will get a request that has only one row 18B.
                // After sent, leave 84 - 32 = 52, so when i in [1,n), we'll get one row request in one loop.
                Assert.assertNotNull(request);
                Assert.assertTrue(BulkLoadRequestSize.reqReservedSize + BulkLoadRequestSize.estimateInfoSize >= request.getSerializedSize());
                Assert.assertEquals(bufferSample.limit(), stream.size());
            }
        }
        // remain 1 row
        ByteArrayOutputStream stream = new ByteArrayOutputStream();
        Tablet.BulkLoadRequest request = dataRegionBuilder.buildPartialRequest(true, stream);
        Assert.assertTrue(BulkLoadRequestSize.reqReservedSize + BulkLoadRequestSize.estimateInfoSize >= request.getSerializedSize());
        Assert.assertEquals(bufferSample.limit(), stream.size());
        // If we try to send after each addDataBlock, it shouldn't have more data after one force request building.
        Assert.assertNull(dataRegionBuilder.buildPartialRequest(true, stream));
    }

}