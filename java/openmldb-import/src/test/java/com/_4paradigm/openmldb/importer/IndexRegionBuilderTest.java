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
import com.google.common.collect.ImmutableMap;
import junit.framework.TestCase;
import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.google.protobuf.ByteString.copyFromUtf8;

public class IndexRegionBuilderTest extends TestCase {
    private static final Logger logger = LoggerFactory.getLogger(IndexRegionBuilderTest.class);

    public void testRequestSize() {
        Tablet.BulkLoadRequest.Builder emptyReq = Tablet.BulkLoadRequest.newBuilder();
        emptyReq.setTid(Integer.MAX_VALUE).setPid(Integer.MAX_VALUE).setPartId(Integer.MAX_VALUE);
        Assert.assertTrue(BulkLoadRequestSize.reqReservedSize >= emptyReq.build().getSerializedSize());

        Tablet.BulkLoadInfoResponse.Builder preInfo = Tablet.BulkLoadInfoResponse.newBuilder();

        // InnerSegmentsList
        Tablet.BulkLoadInfoResponse.InnerSegments.Builder inner1 = preInfo.addInnerSegmentsBuilder();
        inner1.addSegmentBuilder().setTsCnt(1).addTsIdxMapBuilder().setKey(1).setValue(0);

        Tablet.BulkLoadInfoResponse.InnerSegments.Builder inner2 = preInfo.addInnerSegmentsBuilder();
        inner1.addSegmentBuilder().setTsCnt(1).addTsIdxMapBuilder().setKey(2).setValue(0);

        // size growth in buildPartialSegment
        Tablet.Segment.Builder builder = Tablet.Segment.newBuilder();
        builder.setId(Integer.MAX_VALUE);
        Assert.assertTrue(BulkLoadRequestSize.segmentReservedSize >= builder.build().getSerializedSize());
        char[] chars = new char[1000];
        Arrays.fill(chars, '1');
        String key = new String(chars);
        Tablet.Segment.KeyEntries.Builder keyEntriesBuilder = builder.addKeyEntriesBuilder()
                .setKey(copyFromUtf8(key));
        Assert.assertTrue(BulkLoadRequestSize.repeatedTolerance + key.length() > builder.build().getSerializedSize());

    }

    public void testSegmentKeyComparator() {
        IndexRegionBuilder.SegmentIndexRegion region = new IndexRegionBuilder.SegmentIndexRegion(1, null);
        Map<String, List<Map<Long, Integer>>> treeMap = region.keyEntries;
        // test tree map, should be in reverse order, s11 > s1 > S1
        List<String> keys = Arrays.asList("s11", "s1", "S1");
        keys.forEach(key -> treeMap.put(key, null));
        Assert.assertArrayEquals(keys.toArray(), treeMap.keySet().toArray());
        treeMap.clear();

        // inner tree map, TimeComparator is in desc order, so the reverse order is ascending order.
        List<Long> times = Arrays.asList(1111L, 2222L, 3333L);
        times.forEach(time -> region.Put("s1", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(time).build()), 0));
        Object[] timeArray = treeMap.get("s1").get(0).keySet().toArray();
        Assert.assertArrayEquals(times.toArray(), timeArray);
    }

    public void testSegmentMsgBuild() {
        int tsCnt = 8;
        ImmutableMap<Integer, Integer> tsIdxMap = ImmutableMap.of(11, 4, 22, 6);
        {
            logger.info("test empty segment");
            IndexRegionBuilder.SegmentIndexRegion segment = new IndexRegionBuilder.SegmentIndexRegion(tsCnt, tsIdxMap);
            // empty segment won't build segment message.
            Assert.assertNull(segment.buildPartialSegment(321, 0));
            Assert.assertNull(segment.buildPartialSegment(321, 1000));
            Assert.assertTrue(segment.buildCompleted());
            // shouldn't put entries after build.
        }
        {
            logger.info("test not empty segment");
            IndexRegionBuilder.SegmentIndexRegion segment = new IndexRegionBuilder.SegmentIndexRegion(tsCnt, tsIdxMap);
            segment.Put("k1", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(233).build()), 0);
            // find one entry, but limit = 0, revert it but can't update cursors, will throw an IllegalStateException.
            try {
                segment.buildPartialSegment(321, 0);
            } catch (IllegalStateException e) {
                logger.info(e.getMessage());
            }
        }
        {
            logger.info("test build partial segment");
            IndexRegionBuilder.SegmentIndexRegion segment = new IndexRegionBuilder.SegmentIndexRegion(tsCnt, tsIdxMap);
            segment.Put("k1", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(0).setIdx(11).build()), 0);
            segment.Put("k1", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(0).setIdx(22).build()), 1);
            // size limit 20 will put the first entry to segment msg. So building is not completed.
            // TODO(hw): fix, size is estimated now, will be bigger
            Tablet.Segment partSeg = segment.buildPartialSegment(1, 20);
            Assert.assertNotNull(partSeg);
            logger.info("build partial segment size {}", partSeg.getSerializedSize());
            Assert.assertFalse(segment.buildCompleted());
        }

        {
            logger.info("test build partial segments");
            IndexRegionBuilder.SegmentIndexRegion segment = new IndexRegionBuilder.SegmentIndexRegion(tsCnt, tsIdxMap);
            for (int i = 0; i < 100; i++) {
                segment.Put("k1", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(i).setIdx(11).build()), i);
                segment.Put("k2", Collections.singletonList(Tablet.TSDimension.newBuilder().setTs(i).setIdx(22).build()), i * 2 + 1);
            }

            while (!segment.buildCompleted()) {
                Tablet.Segment partSeg = segment.buildPartialSegment(1, 20);
                Assert.assertNotNull(partSeg);
                logger.info("build partial segment size {}", partSeg.getSerializedSize());
            }
        }
    }

    // TODO(hw): test build() time cost?
}