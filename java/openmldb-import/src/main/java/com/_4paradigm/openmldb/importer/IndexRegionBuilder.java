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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

import static com.google.common.collect.MoreCollectors.onlyElement;
import static com.google.protobuf.ByteString.copyFromUtf8;
import static java.lang.Math.max;

// IndexRegionBuilder builds requests of index region(split by size).
// Must build and send index requests after all data requests have been sent.
public class IndexRegionBuilder {
    private static final Logger logger = LoggerFactory.getLogger(IndexRegionBuilder.class);

    private final int tid;
    private final int pid;
    private final int rpcSizeLimit;
    private final List<List<SegmentIndexRegion>> segmentIndexMatrix; // TODO(hw): to map?

    int realIdxCursor = 0, segIdxCursor = 0;
    private int partId = 0; // start after data region part id

    public IndexRegionBuilder(int tid, int pid, Tablet.BulkLoadInfoResponse bulkLoadInfo, int rpcSizeLimit) {
        this.tid = tid;
        this.pid = pid;
        segmentIndexMatrix = new ArrayList<>();

        bulkLoadInfo.getInnerSegmentsList().forEach(
                innerSegments -> {
                    List<SegmentIndexRegion> segments = new ArrayList<>();
                    innerSegments.getSegmentList().forEach(
                            segmentInfo -> {
                                // ts_idx_map array to map, proto2 doesn't support map.
                                Map<Integer, Integer> tsIdxMap = segmentInfo.getTsIdxMapList().stream().collect(Collectors.toMap(
                                        Tablet.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getKey,
                                        Tablet.BulkLoadInfoResponse.InnerSegments.Segment.MapFieldEntry::getValue)); // can't tolerate dup key
                                int tsCnt = segmentInfo.getTsCnt();
                                segments.add(new SegmentIndexRegion(tsCnt, tsIdxMap));
                            }
                    );
                    this.segmentIndexMatrix.add(segments);
                }
        );
        this.rpcSizeLimit = rpcSizeLimit;
    }

    public void setStartPartId(int startPartId) {
        this.partId = startPartId;
    }

    // If null, no more index region rpc.
    public Tablet.BulkLoadRequest buildPartialRequest() {
        Preconditions.checkState(partId > 0, "Data Region has been sent, Index Region part id can't be 0.");
        Tablet.BulkLoadRequest.Builder requestBuilder = Tablet.BulkLoadRequest.newBuilder();
        return setRequest(requestBuilder) ? requestBuilder.build() : null;
    }

    private boolean setRequest(Tablet.BulkLoadRequest.Builder requestBuilder) {
        long start = System.currentTimeMillis();
        if (realIdxCursor == segmentIndexMatrix.size()) {
            return false;
        }
        requestBuilder.setTid(tid).setPid(pid).setPartId(partId);
        boolean addSegment = false;
        int realIdx = realIdxCursor, segIdx = segIdxCursor;
        // SegmentIndexRegion may be empty.
        for (; realIdx < segmentIndexMatrix.size(); realIdx++) {
            List<SegmentIndexRegion> segs = segmentIndexMatrix.get(realIdx);
            Tablet.BulkLoadIndex.Builder innerIndexBuilder = requestBuilder.addIndexRegionBuilder().setInnerIndexId(realIdx);
            for (; segIdx < segs.size(); segIdx++) {
                SegmentIndexRegion seg = segs.get(segIdx);
                if (seg.buildCompleted()) {
                    continue;
                }
                int used = requestBuilder.build().getSerializedSize();
                // seg is not completed now
                // increased size >= segMsg size, cuz it's a repeated field. So we set smaller limit.
                Tablet.Segment segMsg = seg.buildPartialSegment(segIdx, max(rpcSizeLimit - used - BulkLoadRequestSize.repeatedTolerance, 0));
                if (segMsg != null) {
                    innerIndexBuilder.addSegment(segMsg);
                    addSegment = true;
                }
                if (!seg.buildCompleted()) {
                    logger.debug("near limit, used {}, limit {}", requestBuilder.build().getSerializedSize(), rpcSizeLimit);
                    updateSegmentCursor(realIdx, segIdx);
                    partId++;
                    logger.info("build index rpc cost {} ms", System.currentTimeMillis() - start);
                    return addSegment;
                }
                Preconditions.checkState(!(segMsg == null && seg.buildCompleted()),
                        "completed false->true, but no index entry append");
            }
        }
        updateSegmentCursor(realIdx, segIdx);
        partId++;
        Preconditions.checkState(realIdxCursor == segmentIndexMatrix.size());
        requestBuilder.setEof(true);
        logger.info("build index rpc cost {} ms", System.currentTimeMillis() - start);
        return addSegment;
    }

    private void updateSegmentCursor(int i, int j) {
        realIdxCursor = i;
        segIdxCursor = j;
    }

    public SegmentIndexRegion getSegmentIndexRegion(int idx, int segIdx) {
        return segmentIndexMatrix.get(idx).get(segIdx);
    }

    public static class SegmentIndexRegion {
        // segment id can be set when building message
        final int tsCnt;
        final Map<Integer, Integer> tsIdxMap;
        public int ONE_IDX = 0;
        public int NO_IDX = 0;

        // String key reverse order, so it's !SliceComparator
        // each key will create a size=tsCnt list for simplify
        Map<String, List<Map<Long, Integer>>> keyEntries = new TreeMap<>((a, b) -> -a.compareTo(b));

        // used to build message
        private List<String> keyList = null;
        private int keyIdxCursor = 0, timeEntriesIdxCursor = 0, timeEntryIdxCursor = 0;

        public SegmentIndexRegion(int tsCnt, Map<Integer, Integer> tsIdxMap) {
            this.tsCnt = tsCnt;
            this.tsIdxMap = tsIdxMap; // possibly empty
        }

        private void Put(String key, int idxPos, Long time, Integer id) {
            List<Map<Long, Integer>> entryList = keyEntries.getOrDefault(key, new ArrayList<>());
            if (entryList.isEmpty()) {
                for (int i = 0; i < tsCnt; i++) {
                    // !TimeComparator, original TimeComparator is reverse ordered, so here use the default.
                    entryList.add(new TreeMap<>());
                }
                keyEntries.put(key, entryList);
            }
            entryList.get(idxPos).put(time, id);
        }

        // TODO(hw): return val is only for debug
        public boolean Put(String key, List<Tablet.TSDimension> tsDimensions, Integer dataBlockId) {
            if (tsDimensions.isEmpty()) {
                return false;
            }
            boolean put = false;
            if (tsCnt == 1) {
                if (tsDimensions.size() == 1) {
                    Put(key, this.NO_IDX, tsDimensions.get(0).getTs(), dataBlockId);
                    put = true;
                } else {
                    // tsCnt == 1 & has tsIdxMap, so tsIdxMap only has one element.
                    Preconditions.checkArgument(tsIdxMap.size() == 1);
                    Integer tsIdx = tsIdxMap.keySet().stream().collect(onlyElement());
                    Tablet.TSDimension needPutIdx = tsDimensions.stream().filter(tsDimension -> tsDimension.getIdx() == tsIdx).collect(onlyElement());
                    Put(key, this.ONE_IDX, needPutIdx.getTs(), dataBlockId);
                    put = true;
                }
            } else {
                // tsCnt != 1, KeyEntry array for one key
                for (Tablet.TSDimension tsDimension : tsDimensions) {
                    Integer pos = tsIdxMap.get(tsDimension.getIdx());
                    if (pos == null) {
                        continue;
                    }
                    Put(key, pos, tsDimension.getTs(), dataBlockId);
                    put = true;
                }
            }
            return put;
        }

        // won't build message which size == 0, only null or at least one index entry
        public Tablet.Segment buildPartialSegment(int segId, int sizeLimit) {
            Preconditions.checkArgument(sizeLimit >= 0, "sizeLimit should >=0, 0 will return a null segment");
            if (keyList == null) {
                // After the first buildPartialSegment() called, adding more key entries is useless.
                keyList = new ArrayList<>(keyEntries.keySet());
            }
            Tablet.Segment.Builder builder = Tablet.Segment.newBuilder();
            builder.setId(segId);
            int estimatedSize = BulkLoadRequestSize.segmentReservedSize;
            int i = keyIdxCursor, j = timeEntriesIdxCursor, k = timeEntryIdxCursor;
            logger.debug("start with {}-{}-{}", i, j, k);

            int entryCount = 0;
            for (; i < keyList.size(); i++) {
                String key = keyList.get(i);
                Tablet.Segment.KeyEntries.Builder keyEntriesBuilder = builder.addKeyEntriesBuilder()
                        .setKey(copyFromUtf8(key));
                // tolerant KeyEntry(repeated)
                estimatedSize += BulkLoadRequestSize.repeatedTolerance + key.length();

                List<Map<Long, Integer>> timeEntriesList = keyEntries.get(key);
                // TODO(hw): timeEntriesList.size() == tsCnt
                for (; j < timeEntriesList.size(); j++) {
                    Map<Long, Integer> timeEntries = timeEntriesList.get(j);
                    List<Long> timeList = new ArrayList<>(timeEntries.keySet());
                    if (timeList.isEmpty()) {
                        // will set KeyEntryId, so empty key entry could be skipped
                        continue;
                    }

                    Tablet.Segment.KeyEntries.KeyEntry.Builder keyEntryBuilder = keyEntriesBuilder.addKeyEntryBuilder().setKeyEntryId(j);
                    estimatedSize += BulkLoadRequestSize.repeatedTolerance + BulkLoadRequestSize.commonReservedSize;

                    for (; k < timeList.size(); k++) {
                        keyEntryBuilder.addTimeEntryBuilder().setTime(timeList.get(k)).
                                setBlockId(timeEntries.get(timeList.get(k)));
                        estimatedSize += BulkLoadRequestSize.estimateTimeEntrySize;
                        if (logger.isDebugEnabled()) {
                            logger.debug("add one time entry({}-{}-{}), real size {}, cur estimated size {}, limit {}"
                                    , i, j, k, builder.build().getSerializedSize(), estimatedSize, sizeLimit);
                        }

                        if (estimatedSize > sizeLimit) {
                            // revert this one
                            Preconditions.checkState(keyEntryBuilder.getTimeEntryCount() > 0);
                            logger.debug("revert this time entry");
                            keyEntryBuilder.removeTimeEntry(keyEntryBuilder.getTimeEntryCount() - 1);
                            if (keyEntryBuilder.getTimeEntryCount() == 0) {
                                logger.debug("revert the whole empty key entry");
                                keyEntriesBuilder.removeKeyEntry(keyEntriesBuilder.getKeyEntryCount() - 1);
                                if (keyEntriesBuilder.getKeyEntryCount() == 0) {
                                    logger.debug("revert the whole empty key entries");
                                    builder.removeKeyEntries(builder.getKeyEntriesCount() - 1);
                                }
                            }
                            Tablet.Segment req = builder.build();
                            int realSize = req.getSerializedSize();
                            logger.debug("segment req real size {}, entry count {}", realSize, entryCount);
                            Preconditions.checkState(realSize <= sizeLimit,
                                    "real size %d must <= size limit %d", realSize, sizeLimit);
                            // next partial building starts from k
                            // If limit is too small, cursors may not incr.
                            updateIdxCursors(i, j, k);
                            return entryCount == 0 ? null : req;
                        }
                        entryCount++;
                    }
                    k = 0;
                }
                j = 0;
            }
            Preconditions.checkState(i == keyList.size());

            updateIdxCursors(i, j, k);
            return entryCount == 0 ? null : builder.build();
        }

        private void updateIdxCursors(int i, int j, int k) {
            keyIdxCursor = i;
            timeEntriesIdxCursor = j;
            timeEntryIdxCursor = k;
        }

        public boolean buildCompleted() {
            return keyIdxCursor == keyEntries.size();
        }
    }

}
