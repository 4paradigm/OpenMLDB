package com._4paradigm.dataimporter;

public class BulkLoadRequestSize {
    // brpc max_body_size default is 64MB, too small limit is just nonsense
    public static final int minLimitSize = 32 * 1024 * 1024;

    // tid/pid/part_id is int and optional, max serialized size is 6B * 3
    // We set it to the max, to ensure that the real rpc size <= the estimated size
    public static final int reqReservedSize = 18;
    // Tablet.DataBlockInfo 2 int, 1 long, all optional. And DataBlockInfo is optional in BulkLoadRequest too.
    public static final int estimateInfoSize = 24;
    // When we add a repeated message(size = X), the size may grow more than X. It won't be too big, set it to 32
    public static final int repeatedTolerance = 32;
}
