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

// When building, we can use builder.build().getSerializedSize() to get the exact size.
// But it's time-consuming when we need to call it so many times.
// So we use bigger sizes to estimate the request size.
public class BulkLoadRequestSize {
    public static final int commonReservedSize = 10;

    // tid/pid/part_id/eof is int and optional, max serialized size is 6B * 4
    // We set it to the max, to ensure that the real rpc size <= the estimated size
    public static final int reqReservedSize = 24;

    // Tablet.DataBlockInfo 2 int, 1 long, all optional
    public static final int estimateDataBlockInfoSize = 24;

    public static int tagAndFieldLengthTolerance = 5 + 5;

    // When we add a repeated message(size = X), the size may grow more than X. It won't be too big, set it to 32
    public static final int repeatedTolerance = 32;

    public static final int segmentReservedSize = 10;
    public static final int estimateTimeEntrySize = 12;
}
