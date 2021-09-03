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

import com._4paradigm.openmldb.proto.Common;
import com._4paradigm.openmldb.proto.NS;
import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Set;

public class FilesReader {
    private static final Logger logger = LoggerFactory.getLogger(FilesReader.class);

    private final List<String> files;
    private int nextFileIdx = 0;
    private CSVFileReader curReader = null;
    private final Set<String> expectedColSet = Sets.newHashSet();

    public FilesReader(List<String> files) {
        this.files = files;
    }

    private boolean updateParser() throws IOException {
        while (nextFileIdx < files.size()) {
            // TODO(hw): what about no header?
            String filePath = files.get(nextFileIdx).trim();
            logger.info("read next file {}", filePath);
            if (isHDFSFile(filePath)) {
                curReader = new HDFSCSVFileReader(filePath);
            } else {
                curReader = new LocalCSVFileReader(filePath);
            }

            // only check col name set
            Preconditions.checkState(expectedColSet.isEmpty()
                            || Sets.difference(curReader.getHeader().keySet(), expectedColSet).isEmpty(),
                    "file's header " + curReader.getHeader().keySet() + " != expected header " + expectedColSet);

            nextFileIdx++;
            if (curReader.hasNext()) {
                return true;
            }
            // may get empty file, continue
        }
        return false;
    }

    private boolean isHDFSFile(String filePath) {
        return filePath.startsWith("hdfs://");
    }

    public CSVRecord next() throws IOException {
        if ((curReader == null || !curReader.hasNext()) && !updateParser()) {
            return null;
        }
        Preconditions.checkState(curReader.hasNext());
        return curReader.next();
    }

    public void enableCheckHeader(NS.TableInfo tableMetaData) {
        Preconditions.checkState(expectedColSet.isEmpty(), "do not reset the expected header map");
        for (int i = 0; i < tableMetaData.getColumnDescCount(); i++) {
            Common.ColumnDesc col = tableMetaData.getColumnDesc(i);
            expectedColSet.add(col.getName());
        }
    }
}
