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

import com.google.common.base.Preconditions;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.List;

public class FilesReader {
    private static final Logger logger = LoggerFactory.getLogger(FilesReader.class);

    private final List<String> files;
    private int nextFileIdx = 0;
    private Iterator<CSVRecord> iter = null;

    public FilesReader(List<String> files) {
        this.files = files;
    }

    private boolean updateParser() throws IOException {
        while (nextFileIdx < files.size()) {
            // TODO(hw): what about no header?
            String filePath = files.get(nextFileIdx).trim();
            logger.info("read next file {}", filePath);
            // TODO(hw): parse header, local FileReader or hdfs FileReader
            Reader in = new FileReader(filePath);
            CSVFormat format = CSVFormat.Builder.create().setHeader().build();
            iter = format.parse(in).iterator();
            nextFileIdx++;
            if (iter.hasNext()) {
                return true;
            }
            // may get empty file, continue
        }
        return false;
    }

    public CSVRecord next() throws IOException {
        if ((iter == null || !iter.hasNext()) && !updateParser()) {
            return null;
        }
        Preconditions.checkState(iter.hasNext());
        return iter.next();
    }

}
