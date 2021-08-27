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

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Iterator;

public class HDFSCSVFileReader implements CSVFileReader {

    private final Iterator<CSVRecord> iter;

    public HDFSCSVFileReader(String filePath) throws IOException {
        Configuration conf = new Configuration();
        URI uri = URI.create(filePath);
        FileSystem fs = FileSystem.get(uri, conf);
        FSDataInputStream stream = fs.open(new Path(uri));
        CSVFormat format = CSVFormat.Builder.create().setHeader().build();
        iter = CSVParser.parse(stream, StandardCharsets.UTF_8, format).iterator();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public CSVRecord next() {
        return iter.next();
    }
}
