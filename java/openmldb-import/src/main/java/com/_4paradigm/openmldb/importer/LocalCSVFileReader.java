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

import java.io.FileReader;
import java.io.IOException;
import java.io.Reader;
import java.util.Iterator;
import java.util.Map;

public class LocalCSVFileReader implements CSVFileReader {

    private final Iterator<CSVRecord> iter;
    private final Map<String, Integer> headerMap;

    public LocalCSVFileReader(String filePath) throws IOException {
        Reader in = new FileReader(filePath);
        CSVFormat format = CSVFormat.Builder.create().setHeader().build();
        CSVParser parser = format.parse(in);
        iter = parser.iterator();
        headerMap = parser.getHeaderMap();
    }

    @Override
    public boolean hasNext() {
        return iter.hasNext();
    }

    @Override
    public CSVRecord next() {
        return iter.next();
    }

    @Override
    public Map<String, Integer> getHeader() {
        return headerMap;
    }
}
