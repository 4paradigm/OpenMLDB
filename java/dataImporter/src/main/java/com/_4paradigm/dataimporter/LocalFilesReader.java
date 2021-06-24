package com._4paradigm.dataimporter;

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

// TODO(hw): only csv now
public class LocalFilesReader {
    private static final Logger logger = LoggerFactory.getLogger(LocalFilesReader.class);

    private final List<String> files;
    private int nextFileIdx = 0;
    private Iterator<CSVRecord> iter = null;

    public LocalFilesReader(List<String> files) throws IOException {
        this.files = files;
    }

    private boolean updateParser() throws IOException {
        while (nextFileIdx < files.size()) {
            // TODO(hw): what about no header?
            String filePath = files.get(nextFileIdx).trim();
            logger.info("read next file {}", filePath);
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
