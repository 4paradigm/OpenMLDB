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

package com._4paradigm.openmldb.taskmanager.server;

import com._4paradigm.openmldb.taskmanager.config.TaskManagerConfig;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVPrinter;
import org.apache.commons.csv.CSVRecord;
import org.apache.commons.csv.QuoteMode;
import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.Iterator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Should be thread-safe
 * We'll save job result in TaskManagerConfig.JOB_LOG_PATH/tmp_result, not
 * offline storage path,
 * cuz we just want result restored in local file system.
 */
@Slf4j
public class JobResultSaver {
    private static final Log log = LogFactory.getLog(JobResultSaver.class);

    // false: unused, true: using
    // 0: unused, 1: saving, 2: finished but still in use
    private List<Integer> idStatus;

    public JobResultSaver() {
        idStatus = Collections.synchronizedList(new ArrayList<>(Collections.nCopies(128, 0)));
    }

    /**
     * Generate unique id for job result, != spark job id
     * We generate id before submit the spark job, so don't worry about saveFile for
     * result id when result id status==0
     */
    public int genResultId() {
        int id;
        // atomic
        synchronized (idStatus) {
            id = idStatus.indexOf(0);
            if (id == -1) {
                throw new RuntimeException("too much running jobs to save job result, reject this spark job");
            }
            idStatus.set(id, 1);
        }
        return id;
    }

    public String genUniqueFileName() {
        String file;
        synchronized (this) {
            file = UUID.randomUUID().toString();
        }
        return String.format("%s.csv", file);
    }

    public boolean saveFile(int resultId, String jsonData) {
        // No need to wait, cuz id status must have been changed by genResultId before.
        // It's a check.
        if (log.isDebugEnabled()) {
            log.debug("save result " + resultId + ", data " + jsonData);
        }
        int status = idStatus.get(resultId);
        if (status != 1) {
            throw new RuntimeException(
                    String.format("why send to not running save job %d(status %d)", resultId, status));
        }
        if (jsonData.isEmpty()) {
            synchronized (idStatus) {
                idStatus.set(resultId, 2);
                idStatus.notifyAll();
            }
            log.info("saved all result of result " + resultId);
            return true;
        }
        // save to <log path>/tmp_result/<result_id>/<unique file name>
        String savePath = String.format("%s/tmp_result/%d", TaskManagerConfig.getJobLogPath(), resultId);
        synchronized (this) {
            File saveP = new File(savePath);
            if (!saveP.exists()) {
                boolean res = saveP.mkdirs();
                log.info("create save path " + savePath + ", status " + res);
            }
        }
        String fileFullPath = String.format("%s/%s", savePath, genUniqueFileName());
        File resultFile = new File(fileFullPath);
        try {
            if (!resultFile.createNewFile()) {
                throw new RuntimeException("job result file already exsits before creating, file name is not unique? "
                        + fileFullPath);
            }
        } catch (IOException e) {
            log.error("create file failed, path " + fileFullPath, e);
            return false;
        }

        try (FileWriter wr = new FileWriter(fileFullPath)) {
            wr.write(jsonData);
            wr.flush();
        } catch (IOException e) {
            // Write failed, we'll lost a part of result, but it's ok for show sync job
            // output. So we just log it, and response the http request.
            log.error("write result to file failed, path " + fileFullPath, e);
            return false;
        }
        return true;
    }

    // if exception, reset manually by http
    public String readResult(int resultId, long timeoutMs) throws InterruptedException, IOException {
        long timeoutExpiredMs = System.currentTimeMillis() + timeoutMs;
        // wait for idStatus[resultId] == 2
        synchronized (idStatus) {
            while (System.currentTimeMillis() < timeoutExpiredMs && idStatus.get(resultId) != 2) {
                idStatus.wait();
            }
        }
        if (idStatus.get(resultId) != 2) {
            log.warn("read result timeout, result saving may be still running, try read anyway, id " + resultId);
        }
        String output = "";
        // all finished, read csv from savePath
        String savePath = String.format("%s/tmp_result/%d", TaskManagerConfig.getJobLogPath(), resultId);
        File saveP = new File(savePath);
        // If saveP not exists, means no real result saved. But it may use a uncleaned
        // path, whether read result succeed or not, we should delete it.
        if (saveP.exists()) {
            output = printFilesTostr(savePath);
            FileUtils.forceDelete(saveP);
        } else {
            log.info("empty result for " + resultId + ", show empty string");
        }
        // reset id
        synchronized (idStatus) {
            idStatus.set(resultId, 0);
            idStatus.notifyAll();
        }
        return output;
    }

    public String printFilesTostr(String fileDir) {
        try (StringWriter stringWriter = new StringWriter();
                Stream<Path> paths = Files.walk(Paths.get(fileDir))) {
            List<String> csvFiles = paths.filter(Files::isRegularFile).map(f -> f.toString())
                    .filter(f -> f.endsWith(".csv"))
                    .collect(Collectors.toList());
            if (csvFiles.isEmpty()) {
                return "no valid result file, may use the uncleaned result path, clean it by me";
            }
            // print the header the first file
            printFile(csvFiles.get(0), stringWriter, true);
            for (int i = 1; i < csvFiles.size(); i++) {
                printFile(csvFiles.get(i), stringWriter, false);
            }
            return stringWriter.toString();
        } catch (Exception e) {
            log.warn("read result met exception when read " + fileDir + ", " + e.getMessage());
            e.printStackTrace();
            return "read met exception, check the taskmanager log";
        }
    }

    // CSVPrinter can't do pretty print
    private void printFile(String file, StringWriter stringWriter, boolean printHeader) {
        // QuoteMode.MINIMAL is more simillary to spark dataframe output? or None?
        CSVFormat.Builder baseFormat = CSVFormat.Builder.create(CSVFormat.DEFAULT).setEscape('\\')
        .setQuoteMode(QuoteMode.MINIMAL).setNullString("null");
        // DEFAULT header is null, skip print header by printer ctor(we don't want)
        CSVFormat printFormat = baseFormat.build();
        // to auto parse header
        CSVFormat parseFormat = baseFormat.setHeader().build();

        try (BufferedReader br = new BufferedReader(new FileReader(file));
                CSVParser parser = new CSVParser(br, parseFormat);
                CSVPrinter csvPrinter = new CSVPrinter(stringWriter, printFormat)) {
            // When create the CSVPrinter, we don't know the header, so it won't print header by header is null in format.
            // And we get header by parser, it'll parse header automatically by setHeader(), so we print it manually
            if (printHeader) {
                csvPrinter.printRecord(parser.getHeaderNames());
            }

            Iterator<CSVRecord> iter = parser.iterator();
            while (iter.hasNext()) {
                csvPrinter.printRecord(iter.next());
            }
        } catch (Exception e) {
            log.warn("error when print result file " + file + ", ignore it");
            e.printStackTrace();
        }
    }

    // To reset the idStatus and remove tmp result dir
    public void reset() throws IOException {
        synchronized (idStatus) {
            Collections.fill(idStatus, 0);
        }
        String tmpResultDir = String.format("%s/tmp_result", TaskManagerConfig.getJobLogPath());
        // delete anyway
        FileUtils.forceDelete(new File(tmpResultDir));
    }
}
