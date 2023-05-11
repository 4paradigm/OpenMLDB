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
package com._4paradigm.openmldb.synctool;

import com._4paradigm.openmldb.proto.DataSync;

import com.google.common.base.Preconditions;
import com.google.protobuf.TextFormat;

import javax.annotation.concurrent.ThreadSafe;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

import javax.annotation.concurrent.GuardedBy;
import lombok.extern.slf4j.Slf4j;
import lombok.*;

// for every tid-pid sync task
// TODO backup in local filesystem, and we can recover from it
@ThreadSafe
@Slf4j
public class SyncTask {
    @GuardedBy("this")
    private DataSync.AddSyncTaskRequest progress;
    @GuardedBy("this")
    private Long count = 0L;

    @Getter
    private AtomicLong lastUpdateTime;

    public enum Status {
        INIT, RUNNING, SUCCESS, FAILED, REASSIGNING
    }

    @GuardedBy("this")
    private Status status;

    Object lock = new Object();
    @GuardedBy("lock")
    private String dataCollector;
    @Getter
    private final String progressPath;
    // @Getter private final String dbName;
    // @Getter private final String tableName;

    public SyncTask(DataSync.AddSyncTaskRequest request, String dataCollector) {
        this.progress = request;
        this.dataCollector = dataCollector;
        // the path to save progress in local filesystem
        // <tid>/<pid>.progress
        this.progressPath = SyncToolConfig.SYNC_TASK_PROGRESS_PATH + "/" + request.getTid() + "/"
                + request.getPid() + ".progress";
        // we give the new task more patience, check thread won't check INIT task
        this.status = Status.INIT;
        this.lastUpdateTime = new AtomicLong(System.currentTimeMillis());
    }

    // when you create a new sync task, you should call this method
    public void persist() throws IOException {
        Path path = Paths.get(progressPath);
        Preconditions.checkState(
                Files.notExists(path), "progress file already exists, why create again? " + progressPath);
        Files.createDirectories(path.getParent());
        Files.createFile(path);
        saveProgressInFile();
    }

    public static SyncTask recover(String filePath) throws IOException {
        // TODO how about dataCollector?
        DataSync.AddSyncTaskRequest bak = DataSync.AddSyncTaskRequest.parseDelimitedFrom(new FileInputStream(filePath));
        return new SyncTask(bak, "");
    }

    private void saveProgressInFile() throws IOException {
        FileOutputStream output = new FileOutputStream(progressPath);
        progress.writeDelimitedTo(output);
        output.close();
    }

    // TODO(hw): it's better to get a copy of progress
    public synchronized DataSync.AddSyncTaskRequest getProgress() {
        return progress;
    }

    public synchronized void preCheck(DataSync.SendDataRequest request) throws IgnorableException {
        Preconditions.checkState(status == Status.INIT || status == Status.RUNNING,
                "task is not init or running, ignore this request. st: " + status);
        Preconditions.checkArgument(
                request.getTid() == progress.getTid() && request.getPid() == progress.getPid(),
                "tid and pid not match");
        // token not match means we have a new task, it's normal when sync tool restart
        // a sync task
        if (!request.getToken().equals(progress.getToken())) {
            throw new IgnorableException("token not match, ignore this request");
        }
        Preconditions.checkArgument(request.getStartPoint().equals(progress.getSyncPoint()),
                "start point " + request.getStartPoint() + " != current point " + progress.getSyncPoint());
        if (status == Status.INIT) {
            log.info("task {} is init, received the first data, make it running",
                    TextFormat.shortDebugString(progress));
            status = Status.RUNNING;
        }
    }

    public synchronized void updateProgress(DataSync.SendDataRequest request)
            throws IgnorableException {
        preCheck(request);
        // even count == 0, point can be changed, so we use sync point to check if we
        // need to update
        DataSync.SyncPoint oldPoint = progress.getSyncPoint();
        if (request.getNextPoint() != progress.getSyncPoint()) {
            progress = progress.toBuilder().setSyncPoint(request.getNextPoint()).build();
            count += request.getCount();
            try {
                saveProgressInFile();
            } catch (IOException e) {
                // can't handle this exception
                // after preCheck, RUNNING->FAILED
                status = Status.FAILED;
                throw new RuntimeException(
                        "progress persist failed, but we already cache the data. mark this task as unalive, check it manually. failed progress "
                                + progress,
                        e);
            }
        }
        // if no data, we still need to update lastUpdateTime
        lastUpdateTime.set(System.currentTimeMillis());
        log.info("update progress {}: {} -> {}, update time {}", TextFormat.shortDebugString(progress),
                TextFormat.shortDebugString(oldPoint), TextFormat.shortDebugString(request.getNextPoint()),
                lastUpdateTime.get());
    }

    // message Task is only for show
    public synchronized DataSync.Task buildTaskPB() {
        return DataSync.Task.newBuilder()
                .setProgress(progress)
                .setDataCollector(getDataCollector())
                .setCount(count)
                .setStatus(status.name())
                .build();
    }

    public synchronized void setToken(String token) {
        progress = progress.toBuilder().setToken(token).build();
    }

    public String extraInfo() {
        // compact string, you can split it by ";" and get kv by "="
        // and you can know tid and pid in progressPath
        synchronized (lock) {
            return "lastUpdateTime=" + lastUpdateTime.get() + ";progressPath=" + progressPath;
        }
    }

    public String getDataCollector() {
        synchronized (lock) {
            return dataCollector;
        }
    }

    public void setDataCollector(String dataCollector) {
        synchronized (lock) {
            this.dataCollector = dataCollector;
        }
    }

    public synchronized void setTabletServer(String tabletServer) {
        progress = progress.toBuilder().setTabletEndpoint(tabletServer).build();
    }

    public synchronized Status getStatus() {
        return status;
    }

    public synchronized void setStatus(Status status) {
        this.status = status;
    }

    public synchronized void close() throws IOException {
        status = Status.SUCCESS;
        // rename the file to finished
        java.nio.file.Path newFile = Paths.get(progressPath + ".finished." + System.currentTimeMillis());
        Files.move(Paths.get(progressPath), newFile);
    }
}
