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

package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.taskmanager.dao.JobIdGenerator;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;
import java.util.concurrent.Callable;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestJobIdGenerator {
    public CopyOnWriteArrayList<Integer> jobIdSet = new CopyOnWriteArrayList<>();

    public class ChildThread implements Callable<Void> {
        int iterNum = 100000;
        public Void call() throws Exception {
            for (int i=0; i < iterNum; i++) {
                int jobId = JobIdGenerator.getUniqueId();
                if (jobIdSet.contains(jobId)) {
                    throw new Exception("JobId is not unique!");
                }
                jobIdSet.add(jobId);
            }
            return null;
        }
    }

    @Ignore
    @Test
    public void TestJobIdUnique() {
        ExecutorService executorService = Executors.newFixedThreadPool(4);
        Future<Void> future1 = executorService.submit(new ChildThread());
        Future<Void> future2 = executorService.submit(new ChildThread());
        Future<Void> future3 = executorService.submit(new ChildThread());
        Future<Void> future4 = executorService.submit(new ChildThread());
        try {
            future1.get();
            future2.get();
            future3.get();
            future4.get();
            Assert.assertTrue(true);
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executorService.shutdown();
        }
    }
}
