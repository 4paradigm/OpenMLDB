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
