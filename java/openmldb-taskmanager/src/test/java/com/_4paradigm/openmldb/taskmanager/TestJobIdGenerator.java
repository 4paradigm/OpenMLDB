package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.taskmanager.dao.JobIdGenerator;
import org.testng.Assert;
import org.testng.annotations.Ignore;
import org.testng.annotations.Test;

import java.util.concurrent.*;

public class TestJobIdGenerator {
    public CopyOnWriteArrayList<Integer> jobIdSet = new CopyOnWriteArrayList<>();
    public int num = 0;

    public class ChildThread implements Callable<Void> {
        public Void call() throws Exception {
            while (num <= 10) {
                int jobId = JobIdGenerator.getUniqueJobID();
                if (jobIdSet.contains(jobId)) {
                    throw new Exception("JobId is not unique!");
                } else {
                    jobIdSet.add(jobId);
                    num++;
                }
            }
            return null;
        }
    }

    @Ignore
    @Test
    public void TestJobIdUnique() {
        ExecutorService executorService = Executors.newFixedThreadPool(8);
        Future<Void> future1 = executorService.submit(new ChildThread());
        Future<Void> future2 = executorService.submit(new ChildThread());
        Future<Void> future3 = executorService.submit(new ChildThread());
        try {
            future1.get();
            future2.get();
            future3.get();
        } catch (Exception e) {
            e.printStackTrace();
            Assert.fail();
        } finally {
            executorService.shutdown();
        }
    }
}
