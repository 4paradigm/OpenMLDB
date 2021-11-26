package com._4paradigm.openmldb.taskmanager;

import com._4paradigm.openmldb.taskmanager.dao.JobIdGenerator;
import org.testng.Assert;
import org.testng.annotations.Test;

import java.util.HashSet;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestJobIdGenerator {
    public HashSet<Integer> jobIdSet = new HashSet<>();
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
