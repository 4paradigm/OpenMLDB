package com._4paradigm.openmldb.synctool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.runtime.client.JobStatusMessage;
import org.apache.flink.runtime.executiongraph.AccessExecutionGraph;

import  org.junit.Assert;
import junit.framework.TestCase;

public class FlinkTunnelTest extends TestCase {
    public void testFlinkTunnel() throws InterruptedException, ExecutionException {
        FlinkTunnel tunnel = FlinkTunnel.getInstance();
        Assert.assertTrue(tunnel.getMiniCluster().isRunning());
        Thread.sleep(5000);
        // history jobs
        Collection<JobStatusMessage> jobs = tunnel.getMiniCluster().listJobs().get();
        for (JobStatusMessage job : jobs) {
            AccessExecutionGraph graph = tunnel.getMiniCluster().getExecutionGraph(job.getJobId()).get();
            System.out.println("job id: " + graph.getJobID() + " status: " + graph.getState());
            graph.getAllVertices().forEach((k, v) -> {
                System.out.println("vertex task: " + v.getTaskVertices());
            });
        }
        tunnel.createTunnel(6, "/tmp/data_cache/6", "/tmp/flink-sink-test/6");
        // tunnel.createTunnel(7);
        tunnel.closeTunnel(6);
        // System.out.println("hw test" + tunnel.getJobStatus(6) + " " + tunnel.getJobClient(6).getJobID());
        Thread.sleep(2000000);
        tunnel.close();
    }

    public void testCacheDir() throws IOException {
        String cacheDir = "file:///tmp/cache";
        // java.nio.file.Path p = Paths.get(cacheDir);
        // System.out.println("hw test " + p.toAbsolutePath());
        // Assert.assertFalse(Files.exists(p));
        
        // try {
        //     Files.createDirectories(Paths.get(cacheDir));
        // } catch (IOException e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }
        // Assert.assertTrue(Files.exists(Paths.get(cacheDir)));
        // flink path
        Path pp = new Path(cacheDir);
        System.out.println("hw test " + pp.toUri());
        FileSystem fileSystem = FileSystem.get(pp.toUri());
        Assert.assertTrue(fileSystem.exists(pp));
    }
}
