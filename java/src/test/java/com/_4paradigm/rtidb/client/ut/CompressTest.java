package com._4paradigm.rtidb.client.ut;

import com._4paradigm.rtidb.utils.Compress;
import org.testng.Assert;
import org.testng.annotations.Test;

public class CompressTest {
    @Test
    public void TestGzipCompress() {
        String str = "testabcdeft1112222";
        byte[] data = str.getBytes();
        byte[] compressed = Compress.gzip(data);
        byte[] uncompressed = Compress.gunzip(compressed);
        Assert.assertEquals(data.length, uncompressed.length);
        Assert.assertTrue(java.util.Arrays.equals(data, uncompressed));
    }

    @Test
    public void TestSnappyCompress() {
        // String str = "testabcdeft1112222";
        String str = "测试压缩数据北京市海淀区上地东路35号";
        byte[] data = str.getBytes();
        byte[] compressed = Compress.snappyCompress(data);
        byte[] uncompressed = Compress.snappyUnCompress(compressed);
        Assert.assertEquals(data.length, uncompressed.length);
        Assert.assertTrue(java.util.Arrays.equals(data, uncompressed));
    }
}
