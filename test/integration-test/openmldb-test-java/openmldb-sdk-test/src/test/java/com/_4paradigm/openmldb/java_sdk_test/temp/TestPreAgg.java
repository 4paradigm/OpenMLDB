package com._4paradigm.openmldb.java_sdk_test.temp;

import com._4paradigm.openmldb.test_common.util.BinaryUtil;
import org.testng.annotations.Test;

import java.math.BigInteger;

public class TestPreAgg {
    @Test
    public void test(){
        double d = 20.5;
        String s = Long.toBinaryString(Double.doubleToRawLongBits(d));
        String ss = new String();
        System.out.println("ss = " + ss);
        double doubleVal = Double.longBitsToDouble(new BigInteger(s, 2).longValue());
        System.out.println(doubleVal);
    }
    @Test
    public void test1(){
        long l = 1590738990000L;
        String s = Long.toBinaryString(l);
        System.out.println("s = " + s);
        String s1 = Integer.toString(222, 2);
        System.out.println("s1 = " + s1);
    }
    @Test
    public void test2(){
        String s = "ff!";
        String s1 = BinaryUtil.binaryStrToBinaryStr16(BinaryUtil.strToBinaryStr(s));
        System.out.println("s1 = " + s1);
        System.out.println(BinaryUtil.strToBinaryStr(s));
    }
}
