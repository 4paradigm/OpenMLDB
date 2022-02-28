package com._4paradigm.openmldb.batch.utils;

import java.nio.ByteBuffer;

public class ByteArrayUtil {

    private static final char[] HEX_ARRAY = "0123456789ABCDEF".toCharArray();

    public static String bytesToString(byte[] bytes) {
        char[] hexChars = new char[bytes.length * 2];
        for (int j = 0; j < bytes.length; j++) {
            int v = bytes[j] & 0xFF;
            hexChars[j * 2] = HEX_ARRAY[v >>> 4];
            hexChars[j * 2 + 1] = HEX_ARRAY[v & 0x0F];
        }
        return new String(hexChars);
    }

    public static byte[] intToByteArray(int value) {
        return ByteBuffer.allocate(4).putInt(value).array();
    }

    public static byte[] intToOneByteArray(int value) {
        return new byte[] { (byte)value };
    }

    public static void main(String[] args) {
        System.out.println("start main");

        int value = 2;
        byte[] bytes = intToOneByteArray(value);
        System.out.println(bytesToString(bytes));

        System.out.println("end of main");

    }

}
