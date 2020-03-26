/*
 * Test.java
 * Copyright (C) 2020 wangtaize <wangtaize@m7-pce-dev01>
 *
 * Distributed under terms of the MIT license.
 */
package com._4paradigm.fesql;

public class Test
{
	public Test() {
	}

    final public static void main(String[] args) {
        String os = System.getProperty("os.name").toLowerCase();
        if (os.contains("mac")) {
            String path = Test.class.getResource("/libfesql_jsdk.jnilib").getPath();
            System.load(path);
        }else {
            String path = Test.class.getResource("/libfesql_jsdk.so").getPath();
            System.load(path);
        }
        Status status = new Status();
        DBMSSdk sdk = fesql.CreateDBMSSdk("127.0.0.1:9211");
        sdk.CreateDatabase("test", status);
    }
}

