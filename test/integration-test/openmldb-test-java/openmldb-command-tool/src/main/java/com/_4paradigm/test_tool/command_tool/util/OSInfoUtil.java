package com._4paradigm.test_tool.command_tool.util;

public class OSInfoUtil {
    private static String OS = System.getProperty("os.name").toLowerCase();

    public static boolean isLinux(){
        return OS.indexOf("linux")>=0;
    }

    public static boolean isMacOS(){
        return OS.indexOf("mac")>=0&&OS.indexOf("os")>0&&OS.indexOf("x")<0;
    }

    public static boolean isMacOSX(){
        return OS.indexOf("mac")>=0&&OS.indexOf("os")>0&&OS.indexOf("x")>0;
    }

    public static boolean isMac(){
        return OS.indexOf("mac os")>=0;
    }
}
