package com._4paradigm.test_tool.command_tool.common;

import lombok.extern.slf4j.Slf4j;

import java.io.InputStream;
import java.util.Scanner;
import java.util.concurrent.TimeUnit;

@Slf4j
public class CommandUtil {
    public static final int TIMEOUT = 600;
    public static String run(String command) {
        return run(new String[]{"/bin/sh","-c",command});
    }
    public static String run(String[] command) {
        Scanner input = null;
        String result = "";
        Process process = null;
        try {
            process = Runtime.getRuntime().exec(command);
            try {
                //等待命令执行完成
                process.waitFor(TIMEOUT, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            InputStream is = process.getInputStream();
            input = new Scanner(is);
            while (input.hasNextLine()) {
                result += input.nextLine() + "\n";
            }
//            result = command + "\n" + result; //加上命令本身，打印出来
        } catch (Exception e){
           e.printStackTrace();
        }finally {
            if (input != null) {
                input.close();
            }
            if (process != null) {
                process.destroy();
            }
        }
        return result;
    }

}
