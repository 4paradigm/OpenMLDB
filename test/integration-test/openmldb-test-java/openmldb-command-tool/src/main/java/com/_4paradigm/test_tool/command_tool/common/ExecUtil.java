package com._4paradigm.test_tool.command_tool.common;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.exec.*;
import org.apache.commons.lang3.exception.ExceptionUtils;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;

@Slf4j
public class ExecUtil {
    private static final String DEFAULT_CHARSET = "UTF-8";
    private static final Long TIMEOUT = 1000*1000L;

    /**
     * 执行指定命令
     *
     * @param command 命令
     * @return 命令执行完成返回结果
     * @throws RuntimeException 失败时抛出异常，由调用者捕获处理
     */
    public synchronized static String exeCommand(String command) throws RuntimeException {
        try (ByteArrayOutputStream out = new ByteArrayOutputStream()) {
            int exitCode = exeCommand(command, out);
            if (exitCode == 0) {
                log.info("命令运行成功:" + command);
            } else {
                log.info("命令运行失败:" + command);
            }
            return out.toString(DEFAULT_CHARSET);
        } catch (Exception e) {
            log.info(ExceptionUtils.getStackTrace(e));
            throw new RuntimeException(ExceptionUtils.getStackTrace(e));
        }
    }
    public synchronized static String exeCommand(String[] commands) throws RuntimeException {
        String result = "";
        for(String command:commands){
            result+=exeCommand(command)+"\n";
        }
        return result;
    }
    public synchronized static int exeCommand(String[] commands, OutputStream out) throws Exception {
        int status = 0;
        for(String command:commands){
            status = exeCommand(command,out);
            if(status!=0){
                log.info("命令运行失败:" + command);
                continue;
            }else {
                log.info("命令运行成功:" + command);
            }
        }
        return status;
    }

        /**
         * 执行指定命令，输出结果到指定输出流中
         *
         * @param command 命令
         * @param out     执行结果输出流
         * @return 执行结果状态码：执行成功返回0
         * @throws ExecuteException 失败时抛出异常，由调用者捕获处理
         * @throws IOException      失败时抛出异常，由调用者捕获处理
         */
    public synchronized static int exeCommand(String command, OutputStream out) throws ExecuteException, IOException {
//        logger.info("command:"+command);
        CommandLine commandLine = CommandLine.parse(command);
        PumpStreamHandler pumpStreamHandler = null;
        if (null == out) {
            pumpStreamHandler = new PumpStreamHandler();
        } else {
            pumpStreamHandler = new PumpStreamHandler(out);
        }
        // 设置超时时间为10秒
        ExecuteWatchdog watchdog = new ExecuteWatchdog(TIMEOUT);
        DefaultExecutor executor = new DefaultExecutor();
        executor.setStreamHandler(pumpStreamHandler);
        executor.setWatchdog(watchdog);
        return executor.execute(commandLine);
    }

}
