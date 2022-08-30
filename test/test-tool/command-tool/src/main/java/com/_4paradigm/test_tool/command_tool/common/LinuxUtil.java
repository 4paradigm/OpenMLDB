package com._4paradigm.test_tool.command_tool.common;

import com._4paradigm.test_tool.command_tool.util.OSInfoUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.List;

@Slf4j
public class LinuxUtil {
    public static int port = 30000;
    public static boolean checkPortIsUsed(int port){
        if (OSInfoUtil.isMac()) {
            String command = "lsof -i:" + port;
            List<String> result = ExecutorUtil.run(command);
            return result.size()>0;
        }else {
            String command = "netstat -ntulp | grep " + port;
            List<String> result = ExecutorUtil.run(command);
            for (String line : result) {
                if (line.contains(port + "")) {
                    return true;
                }
            }
        }
        return false;
    }

    public static String getHome(){
        String command ="echo $HOME";
        List<String> result = ExecutorUtil.run(command);
        return result.get(0);
    }

    public static boolean checkPortIsUsed(int port,int time,int count){
        int num = 0;
        do{
            boolean flag = checkPortIsUsed(port);
            if(flag){
                return true;
            }else {
                num++;
                try {
                    Thread.sleep(time);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }while (num<count);
        return false;
    }

    public static boolean cp(String src,String dst,String removePath){
        if(StringUtils.isNotEmpty(removePath)) {
            String command = "rm -rf " + removePath;
            ExecutorUtil.run(command);
        }
        String command = "cp -rf "+src+" "+dst;
        List<String> result = ExecutorUtil.run(command);
        return result.size()==0;
    }
    public static boolean cp(String src,String dst){
        return cp(src,dst,null);
    }

    public static String hostnameI(){
        if(OSInfoUtil.isMac()){
            return "127.0.0.1";
        }else{
            String command = "hostname -i";  ///usr/sbin/
            List<String> result = ExecutorUtil.run(command);
            return result.get(0);
        }
    }

    public static String getLocalIP(){
        String command = "hostname -i";
        try {
            List<String> result = ExecutorUtil.run(command);
            String ip = result.get(0);
            log.info("IP:" + ip);
            return ip;
        }catch (Exception e){
            e.printStackTrace();
        }
        return null;
    }

    // public static String getLocalIP(){
    //     String command = "ifconfig";  ///usr/sbin/  hostname -i
    //     try {
    //         List<String> result = ExecutorUtil.run(command);
    //         if(result.get(0).contains("command not found")){
    //             command = "/usr/sbin/ifconfig";
    //             result = ExecutorUtil.run(command);
    //         }
    //         for(String line:result){
    //             if(line.contains("inet")){
    //                 String ip = line.trim().split("\\s+")[1];
    //                 if(ip.contains(":")){
    //                     ip = ip.split(":")[1];
    //                 }
    //                 log.info("IP:" + ip);
    //                 return ip;
    //             }
    //         }
    //
    //     }catch (Exception e){
    //         e.printStackTrace();
    //     }
    //     return null;
    // }
    public static int getNoUsedPort(){
        for(int i=port;i<=65535;i++){
            boolean used = checkPortIsUsed(i);
            if(!used) {
                port=i+1;
                return i;
            }
        }
        throw new RuntimeException("无可用端口");
    }

    public static void stopZK(String zkPath){
        String command = zkPath+"/bin/zkServer.sh stop";
        ExecutorUtil.run(command);
    }
    public static boolean fileIsExist(String filePath){
        String command = "ls -al "+filePath;
        List<String> result = ExecutorUtil.run(command);
        if(result.size()==0){
            return false;
        }
        return !result.get(0).contains("No such file or directory");
    }
    public static void rm(String path){
        String command = "rm -rf "+path;
        ExecutorUtil.run(command);
    }
    public static String uploadFile(File file,String path,String fileName){
        if(fileName==null){
            fileName = file.getName();
        }
        String command = "mkdir -p "+path;
        ExecutorUtil.run(command);
        String filePath = path+"/"+fileName;
        boolean isExist = fileIsExist(filePath);
        if(isExist){
            rm(filePath);
        }
        command = "touch "+filePath;
        ExecutorUtil.run(command);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(file.getAbsolutePath())));
            String line = null;
            while( (line = br.readLine())!=null){
                line = line.replaceAll("","");
                command = "echo '"+line+"' >> "+filePath;
                ExecutorUtil.run(command);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(br!=null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return filePath;
    }
    public static String uploadFile(InputStream in,String path,String fileName){
        String command = "mkdir -p "+path;
        ExecutorUtil.run(command);
        String filePath = path+"/"+fileName;
        boolean isExist = fileIsExist(filePath);
        if(isExist){
            rm(filePath);
        }
        command = "touch "+filePath;
        ExecutorUtil.run(command);
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(in));
            String line = null;
            while( (line = br.readLine())!=null){
                line = line.replaceAll("","");
                command = "echo '"+line+"' >> "+filePath;
                ExecutorUtil.run(command);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }finally {
            try {
                if(br!=null) {
                    br.close();
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return filePath;
    }
}