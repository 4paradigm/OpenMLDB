package com._4paradigm.test_tool.command_tool.common;

import ch.ethz.ssh2.Connection;
import ch.ethz.ssh2.Session;
import ch.ethz.ssh2.StreamGobbler;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;

@Slf4j
public class RemoteExecuteCommand {
    // 字符编码默认是utf-8
    private static String DEFAULTCHART = "UTF-8";
    private Connection conn;
    private String ip;
    private String userName;
    private String privateKeyPath;
    private String password;

    public RemoteExecuteCommand(String ip, String userName, String privateKeyPath, String password) {
        this.ip = ip;
        this.userName = userName;
        this.privateKeyPath = privateKeyPath;
        this.password = password;
    }

    public RemoteExecuteCommand() {

    }

    /**
     * 远程登录linux主机
     *
     * @return 登录成功返回true，否则返回false
     * @throws Exception
     * @since V0.1
     */
    public Boolean loginByUser() throws Exception {
        boolean flg = false;
        try {
            conn = new Connection(ip);
            // 连接
            conn.connect();
            // 认证
            flg = conn.authenticateWithPassword(userName, password);
        } catch (IOException e) {
            throw new Exception("远程连接服务器失败", e);
        }
        return flg;
    }
    /**
     * 远程登录linux主机
     *
     * @return 登录成功返回true，否则返回false
     * @throws Exception
     * @since V0.1
     */
    public Boolean loginByKey() throws Exception {
        boolean flg = false;
        try {
            conn = new Connection(ip);
            // 连接
            conn.connect();
            // 认证
            File privateKey = new File(privateKeyPath);
            flg =conn.authenticateWithPublicKey(userName,privateKey,password);
        } catch (IOException e) {
            throw new Exception("远程连接服务器失败", e);
        }
        return flg;
    }

    public Boolean login() throws Exception {
        if(privateKeyPath!=null&&privateKeyPath.length()>0){
            return loginByKey();
        }else {
            return loginByUser();
        }
    }

    /**
     * 远程执行shell脚本或者命令
     *
     * @param cmd 即将执行的命令
     * @return 命令执行完后返回的结果值
     * @throws Exception
     * @since V0.1
     */
    public String execute(String cmd) throws Exception {
//        log.info("command:"+cmd);
        String result = "";
        Session session = null;
        try {
            if (login()) {
                // 打开一个会话
                session = conn.openSession();
                // 执行命令
                session.execCommand(cmd);
                result = processStdout(session.getStdout(), DEFAULTCHART);
                // 如果为输出为空，说明脚本执行出错了
                if (StringUtils.isBlank(result)) {
                    result = processStdout(session.getStderr(), DEFAULTCHART);
                }
                conn.close();
                session.close();
            }
        } catch (IOException e) {
            throw new Exception("命令执行失败", e);
        } finally {
            if (conn != null) {
                conn.close();
            }
            if (session != null) {
                session.close();
            }
        }
//        log.info("remote exec result:\n"+result);
        return result;
    }
    public List<String> executeReturnList(String cmd){
        List<String> list = new ArrayList<>();
        try{
            String result = execute(cmd);
            String[] ss = result.split("\\n+");
            for(String s:ss){
                if(s.length()>0) {
                    list.add(s);
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }
        return list;
    }

    /**
     * 解析脚本执行返回的结果集
     *
     * @param in      输入流对象
     * @param charset 编码
     * @return 以纯文本的格式返回
     * @throws Exception
     * @since V0.1
     */
    private String processStdout(InputStream in, String charset) throws Exception {
        InputStream stdout = new StreamGobbler(in);
        StringBuffer buffer = new StringBuffer();
        InputStreamReader isr = null;
        BufferedReader br = null;
        try {
            isr = new InputStreamReader(stdout, charset);
            br = new BufferedReader(isr);
            String line = null;
            while ((line = br.readLine()) != null) {
                buffer.append(line + "\n");
            }
        } catch (UnsupportedEncodingException e) {
            throw new Exception("不支持的编码字符集异常", e);
        } catch (IOException e) {
            throw new Exception("读取指纹失败", e);
        } finally {
            br.close();
            isr.close();
            stdout.close();
        }
        return buffer.toString();
    }

    public static void main(String[] args) throws Exception {
       String linuxIP = "172.24.4.55";
       String usrName = "zhaowei01";
       String passwd  = "1qaz0p;/";
       String path = null;
//         String linuxIP = "172.27.231.16";
//         String usrName = "work";
//         String path = "src/main/resources/zw-mac-id_rsa";
//         String passwd = null;
        RemoteExecuteCommand rec = new RemoteExecuteCommand(linuxIP, usrName,path, passwd);
        List<String> result = rec.executeReturnList("ls");
        System.out.println(result);
//        System.out.println(rec.execute("netstat -anp | grep 2181"));
        // 执行命令
//        System.out.println(rec.execute("ifconfig"));
        // 执行脚本
//        rec.execute("sh /usr/local/tomcat/bin/statup.sh");
    }
}