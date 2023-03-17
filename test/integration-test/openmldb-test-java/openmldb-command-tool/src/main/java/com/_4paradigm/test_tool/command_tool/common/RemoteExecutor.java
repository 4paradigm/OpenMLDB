package com._4paradigm.test_tool.command_tool.common;


import com._4paradigm.test_tool.command_tool.conf.CommandConfig;

public class RemoteExecutor implements CommandExecutor {
    private RemoteExecuteCommand rec;
    public RemoteExecutor(){
        String linuxIP = CommandConfig.REMOTE_IP;
        String usrName = CommandConfig.REMOTE_USER;
        String passwd  = CommandConfig.REMOTE_PASSWORD;
        String privateKeyPath = CommandConfig.REMOTE_PRIVATE_KEY_PATH;
        rec = new RemoteExecuteCommand(linuxIP, usrName,privateKeyPath, passwd);
    }
    @Override
    public String execute(String command) {
        String result = null;
        try {
            result = rec.execute(command);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return result;
    }
}
