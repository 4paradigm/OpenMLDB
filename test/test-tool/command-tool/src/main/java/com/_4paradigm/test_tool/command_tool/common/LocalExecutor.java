package com._4paradigm.test_tool.command_tool.common;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

@Slf4j
public class LocalExecutor implements CommandExecutor {

    private List<String> starts = new ArrayList<>();

    public LocalExecutor(){
        starts.add("wget");
        starts.add("tar");
    }
    public boolean isUseExec(String command){
        for(String start:starts){
            if(command.startsWith(start)){
                return true;
            }
        }
        return false;
    }
    @Override
    public String execute(String command) {
        String result;
        if(isUseExec(command)){
            result = ExecUtil.exeCommand(command);
        }else{
            result = CommandUtil.run(command);
        }

//        Scanner input = null;
//        Process process = null;
//        try {
//            process = Runtime.getRuntime().exec(new String[]{"/bin/sh","-c",command});
//            try {
//                //等待命令执行完成
//                process.waitFor(600, TimeUnit.SECONDS);
//            } catch (InterruptedException e) {
//                e.printStackTrace();
//            }
//            InputStream is = process.getInputStream();
//            input = new Scanner(is);
//            while (input.hasNextLine()) {
//                String line = input.nextLine().trim();
//                if(line.contains("ZOO_INFO@log_env") || line.contains("src/zk/zk_client.cc")||
//                        line.startsWith("ns leader:")){
//                    continue;
//                }
//                if(line.length()==0) continue;
//                list.add(line);
//            }
//        }catch (Exception e){
//            e.printStackTrace();
//        }finally {
//            if (input != null) {
//                input.close();
//            }
//            if (process != null) {
//                process.destroy();
//            }
//        }
        return result;
    }

}
