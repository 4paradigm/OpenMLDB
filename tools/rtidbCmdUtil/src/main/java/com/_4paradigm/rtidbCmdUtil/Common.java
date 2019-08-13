package com._4paradigm.rtidbCmdUtil;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;

public class Common {
    public static String parseFromByteBuffer(ByteBuffer buffer){
        Charset charset = Charset.forName("utf-8");
        return charset.decode(buffer).toString();
    }
}
