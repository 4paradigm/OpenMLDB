package com._4paradigm.rtidbCmdUtil;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.ArrayList;

import com._4paradigm.rtidb.common.Common;

public class Common {
    public static String parseFromByteBuffer(ByteBuffer buffer){
        Charset charset = Charset.forName("utf-8");
        return charset.decode(buffer).toString();
    }
}
