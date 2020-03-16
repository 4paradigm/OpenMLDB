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

    public static List<Common.ColumnKey> filterColumnKey(List<Common.ColumnKey> columnKeyList) {
        List<Common.ColumnKey> result = new ArrayList<Common.ColumnKey>();
        for(Common.ColumnKey ck : columnKeyList) {
            if (ck.getFlag == 0) {
                result.add(ck);
            }
        }
        return result;
    }
}
