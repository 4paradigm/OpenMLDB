/*
 * Copyright 2022 paxos.cn.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package cn.paxos.mysql.util;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class SHAUtils {

    public static final String SHA_1 = "SHA-1";
    public static final String SHA_256 = "SHA-256";

    public static String SHA(final String strText, final String strType)  {
        byte byteBuffer[] = SHA(strText.getBytes(), strType);

        StringBuffer strHexString = new StringBuffer();
        for (int i = 0; i < byteBuffer.length; i++) {
            String hex = Integer.toHexString(0xff & byteBuffer[i]);
            if (hex.length() == 1) {
                strHexString.append('0');
            }
            strHexString.append(hex);
        }

        return strHexString.toString();
    }

    public static byte[] SHA(final byte[] strText, final String strType)  {
        MessageDigest messageDigest;
        try {
            messageDigest = MessageDigest.getInstance(strType);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Impossible", e);
        }
        messageDigest.update(strText);
        return messageDigest.digest();
    }
}
