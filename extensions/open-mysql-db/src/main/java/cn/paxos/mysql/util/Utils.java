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

import java.lang.management.ManagementFactory;
import java.lang.management.RuntimeMXBean;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.security.SecureRandom;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Base64;

public final class Utils {

    private static final ZoneOffset BEIJING_TIMEZONE = ZoneOffset.of("+8");

    public static LocalDateTime getLocalDateTimeNow() {
        return LocalDateTime.now(BEIJING_TIMEZONE);
    }

    public static byte[] generateRandomAsciiBytes(int numBytes) {
        byte[] s = new byte[numBytes];
        new SecureRandom().nextBytes(s);
        for (int i = 0; i < s.length; i++) {
            s[i] &= 0x7f;
            if (s[i] == 0 || s[i] == 36)
                s[i]++;
        }
        return s;
    }

    public static long getJVMUptime() {
        RuntimeMXBean bean = ManagementFactory.getRuntimeMXBean();
        return bean.getUptime() / 1000;
    }

    public static byte[] hexToBytes(String hex) {
        String s = hex.replace(" ", "");
        int len = s.length();
        if ((len & 1) != 0)
            throw new IllegalArgumentException("Odd number of characters.");

        byte[] data = new byte[len / 2];
        for (int i = 0; i < len; i += 2) {
            data[i / 2] = (byte) ((Character.digit(s.charAt(i), 16) << 4)
                    + Character.digit(s.charAt(i+1), 16));
        }
        return data;
    }

    public static boolean compareDigest(String a, String b) {
        if (a == null || b == null) return false;
        return MessageDigest.isEqual(a.getBytes(StandardCharsets.UTF_8), b.getBytes(StandardCharsets.UTF_8));
    }

    public static String scramble411(String passwordSha1Hex, byte[] seedAsBytes) {
        byte[] passwordSha1 = hexToBytes(passwordSha1Hex);

        MessageDigest md;
        try {
            md = MessageDigest.getInstance(SHAUtils.SHA_1);
        } catch (NoSuchAlgorithmException e) {
            throw new RuntimeException("Impossible", e);
        }

        byte[] passwordHashStage2 = md.digest(passwordSha1);
        md.reset();

        md.update(seedAsBytes);
        md.update(passwordHashStage2);

        byte[] toBeXord = md.digest();

        int numToXor = toBeXord.length;

        for (int i = 0; i < numToXor; i++) {
            toBeXord[i] = (byte) (toBeXord[i] ^ passwordSha1[i]);
        }

        return Base64.getEncoder().encodeToString(toBeXord);
    }
}