package com._4paradigm.openmldb.test_common.util;

import org.apache.commons.lang3.StringUtils;

public class BinaryUtil {
    // 将Unicode字符串转换成bool型数组
    public static boolean[] StrToBool(String input) {
        boolean[] output = Binstr16ToBool(binaryStrToBinaryStr16(strToBinaryStr(input)));
        return output;
    }

    // 将bool型数组转换成Unicode字符串
    public static String BoolToStr(boolean[] input) {
        String output = binaryStrToStr(Binstr16ToBinstr(BoolToBinstr16(input)));
        return output;
    }

    // 将字符串转换成二进制字符串
    public static String strToBinaryStr(String str) {
        char[] strChar = str.toCharArray();
        String result = "";
        for (int i = 0; i < strChar.length; i++) {
            String s = Integer.toBinaryString(strChar[i]);
            result += s;
        }
        return result;
    }
    public static String strToBinaryStr16(String str) {
        char[] strChar = str.toCharArray();
        String result = "";
        for (int i = 0; i < strChar.length; i++) {
            String s = Integer.toBinaryString(strChar[i]);
            s = StringUtils.leftPad(s,16,'0');
            result += s;
        }
        return result;
    }
    public static String strToBinaryStr(String str,String separator) {
        char[] strChar = str.toCharArray();
        String result = "";
        for (int i = 0; i < strChar.length; i++) {
            int x = (int)strChar[i];
            String s = Integer.toBinaryString(strChar[i]) + separator;
            result += s;
        }
        return result;
    }

    public static String strToStr(String str) {
        String binaryStr = strToBinaryStr(str);
        String result = binaryStrToStr(binaryStr);
        return result;
    }

    // 将二进制字符串转换成Unicode字符串
    private static String binaryStrToStr(String binStr) {
        String[] tempStr = strToStrArray(binStr);
        char[] tempChar = new char[tempStr.length];
        for (int i = 0; i < tempStr.length; i++) {
            tempChar[i] = binaryStrToChar(tempStr[i]);
        }
        return String.valueOf(tempChar);
    }

    // 将二进制字符串格式化成全16位带空格的Binstr
    public static String binaryStrToBinaryStr16(String input) {
        StringBuffer output = new StringBuffer();
        String[] tempStr = strToStrArray(input);
        for (int i = 0; i < tempStr.length; i++) {
            for (int j = 16 - tempStr[i].length(); j > 0; j--) {
                output.append('0');
            }
            output.append(tempStr[i] + " ");
        }
        return output.toString();
    }

    // 将全16位带空格的Binstr转化成去0前缀的带空格Binstr
    private static String Binstr16ToBinstr(String input) {
        StringBuffer output = new StringBuffer();
        String[] tempStr = strToStrArray(input);
        for (int i = 0; i < tempStr.length; i++) {
            for (int j = 0; j < 16; j++) {
                if (tempStr[i].charAt(j) == '1') {
                    output.append(tempStr[i].substring(j) + " ");
                    break;
                }
                if (j == 15 && tempStr[i].charAt(j) == '0')
                    output.append("0" + " ");
            }
        }
        return output.toString();
    }

    // 二进制字串转化为boolean型数组 输入16位有空格的Binstr
    private static boolean[] Binstr16ToBool(String input) {
        String[] tempStr = strToStrArray(input);
        boolean[] output = new boolean[tempStr.length * 16];
        for (int i = 0, j = 0; i < input.length(); i++, j++)
            if (input.charAt(i) == '1')
                output[j] = true;
            else if (input.charAt(i) == '0')
                output[j] = false;
            else
                j--;
        return output;
    }

    // boolean型数组转化为二进制字串 返回带0前缀16位有空格的Binstr
    private static String BoolToBinstr16(boolean[] input) {
        StringBuffer output = new StringBuffer();
        for (int i = 0; i < input.length; i++) {
            if (input[i])
                output.append('1');
            else
                output.append('0');
            if ((i + 1) % 16 == 0)
                output.append(' ');
        }
        output.append(' ');
        return output.toString();
    }

    // 将二进制字符串转换为char
    private static char binaryStrToChar(String binStr) {
        int[] temp = binaryStrToIntArray(binStr);
        int sum = 0;
        for (int i = 0; i < temp.length; i++) {
            sum += temp[temp.length - 1 - i] << i;
        }
        return (char) sum;
    }

    // 将初始二进制字符串转换成字符串数组，以空格相隔
    private static String[] strToStrArray(String str) {
        return str.split(" ");
    }

    // 将二进制字符串转换成int数组
    private static int[] binaryStrToIntArray(String binStr) {
        char[] temp = binStr.toCharArray();
        int[] result = new int[temp.length];
        for (int i = 0; i < temp.length; i++) {
            result[i] = temp[i] - 48;
        }
        return result;
    }
}
