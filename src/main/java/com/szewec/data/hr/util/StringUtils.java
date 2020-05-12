package com.szewec.data.hr.util;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

import static java.util.regex.Pattern.*;

public class StringUtils {


    public static String leftPad(String str, int size, String padStr) {

        if (str == null) {
            str = "";
        }

        if (str.length() > size) {
            return str;
        }

        StringBuilder sb = new StringBuilder();

        int i = size - str.length();

        for (int j = 0; j < i; j++) {
            sb.append(padStr);
        }

        sb.append(str);

        return sb.toString();
    }

    public static boolean isNotEmpty(String str) {
        return !isEmpty(str);
    }

    public static boolean isEmpty(String str) {
        return str == null || str.trim().isEmpty() || str.equalsIgnoreCase("null");
    }


    public static boolean isContainChinese(String str) {
        Pattern p = compile("[\u4e00-\u9fa5]");
        Matcher m = p.matcher(str);
        if (m.find()) {
            return true;
        }
        return false;
    }


}
