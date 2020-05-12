package com.szewec.data.hr.util;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public class DateUtils {


    public static DateTime parseDateTime(String str) {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        return formatter.parseDateTime(str);
    }


    public static String getNowTime() {
        DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");
        DateTime dateTime = new DateTime();
        return formatter.print(dateTime);
    }
}
