package com.szewec.data.hr.util;

public class SqlUtils {

    public static String addSQuote(String sql) {
        return "\'" + sql + "\'";
    }

    public static String andDQuote(String sql) {
        return "\"" + sql + "\"";
    }

    public static String addSSValQuote(String val) {
        if (val == null) {
            return "null";
        }
        return "N\'" + val + "\'";
    }

    public static String addParenthesis(String sql) {
        return "(" + sql + ")";
    }

    public static String addSemAndNewLine(String sql) {
        if (sql == null || sql.length() == 0) {
            return "";
        } else {
            return sql + ";\r\n";
        }
    }

    public static String addSSValQuote2(String val) {
        if (val == null) {
            return "null";
        }
        return "\'" + val + "\'";
    }
}
