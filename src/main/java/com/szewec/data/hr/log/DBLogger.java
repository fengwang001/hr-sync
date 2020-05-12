package com.szewec.data.hr.log;

import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.util.SqlUtils;
import org.apache.ibatis.jdbc.SQL;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import static com.szewec.data.hr.util.SqlUtils.addSQuote;

@Component
public class DBLogger {

    private static final Logger LOGGER = LoggerFactory.getLogger(DBLogger.class);

    @Autowired
    private SyncDataSource syncDataSource;

    public void log(String remark, String sql, String errorMessage) {
        try {
            LocalDateTime localDate = new LocalDateTime();

            DateTimeFormatter formatter = DateTimeFormat.forPattern("yyyy-MM-dd");

            String executeId = formatter.print(localDate);


            DateTimeFormatter fullFormatter = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss");

            String executeTime = fullFormatter.print(localDate);

            SQL executeSQL = new SQL();
            executeSQL.INSERT_INTO("t_hr_sync_log");
            executeSQL.VALUES("fexecuteId", addSQuote(executeId));
            executeSQL.VALUES("fexecute_time", addSQuote(executeTime));

            if (sql != null) {
                sql = sql.replaceAll("\'", "\"");
            }

            if (errorMessage != null) {
                errorMessage = errorMessage.replaceAll("\'", "\"");
            }

            executeSQL.VALUES("fexecute_sql", addSQuote(sql));
            executeSQL.VALUES("fexecute_remark", addSQuote(remark));
            executeSQL.VALUES("ferror_message", addSQuote(errorMessage));

            syncDataSource.getOldDataSource().execute(executeSQL.toString());
        } catch (Exception e) {
            LOGGER.error("打印日志失败:", e);
        }
    }
}
