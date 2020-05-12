package com.szewec.data.hr.service.dept;

import com.szewec.data.hr.handler.Executor;
import com.szewec.data.hr.service.BaseSyncService;
import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.service.SyncHelper;
import com.szewec.data.hr.util.SqlUtils;
import com.szewec.data.hr.util.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static com.szewec.data.hr.util.SqlUtils.addSSValQuote;

@Component
public class Dep1SyncService implements BaseSyncService {


    public static final Logger LOGGER = LoggerFactory.getLogger(Dep1SyncService.class);


    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    @Qualifier("oldHrDBLogExecutor")
    private Executor executor;

    @Override
    public void sync() {
        SQL sql = new SQL();
        sql.SELECT("fdept_number", "fdept_name");
        sql.FROM("t_hr_dept");
        sql.WHERE("fpdept_number=\'000\'");
        sql.WHERE("fdeleted = \'f\'");

        List<Map<String, Object>> dataList = syncHelper.query(sql.toString());

        for (Map<String, Object> row : dataList) {
            executor.execute(syncRow(row));
        }
    }


    private String syncRow(Map<String, Object> row) {
        boolean exists = beforeSyncRow(row);
        if (!exists) {
            return (doSyncRow(row) + "\r\n");
        } else {
            return "";
        }
    }


    private boolean beforeSyncRow(Map<String, Object> row) {
        SQL sql = new SQL();
        sql.SELECT("count(1)");
        sql.FROM("[部门一]");
        sql.WHERE("名称 =" + SqlUtils.addSQuote((String) row.get("fdept_name")));
        sql.WHERE("isshow = \'1\'");

        Long count = syncDataSource.getOldDataSource().queryForObject(sql.toString(), long.class);

        return count > 0;
    }


    private String doSyncRow(Map<String, Object> row) {
        SQL sql = new SQL();
        sql.INSERT_INTO("[部门一]");
        sql.VALUES("[名称]", addSSValQuote((String) row.get("fdept_name")));

        String orderString = selectMaxOrderString();

        sql.VALUES("[orderstring]", addSSValQuote(orderString));
        sql.VALUES("[isshow]", addSSValQuote("1"));


        if (StringUtils.isNotEmpty((String) row.get("fdept_manager"))) {
            String fzr = "select fname from t_hr_employee where fid = " + SqlUtils.addSQuote((String) row.get("fdept_manager"));
            String empName = syncDataSource.getNewDataSource().queryForObject(fzr, String.class);

            String empId = syncDataSource.getOldDataSource().queryForObject("SELECT ID from [个人基本信息] where 姓名 = " + SqlUtils.addSQuote(empName), String.class);

            sql.VALUES("[fzr]", addSSValQuote(empId));
        }


        return sql.toString();
    }

    private String selectMaxOrderString() {
        String sql = "SELECT max(orderstring) + 1 FROM [dbo].[部门一]";
        String maxOrderString = syncDataSource.getOldDataSource().queryForObject(sql, String.class);

        maxOrderString = StringUtils.leftPad(maxOrderString, 4, "0");

        return maxOrderString;
    }
}
