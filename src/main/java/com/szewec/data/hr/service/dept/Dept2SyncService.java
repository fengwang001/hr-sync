package com.szewec.data.hr.service.dept;

import com.szewec.data.hr.handler.Executor;
import com.szewec.data.hr.service.BaseSyncService;
import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.service.SyncHelper;
import com.szewec.data.hr.util.ConvertUtils;
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
public class Dept2SyncService implements BaseSyncService {


    public static final Logger LOGGER = LoggerFactory.getLogger(Dept2SyncService.class);

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    @Qualifier("oldHrDBLogExecutor")
    private Executor executor;

    @Override
    public void sync() {
        List<Map<String, Object>> dep1List = doQueryDep1List();
        doProcessDep1List(dep1List);
    }

    private List<Map<String, Object>> doQueryDep1List() {
        SQL sql = new SQL();
        sql.SELECT("fdept_number", "fdept_name");
        sql.FROM("t_hr_dept");
        sql.WHERE("fpdept_number=\'000\'");
        sql.WHERE("fdeleted = \'f\'");
        return syncHelper.query(sql.toString());
    }

    private void doProcessDep1List(List<Map<String, Object>> dep1List) {
        for (Map<String, Object> row : dep1List) {
            executor.execute(doProcessDep1Row(row));
        }
    }

    private String doProcessDep1Row(Map<String, Object> row) {
        String executeSql = "";
        List<Map<String, Object>> dep2Rows = doQueryDep2List((String) row.get("fdept_number"));
        for (Map<String, Object> dep2Row : dep2Rows) {
            boolean exists = beforeSyncRow(dep2Row, row);
            if (!exists) {
                executeSql += (doSyncRow(dep2Row, row) + ";\r\n");
            }
        }

        return executeSql;
    }


    private List<Map<String, Object>> doQueryDep2List(String fpdeptNumber) {
        SQL sql = new SQL();
        sql.SELECT("fdept_number", "fdept_name","fdept_manager");
        sql.FROM("t_hr_dept");
        sql.WHERE("fpdept_number =" + SqlUtils.addSQuote(fpdeptNumber));
        sql.WHERE("fdeleted = \'f\'");
        sql.WHERE("factive = \'t\'");

        return syncHelper.query(sql.toString());
    }


    /**
     * 同步前处理
     * @param dep2Row
     * @param dep1Row
     * @return
     */
    private boolean beforeSyncRow(Map<String, Object> dep2Row, Map<String, Object> dep1Row) {
        SQL sql = new SQL();
        sql.SELECT("count(1)");
        sql.FROM("[部门二]");
        sql.WHERE("名称 =" + SqlUtils.addSQuote((String) dep2Row.get("fdept_name")));
        sql.WHERE("isshow = \'1\'");

        String deptNumber = (String) dep1Row.get("fdept_number");
        String deptId = ConvertUtils.convertDep12Old(deptNumber);
        sql.WHERE("隶属一部门ID =" + SqlUtils.addSQuote(deptId));

        Long count = syncDataSource.getOldDataSource().queryForObject(sql.toString(), long.class);

        return count > 0;
    }


    private String doSyncRow(Map<String, Object> dep2Row, Map<String, Object> dep1Row) {
        SQL sql = new SQL();
        sql.INSERT_INTO("[部门二]");
        sql.VALUES("[名称]", addSSValQuote((String) dep2Row.get("fdept_name")));

        String deptNumber = (String) dep1Row.get("fdept_number");
        String deptId = ConvertUtils.convertDep12Old(deptNumber);

        String orderString = selectMaxOrderString(deptId);

        sql.VALUES("[隶属一部门ID]", addSSValQuote(deptId));
        sql.VALUES("[orderstring]", addSSValQuote(orderString));
        sql.VALUES("[isshow]", addSSValQuote("1"));

        if (StringUtils.isNotEmpty((String) dep2Row.get("fdept_manager"))) {
            String fzr = "select fname from t_hr_employee where fid = " + SqlUtils.addSQuote((String) dep2Row.get("fdept_manager"));
            String empName = syncDataSource.getNewDataSource().queryForObject(fzr, String.class);

            String empId = syncDataSource.getOldDataSource().queryForObject("SELECT ID from [个人基本信息] where 姓名 = " + SqlUtils.addSQuote(empName), String.class);

            sql.VALUES("[fzr]", addSSValQuote(empId));
        }

        return sql.toString();
    }

    private String selectMaxOrderString(String dep1Id) {
        String sql = "SELECT max(orderstring) + 1 FROM [dbo].[部门二] where 隶属一部门ID = " + SqlUtils.addSQuote(dep1Id);
        String maxOrderString = syncDataSource.getOldDataSource().queryForObject(sql, String.class);

        maxOrderString = StringUtils.leftPad(maxOrderString, 4, "0");

        return maxOrderString;
    }
}
