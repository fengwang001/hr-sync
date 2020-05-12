package com.szewec.data.hr.service;

import com.szewec.data.hr.config.SyncTableProperties;
import com.szewec.data.hr.handler.Executor;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.szewec.data.hr.util.SqlUtils.addSSValQuote;
import static com.szewec.data.hr.util.SqlUtils.addSQuote;

@Service("t_hr_salary_account")
public class SalaryAccountSyncService extends AbstractSyncService{

    private static final Logger LOGGER = LoggerFactory.getLogger(SalaryAccountSyncService.class);

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    @Qualifier("oldHrDBLogExecutor")
    private Executor executor;

    @Autowired
    private SyncDataSource syncDataSource;

    @Override
    public void sync(SyncTableProperties syncTableProperties) {
        syncNewData(syncTableProperties);
        syncUpdateData(syncTableProperties);
    }

    @Override
    public void syncNewData(SyncTableProperties syncTableProperties) {
        return;
//        String sql = "SELECT\n" +
//                "  fid\n" +
//                ", fcard_number\n" +
//                ", fdeposit_bank\n" +
//                ", fdeposit_location\n" +
//                ", fdeposit_bank\n" +
//                ", femp_id\n" +
//                "FROM "+ syncTableProperties.getTableName() + " where fcreate_time > \'" + syncTableProperties.getLastSyncTime() + "\' and fdeleted ='f' and fenable = 't'";
//
//        List<Map<String, Object>> rows = syncHelper.query(sql);
//
//        doSyncNewData(syncTableProperties, rows);
    }

    @Override
    public void doSyncNewData(SyncTableProperties syncTableProperties, List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            executor.execute(doSyncNewRowData(syncTableProperties, row) + ";" + "\r\n");
        }
    }

    private boolean beforeSyncNewRowData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        StringBuilder idsql = new StringBuilder();

        if (row.get("femp_id") != null) {
            String femp_id = (String) row.get("femp_id");
            String empName = syncHelper.getEmpNameFromNewSys(femp_id);


            if (empName == null) {
                return false;
            }
            idsql.append("SELECT ID FROM [个人基本信息] where 姓名 = ").append(addSQuote(empName));

            String empId;
            try {
               empId = syncDataSource.getOldDataSource().queryForObject(idsql.toString(), String.class);
            } catch (Exception e) {
                return false;
            }

            StringBuilder csql = new StringBuilder();
            csql.append("select count(1) from " + syncTableProperties.getOldTableName() + " where 员工ID = " + addSQuote(empId));

            try {
               int count = syncDataSource.getOldDataSource().queryForObject(csql.toString(), Integer.class);
                if (count > 0) {
                    return false;
                } else {
                    return true;
                }
            } catch (Exception e) {
                return false;
            }

        } else {
            return false;
        }
    }

    public String doSyncNewRowData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        //校验工资卡是否已经存在，如果存在，不可插入
        boolean precheck = beforeSyncNewRowData(syncTableProperties, row);
        if (!precheck) {
            return "";
        }

        StringBuilder retSql = new StringBuilder();
        SQL sql = new SQL();
        sql.INSERT_INTO(syncTableProperties.getOldTableName());

        retSql.append(sql.toString());
        retSql.append(" ([工资卡号], [开户行], [开户地], [员工ID]) ");
        retSql.append("SELECT ");
        retSql.append(addSQuote((String) row.get("fcard_number")) + ",");
        retSql.append(addSQuote((String) row.get("fdeposit_bank")) + ",");
        retSql.append(addSQuote((String) row.get("fdeposit_location")) + ",");


        if (row.get("femp_id") != null) {
            String femp_id = (String) row.get("femp_id");
            String empName = syncHelper.getEmpNameFromNewSys(femp_id);


            if (empName == null) {
                return "";
            }
            retSql.append(" ID FROM [个人基本信息] where 姓名 = " + addSQuote(empName));
        }


        return retSql.toString();
    }

    @Override
    public void syncUpdateData(SyncTableProperties syncTableProperties) {
        return;
//        String sql = "SELECT\n" +
//                "  fid\n" +
//                ", fcard_number\n" +
//                ", fdeposit_bank\n" +
//                ", fdeposit_location\n" +
//                ", fdeposit_bank\n" +
//                ", femp_id\n" +
//                "FROM "+ syncTableProperties.getTableName() + " where fupdate_time > \'" + syncTableProperties.getLastSyncTime() + "\' and fdeleted ='f' and fenable = 't'";
//        List<Map<String, Object>> rows = syncHelper.query(sql);
//        doSyncUpdateData(syncTableProperties, rows);
    }

    @Override
    public void doSyncUpdateData(SyncTableProperties syncTableProperties, List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
           executor.execute (doSyncUpdateRowData(syncTableProperties, row) + ";" + "\r\n");
        }
    }

    public String doSyncUpdateRowData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        SQL sql = new SQL();
        sql.UPDATE(syncTableProperties.getOldTableName());

        if (row.get("fcard_number") != null) {
            sql.SET("[工资卡号] =" + addSSValQuote((String) row.get("fcard_number")));
        }

        if (row.get("fdeposit_bank") != null) {
            sql.SET("[开户行] = " + addSSValQuote((String) row.get("fdeposit_bank")));
        }

        if (row.get("fdeposit_location") != null) {
            sql.SET("[开户地] ="+ addSSValQuote((String) row.get("fdeposit_location")));
        }

        if (row.get("femp_id") != null) {
            String femp_id = (String) row.get("femp_id");
            String empName = syncHelper.getEmpNameFromNewSys(femp_id);

            if (empName == null) {
                return "";
            }


            String whereSql = sql.toString() +
                    " where 员工ID =  " + "(select ID from [个人基本信息] where 姓名 = \'" + empName + "\')";
            return whereSql;
        }

        return "";
    }
}
