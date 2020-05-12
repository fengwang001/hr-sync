package com.szewec.data.hr.service.history;

import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.service.SyncHelper;
import com.szewec.data.hr.util.SqlUtils;
import com.szewec.data.hr.util.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

import static com.szewec.data.hr.util.ConvertUtils.convertCode2Name;
import static com.szewec.data.hr.util.ConvertUtils.convertDep22Old;
import static com.szewec.data.hr.util.SqlUtils.addSSValQuote;

@Component
public class HistoryEmployeeSyncService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HistoryEmployeeSyncService.class);

    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    private SyncHelper syncHelper;

    public void sync() {
        String sql = "select * from t_hr_employee WHERE fname in (\'张希孟\',\'霍隆祥\',\'吴纯辉\',\'邓金连\',\'徐少娟\')";
        List<Map<String, Object>> ret = syncHelper.query(sql);


        String syncSql = "";
        for (Map<String, Object> row : ret) {
            SQL rowSql = new SQL();
            rowSql.UPDATE("dbo.[个人基本信息]");
            rowSql.SET("[入职时间] =" + SqlUtils.addSSValQuote((String) row.get("fhire_date")));
            rowSql.SET("[员工类别] =" + addSSValQuote(convertCode2Name((String) row.get("femp_type"))));

            if (StringUtils.isNotEmpty((String) row.get("fdepartment3"))) {
                //部门二
                String fdepartment2 = (String) row.get("fdepartment2");
                //部门三
                String fdepartment3 = getOldDep3Id(fdepartment2, (String) row.get("fdepartment3"));

                if (StringUtils.isNotEmpty(fdepartment3)) {
                    rowSql.SET("[部门三ID] = ", fdepartment3);
                }
            }

            rowSql.WHERE("[姓名] =" + addSSValQuote((String) row.get("fname")));
            syncSql += SqlUtils.addSemAndNewLine(rowSql.toString());
        }

        LOGGER.info(syncSql);
    }

    /**
     * 根据新系统部门二和部门三获取老系统部门三
     *
     * @param dep2Number
     * @param dep3Name
     * @return
     */
    private String getOldDep3Id(String dep2Number, String dep3Name) {
        String fdepartment2 = convertDep22Old(dep2Number);
        if (fdepartment2 == null) {
            return "null";
        }

        String sql = "select top(1) ID from 部门三 WHERE [隶属二部门ID] = \'" + fdepartment2 + "\' and 名称 = \'" + dep3Name + "\'";

        try {
            return addSSValQuote(syncDataSource.getOldDataSource().queryForObject(sql, String.class));
        } catch (EmptyResultDataAccessException e) {
            return "null";
        }
    }

}
