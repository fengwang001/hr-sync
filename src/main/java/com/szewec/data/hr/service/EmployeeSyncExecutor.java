package com.szewec.data.hr.service;

import com.szewec.data.hr.config.SyncConfig;
import com.szewec.data.hr.config.SyncTableProperties;
import com.szewec.data.hr.handler.Executor;
import com.szewec.data.hr.handler.event.EventObject;
import com.szewec.data.hr.handler.event.listener.EventListener;
import com.szewec.data.hr.util.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.util.Map;

import static com.szewec.data.hr.constants.SQLConstant.SQL_NULL;
import static com.szewec.data.hr.util.ConvertUtils.*;
import static com.szewec.data.hr.util.SqlUtils.addSSValQuote;

@Component
public class EmployeeSyncExecutor {

    @Autowired
    @Qualifier("oldHrDBLogExecutor")
    private Executor executor;

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    private SyncConfig syncConfig;

    @Autowired
    @Qualifier("oaEmployeeListener")
    private EventListener eventListener;

    private static final Logger LOGGER = LoggerFactory.getLogger(EmployeeSyncExecutor.class);

    private int i = 0;

    @Async("employeeAsync")
    public void doSingleInsert(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        //如果姓名存在，则把之前的名字更新为##
        String beforeSQL = beforeExecuteNewSync(row);
        if (beforeSQL != null) {
            executor.execute(beforeSQL + ";" + "\r\n");
        }
        executor.execute(doSyncNewRowData(syncTableProperties, row) + ";" + "\r\n");
        LOGGER.info("处理一条" + i++);
    }

    @Async("employeeAsync")
    public void doSingleUpdate(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        boolean needNew = beforeSyncUpdateData(syncTableProperties, row);
        if (needNew) {
            executor.execute(doSyncNewRowData(syncTableProperties, row) + ";" + "\r\n");
        } else {
            executor.execute(doSyncUpdateRowData(syncTableProperties, row) + ";" + "\r\n");
            afterSyncNewRowData(syncTableProperties, row);
        }
        LOGGER.info("处理一条" + i++);
    }


    private String beforeExecuteNewSync(Map<String, Object> row) {
        String name = (String) row.get("fname");
        String idNumber = (String) row.get("fid_number");


        String sql = "select count(1) from [个人基本信息] where 姓名 = " + addSSValQuote(name) + " and 身份证号 =" + addSSValQuote(idNumber);

        int count = syncDataSource.getOldDataSource().queryForObject(sql, Integer.class);

        if (count != 0) {
//        if (true) {
            String updateNameSQL = "update [个人基本信息] set [身份证号] = " + addSSValQuote(idNumber + "-" + getNewCurrentNumber())
                    + ", [姓名] = " + addSSValQuote(name + "#" + getNewCurrentNumber()) + "where  姓名 ="
                    + addSSValQuote(name);
            return updateNameSQL;
        }
        return null;
    }


    public String doSyncNewRowData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        SQL sql = new SQL();
        sql.INSERT_INTO(syncTableProperties.getOldTableName());
        if (StringUtils.isNotEmpty((String) row.get("fnumber"))) {
            sql.VALUES("[工号]", addSSValQuote((String) row.get("fnumber")));
        } else {
            //待审核的人员没有工号，先分配一个临时工号
            String number = getNewNumber();
            sql.VALUES("[工号]", addSSValQuote(number));
        }

        if (row.get("fname") != null) {
            sql.VALUES("[姓名]", addSSValQuote((String) row.get("fname")));
        }

        if (row.get("fdepartment1") != null) {
            String fdepartment1 = (String) row.get("fdepartment1");

            sql.VALUES("[部门一ID]", addSSValQuote(convertDep12Old(fdepartment1)));
        }

        if (StringUtils.isNotEmpty((String) row.get("fdepartment2"))) {
            String fdepartment2 = getOldDep2Id((String) row.get("fdepartment2"));

            if (StringUtils.isNotEmpty(fdepartment2)) {
                sql.VALUES("[部门二ID]", fdepartment2);
            } else {
                sql.SET("[部门二ID] = " + addSSValQuote("4"));
            }
        } else {
            sql.SET("[部门二ID] = " + addSSValQuote("4"));
        }


        if (StringUtils.isNotEmpty((String) row.get("fdepartment3"))) {
            String fdepartment3 = getOldDep3Id((String) row.get("fdepartment3"));

            if (StringUtils.isNotEmpty(fdepartment3)) {
                sql.VALUES("[部门三ID]", fdepartment3);
            } else {
                sql.SET("[部门三ID] = " + addSSValQuote("4"));
            }
        } else {
            sql.SET("[部门三ID] = " + addSSValQuote("4"));
        }

        if (row.get("fpost") != null) {
            sql.VALUES("[岗位]", addSSValQuote((String) row.get("fpost")));
        }

        if (row.get("femp_status") != null) {
            String femp_status = (String) row.get("femp_status");
            sql.VALUES("[员工状态]", addSSValQuote(convertCode2Name(femp_status)));
        }


        if (StringUtils.isNotEmpty((String) row.get("femp_type"))) {
            String femp_type = (String) row.get("femp_type");
            sql.VALUES("[员工类别]", addSSValQuote(convertCode2Name(femp_type)));
        }

        if (row.get("fgender") != null) {
            String fgender = (String) row.get("fgender");
            sql.VALUES("[性别]", addSSValQuote(convertGender2Old(fgender)));
        }

        if (row.get("fbirth_date") != null) {
            sql.VALUES("[出生日期]", addSSValQuote((String) row.get("fbirth_date")));
        }

        if (row.get("fage") != null) {
            sql.VALUES("[年龄]", addSSValQuote((String) row.get("fage")));
        }

        if (row.get("fnationality") != null) {
            String fnationality = (String) row.get("fnationality");
            sql.VALUES("[民族]", addSSValQuote(convertCode2Name(fnationality)));
        }

        if (row.get("fnative_place") != null) {
            sql.VALUES("[籍贯]", addSSValQuote((String) row.get("fnative_place")));
        }

        if (row.get("freg_residence") != null) {
            sql.VALUES("[户口所在地]", addSSValQuote((String) row.get("freg_residence")));
        }
        //sql.VALUES("第一学历", (String) row.get(""));
        //sql.VALUES("最高学历", (String) row.get(""));

        if (row.get("farchives_address") != null) {
            sql.VALUES("[档案所在地]", addSSValQuote((String) row.get("farchives_address")));
        }


        if (row.get("fmarry_status") != null) {
            String fmarry_status = (String) row.get("fmarry_status");
            sql.VALUES("[婚姻状况]", addSSValQuote(convertCode2Name(fmarry_status)));
        }

        if (row.get("fto_regular_time") != null) {
            sql.VALUES("[转正时间]", addSSValQuote((String) row.get("fto_regular_time")));
        }

        //2018-12-16 新增同步入职时间 -- liyao
        if (StringUtils.isNotEmpty((String) row.get("fhire_date"))) {
            sql.VALUES("[入职时间]", addSSValQuote((String) row.get("fhire_date")));
        }

        if (row.get("fend_date") != null) {
            sql.VALUES("[离职时间]", addSSValQuote((String) row.get("fend_date")));
        }


        if (row.get("fleave_reason") != null) {
            sql.VALUES("[离职原因]", addSSValQuote((String) row.get("fleave_reason")));
        }

        if (row.get("fwork_time") != null) {
            sql.VALUES("[参加工作时间]", addSSValQuote((String) row.get("fwork_time")));
        }

        if (row.get("fphone") != null) {
            sql.VALUES("[联系方式]", addSSValQuote((String) row.get("fphone")));
        }

        if (row.get("foffice_phone") != null) {
            sql.VALUES("[办公电话]", addSSValQuote((String) row.get("foffice_phone")));
        }

        //sql.VALUES("短号", (String) row.get(""));

        if (row.get("femail") != null) {
            sql.VALUES("[电子邮箱]", addSSValQuote((String) row.get("femail")));
        }
        //sql.VALUES("最高职称", (String) row.get(""));

        if (row.get("fid_number") != null) {
            sql.VALUES("[身份证号]", addSSValQuote((String) row.get("fid_number")));
        }

//        if (row.get("fphoto") != null) {
//            sql.VALUES("[照片]", addSSValQuote((String) row.get("fphoto")));
//        }

        if (row.get("fwork_rank") != null) {
            sql.VALUES("[职级]", addSSValQuote((String) row.get("fwork_rank")));
        }


        if (row.get("fwork_place") != null) {
            sql.VALUES("[工作地点]", addSSValQuote((String) row.get("fwork_place")));
        }


        //sql.VALUES("排序号", (String) row.get(""));

        if (row.get("fwork_content") != null) {
            String fwork_content = (String) row.get("fwork_content");
            sql.VALUES("[工作内容]", addSSValQuote(convertCode2Name(fwork_content)));
        }

        if (row.get("fwork_content") != null) {
            String fbelong_company = (String) row.get("fbelong_company");
            sql.VALUES("[所属公司]", addSSValQuote(convertCode2Name(fbelong_company)));
        }


        if (row.get("fremark") != null) {
            sql.VALUES("[备注]", addSSValQuote((String) row.get("fremark")));
        }

        //sql.VALUES("isglc", (String) row.get("fdeleted"));
        //sql.VALUES("glcxh", (String) row.get(""));
        //sql.VALUES("KDFID", (String) row.get(""));

        if (row.get("fpost_level") != null) {
            sql.VALUES("[岗位级别]", addSSValQuote((String) row.get("fpost_level")));
        }

        if (row.get("fhome_address") != null) {
            sql.VALUES("[家庭住址]", addSSValQuote((String) row.get("fhome_address")));
        }

        if (row.get("fpost_address") != null) {
            sql.VALUES("[现联系地址]", addSSValQuote((String) row.get("fpost_address")));
        }

        if (row.get("fspouse_name") != null) {
            sql.VALUES("[配偶姓名]", addSSValQuote((String) row.get("fspouse_name")));
        }


        if (row.get("fspouse_id_number") != null) {
            sql.VALUES("[配偶身份证]", addSSValQuote((String) row.get("fspouse_id_number")));
        }


        if (row.get("fenterprise_mail") != null) {
            sql.VALUES("[企业邮箱]", addSSValQuote((String) row.get("fenterprise_mail")));
        }


        //sql.VALUES("招聘渠道", (String) row.get(""));
        //sql.VALUES("入职次数", (String) row.get(""));
        //sql.VALUES("职位", (String) row.get(""));

        if (row.get("fconcurrent_post") != null) {
            sql.VALUES("[兼任]", addSSValQuote((String) row.get("fconcurrent_post")));
        }

        if (row.get("ftribe") != null) {
            String ftribe = (String) row.get("ftribe");
            sql.VALUES("[族]", addSSValQuote(convertCode2Name(ftribe)));
        }

        if (row.get("fcategory") != null) {
            sql.VALUES("[类]", addSSValQuote((String) row.get("fcategory")));
        }

        if (row.get("fsub_category") != null) {
            sql.VALUES("[子类]", addSSValQuote((String) row.get("fsub_category")));
        }
        //sql.VALUES("其他", (String) row.get(""));

        if (row.get("fsequence") != null) {
            String fsequence = (String) row.get("fsequence");
            sql.VALUES("[序列]", addSSValQuote(convertCode2Name(fsequence)));
        }

        return sql.toString();
    }


    private String getNewNumber() {
        return new StringBuilder().append("ls").append(getNewCurrentNumber()).toString();
    }

    private String getNewCurrentNumber() {
        String sql = "select max(fcurrent_number) + 1 from t_hr_sync_number_sequence";
        int current = syncDataSource.getOldDataSource().queryForObject(sql, Integer.class);
        syncDataSource.getOldDataSource().update("update t_hr_sync_number_sequence set fcurrent_number = fcurrent_number + 1");
        return new StringBuilder().append(current).toString();
    }

    public boolean beforeSyncUpdateData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        String name = (String) row.get("fname");

        String sql = "select count(1) from [个人基本信息] where 姓名 = " + addSSValQuote(name);

        int count = syncDataSource.getOldDataSource().queryForObject(sql, Integer.class);

        //如果人员不存在，则进行新增
        return count == 0;
    }

    public void afterSyncNewRowData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        if (syncConfig.isEnableOASync()) {
            EventObject event = new EventObject();
            event.setData(row);
            eventListener.onEvent(event);
        }
    }

    public String doSyncUpdateRowData(SyncTableProperties syncTableProperties, Map<String, Object> row) {
        SQL sql = new SQL();
        sql.UPDATE(syncTableProperties.getOldTableName());
        if (StringUtils.isNotEmpty((String) row.get("fnumber"))) {
            sql.SET("[工号] =" + addSSValQuote((String) row.get("fnumber")));
        } else {
            //待审核的人员没有工号，先分配一个临时工号
            String number = getNewNumber();
            sql.VALUES("[工号]", addSSValQuote(number));
        }

        if (row.get("fname") != null) {
            sql.SET("[姓名] =" + addSSValQuote((String) row.get("fname")));
        }

        if (StringUtils.isNotEmpty((String) row.get("fdepartment1"))) {
            String fdepartment1 = (String) row.get("fdepartment1");

            sql.SET("[部门一ID] =" + addSSValQuote(convertDep12Old(fdepartment1)));
        } else {
            sql.SET("[部门一ID] =" + addSSValQuote("4"));
        }

        if (StringUtils.isNotEmpty((String) row.get("fdepartment2"))) {
            String fdepartment2 = getOldDep2Id((String) row.get("fdepartment2"));
            if (StringUtils.isNotEmpty(fdepartment2)) {
                sql.SET("[部门二ID] =" + fdepartment2);
            } else {
                sql.SET("[部门二ID] =" + addSSValQuote("4"));
            }
        } else {
            sql.SET("[部门二ID] =" + addSSValQuote("4"));
        }


        if (StringUtils.isNotEmpty((String) row.get("fdepartment3"))) {
            String fdepartment3 = getOldDep3Id((String) row.get("fdepartment3"));

            if (StringUtils.isNotEmpty(fdepartment3)) {
                sql.SET("[部门三ID] = " + fdepartment3);
            } else {
                sql.SET("[部门三ID] = " + addSSValQuote("4"));
            }
        } else {
            sql.SET("[部门三ID] = " + addSSValQuote("4"));
        }

        if (row.get("fpost") != null) {
            sql.SET("[岗位] =" + addSSValQuote((String) row.get("fpost")));
        }

        if (row.get("femp_status") != null) {
            String femp_status = (String) row.get("femp_status");
            sql.SET("[员工状态] =" + addSSValQuote(convertCode2Name(femp_status)));
        }


        if (StringUtils.isNotEmpty((String) row.get("femp_type"))) {
            String femp_type = (String) row.get("femp_type");
            sql.SET("[员工类别] =" + addSSValQuote(convertCode2Name(femp_type)));
        }

        if (row.get("fgender") != null) {
            String fgender = (String) row.get("fgender");
            sql.SET("[性别] =" + addSSValQuote(convertGender2Old(fgender)));
        }

        if (row.get("fbirth_date") != null) {
            sql.SET("[出生日期] =" + addSSValQuote((String) row.get("fbirth_date")));
        }

        if (row.get("fage") != null) {
            sql.SET("[年龄] =" + addSSValQuote((String) row.get("fage")));
        }

        if (row.get("fnationality") != null) {
            String fnationality = (String) row.get("fnationality");
            sql.SET("[民族] =" + addSSValQuote(convertCode2Name(fnationality)));
        }

        if (row.get("fnative_place") != null) {
            sql.SET("[籍贯] =" + addSSValQuote((String) row.get("fnative_place")));
        }

        if (row.get("freg_residence") != null) {
            sql.SET("[户口所在地] =" + addSSValQuote((String) row.get("freg_residence")));
        }
        //sql.VALUES("第一学历", (String) row.get(""));
        //sql.VALUES("最高学历", (String) row.get(""));

        if (row.get("farchives_address") != null) {
            sql.SET("[档案所在地] =" + addSSValQuote((String) row.get("farchives_address")));
        }


        if (row.get("fmarry_status") != null) {
            String fmarry_status = (String) row.get("fmarry_status");
            sql.SET("[婚姻状况] =" + addSSValQuote(convertCode2Name(fmarry_status)));
        }

        if (row.get("fto_regular_time") != null) {
            sql.SET("[转正时间] =" + addSSValQuote((String) row.get("fto_regular_time")));
        }

        if (StringUtils.isNotEmpty((String) row.get("fhire_date"))) {
            sql.SET("[入职时间] =" + addSSValQuote((String) row.get("fhire_date")));
        }

        if (row.get("fend_date") != null) {
            sql.SET("[离职时间] =" + addSSValQuote((String) row.get("fend_date")));
        }


        if (row.get("fleave_reason") != null) {
            sql.SET("[离职原因] =" + addSSValQuote((String) row.get("fleave_reason")));
        }

        if (row.get("fwork_time") != null) {
            sql.SET("[参加工作时间] =" + addSSValQuote((String) row.get("fwork_time")));
        }

        if (row.get("fphone") != null) {
            sql.SET("[联系方式] =" + addSSValQuote((String) row.get("fphone")));
        }

        if (row.get("foffice_phone") != null) {
            sql.SET("[办公电话] =" + addSSValQuote((String) row.get("foffice_phone")));
        }

        if (row.get("femail") != null) {
            sql.SET("[电子邮箱] =" + addSSValQuote((String) row.get("femail")));
        }

        if (row.get("fid_number") != null) {
            sql.SET("[身份证号] =" + addSSValQuote((String) row.get("fid_number")));
        }

        if (row.get("fwork_rank") != null) {
            sql.SET("[职级] =" + addSSValQuote((String) row.get("fwork_rank")));
        }


        if (row.get("fwork_place") != null) {
            sql.SET("[工作地点] =" + addSSValQuote((String) row.get("fwork_place")));
        }

        if (row.get("fwork_content") != null) {
            String fwork_content = (String) row.get("fwork_content");
            sql.SET("[工作内容] =" + addSSValQuote(convertCode2Name(fwork_content)));
        }

        if (row.get("fwork_content") != null) {
            String fbelong_company = (String) row.get("fbelong_company");
            sql.SET("[所属公司] =" + addSSValQuote(convertCode2Name(fbelong_company)));
        }


        if (row.get("fremark") != null) {
            sql.SET("[备注] =" + addSSValQuote((String) row.get("fremark")));
        }

        if (row.get("fpost_level") != null) {
            sql.SET("[岗位级别] =" + addSSValQuote((String) row.get("fpost_level")));
        }

        if (row.get("fhome_address") != null) {
            sql.SET("[家庭住址] =" + addSSValQuote((String) row.get("fhome_address")));
        }

        if (row.get("fpost_address") != null) {
            sql.SET("[现联系地址] =" + addSSValQuote((String) row.get("fpost_address")));
        }

        if (row.get("fspouse_name") != null) {
            sql.SET("[配偶姓名] =" + addSSValQuote((String) row.get("fspouse_name")));
        }


        if (row.get("fspouse_id_number") != null) {
            sql.SET("[配偶身份证] =" + addSSValQuote((String) row.get("fspouse_id_number")));
        }


        if (row.get("fenterprise_mail") != null) {
            sql.SET("[企业邮箱] =" + addSSValQuote((String) row.get("fenterprise_mail")));
        }


        //sql.VALUES("招聘渠道", (String) row.get(""));
        //sql.VALUES("入职次数", (String) row.get(""));
        //sql.VALUES("职位", (String) row.get(""));

        if (row.get("fconcurrent_post") != null) {
            sql.SET("[兼任] =" + addSSValQuote((String) row.get("fconcurrent_post")));
        }

        if (row.get("ftribe") != null) {
            String ftribe = (String) row.get("ftribe");
            sql.SET("[族] =" + addSSValQuote(convertCode2Name(ftribe)));
        }

        if (row.get("fcategory") != null) {
            sql.SET("[类] =" + addSSValQuote((String) row.get("fcategory")));
        }

        if (row.get("fsub_category") != null) {
            sql.SET("[子类] =" + addSSValQuote((String) row.get("fsub_category")));
        }
        //sql.VALUES("其他", (String) row.get(""));

        if (row.get("fsequence") != null) {
            String fsequence = (String) row.get("fsequence");
            sql.SET("[序列] =" + addSSValQuote(convertCode2Name(fsequence)));
        }

        sql.WHERE("[姓名] =" + addSSValQuote((String) row.get("fname")));

        return sql.toString();

    }

    /**
     * 根据新系统部门二获取老系统部门二ID
     *
     * @param dep2Number
     * @return
     */
    private String getOldDep2Id(String dep2Number) {
        String fdepartment2 = convertDep22Old(dep2Number);
        if (fdepartment2 == null) {
            fdepartment2 = SQL_NULL;
        } else {
            fdepartment2 = addSSValQuote(fdepartment2);
        }

        return fdepartment2;
    }


    /**
     * 根据新系统部门二和部门三获取老系统部门三
     *
     * @param dep3Name
     * @return
     */
    private String getOldDep3Id(String dep3Name) {
        String sql = "select top(1) ID from 部门三 where isshow = '1' and 名称 = \'" + dep3Name + "\'";
        try {
            return addSSValQuote(syncDataSource.getOldDataSource().queryForObject(sql, String.class));
        } catch (Exception e) {
            return addSSValQuote("4");
        }
    }

}
