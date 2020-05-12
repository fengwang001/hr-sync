package com.szewec.data.hr.handler.event.listener;

import com.szewec.data.hr.handler.event.EventObject;
import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.util.SqlUtils;
import com.szewec.data.hr.util.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static com.szewec.data.hr.util.ConvertUtils.convertCode2Name;
import static com.szewec.data.hr.util.SqlUtils.addSQuote;

@Component("oaEmployeeListener")
public class OAEmployeeListener implements EventListener{

    private static final Logger LOGGER = LoggerFactory.getLogger(OAEmployeeListener.class);

    @Autowired
    private SyncDataSource syncDataSource;

    @Override
    public void onEvent(EventObject event) {
        if (1 == 1) {
            return;
        }
        Map<String, Object> data = (Map<String, Object>) event.getData();

        SQL sql = new SQL();
        sql.INSERT_INTO("user");

        String uidSql = "SELECT max(UID) + 1 FROM `user`";
        String uid = syncDataSource.getOldOaDataSource().queryForObject(uidSql, String.class);

        sql.VALUES("UID", uid);

        if (StringUtils.isNotEmpty((String) data.get("fnumber"))) {
            sql.VALUES("USER_ID", (String) data.get("fnumber"));
        }

        if (StringUtils.isNotEmpty((String) data.get("fname"))) {
            sql.VALUES("USER_NAME", (String) data.get("fname"));
        }

        //USER_NAME_INDEX 姓名索引，暂时不填


        //BYNAME  昵称? 暂时不填


        //USEING_KEY

        sql.VALUES("USEING_KEY", addSQuote("0"));


        //USING_FINGER 默认值 '0',暂时不填


        //PASSWORD 写死空密码
        sql.VALUES("PASSWORD", addSQuote("$1$aT/.b25.$16lYGX7/pPNYIQpCyDVy2."));


        //KEY_SN  暂不填

        //SECURE_KEY_SN  暂不填

        //USER_PRIV 员工类型

        if (StringUtils.isNotEmpty((String) data.get("femp_type"))) {
            String femp_type = (String) data.get("femp_type");
            String empType = convertCode2Name(femp_type);

            if (empType.equals("职员")) {
                sql.VALUES("USER_PRIV", addSQuote("5"));
            } else if (empType.equals("实习生")) {
                sql.VALUES("USER_PRIV", addSQuote("26"));
            } else {
                sql.VALUES("USER_PRIV", addSQuote("25"));
            }
        }


        //POST_PRIV 默认0
        sql.VALUES("POST_PRIV", addSQuote("0"));

        //POST_DEPT

        //DEPT_ID 部门ID，暂时先不填
        String depId = getDepId((String)data.get("fdepartment1"), (String)data.get("fdepartment2"), (String)data.get("fdepartment3"));
        if (depId != null) {
            sql.VALUES("DEPT_ID", depId);
        }

        //DEPT_ID_OTHER

        //SEX 0男/1女
        if (data.get("fgender") != null) {
            String fgender = (String) data.get("fgender");
            sql.VALUES("SEX", fgender);
        }

        //BIRTHDAY
        if (data.get("fbirth_date") != null) {
            sql.VALUES("BIRTHDAY", addSQuote((String) data.get("fbirth_date")));

        }

        //IS_LUNAR


        //TEL_NO_DEPT

        //FAX_NO_DEPT


        //ADD_HOME  地址，暂时不同步


        //POST_NO_HOME


        //TEL_NO_HOME


        //MOBIL_NO
        if (StringUtils.isNotEmpty((String) data.get("fphone"))) {
            sql.VALUES("MOBIL_NO", addSQuote((String) data.get("fphone")));
        }


        //BP_NO



        //EMAIL
        if (StringUtils.isNotEmpty((String) data.get("femail"))) {
            sql.SET("EMAIL", addSQuote((String) data.get("femail")));
        }

        //OICQ_NO

        //ICQ_NO

        //MSN

        //NICK_NAME

        //AVATAR
        sql.VALUES("AVATAR", addSQuote("0"));

        //CALL_SOUND
        sql.VALUES("CALL_SOUND", addSQuote("1"));

        //BBS_SIGNATURE

        //BBS_COUNTER

        //DUTY_TYPE

        //LAST_VISIT_TIME


        //SMS_ON
        sql.VALUES("SMS_ON", addSQuote("1"));
        //MENU_TYPE
        sql.VALUES("MENU_TYPE", addSQuote("1"));
        //UIN
        sql.VALUES("UIN", addSQuote("0"));
        //PIC_ID
        sql.VALUES("PIC_ID", addSQuote("0"));
        //AUTHORIZE
        sql.VALUES("AUTHORIZE", addSQuote("0"));


        //CANBROADCAST 默认0

        //DISABLED  默认0

        //MOBILE_SP 不填

        //MOBILE_PS1 不填

        //MOBILE_PS2 不填

        //LAST_PASS_TIME 不填

        //THEME 默认1
        //SHORTCUT
        sql.VALUES("SHORTCUT", addSQuote("1,3,42,4,147,8,9,16,130,5,131,132,182,183,15,76,"));
        //PORTAL 不填

        //PANEL 默认1

        //ONLINE 默认0

        //ON_STATUS 默认1

        //USER_DEFINE 不填

        //MOBIL_NO_HIDDEN 默认0

        //MYTABLE_LEFT 默认ALL

        //MYTABLE_RIGHT 默认ALL

        //EMAIL_CAPACITY 默认0

        //FOLDER_CAPACITY 默认0

        //USER_PRIV_OTHER 不填

        //USER_NO 默认 10

        //NOT_LOGIN 默认0

        //NOT_VIEW_USER 默认0

        //NOT_VIEW_TABLE 默认0

        //NOT_SEARCH 默认0

        //BKGROUND 不填

        //BIND_IP 不填

        //LAST_VISIT_IP 不填

        //MENU_IMAGE 默认0

        //WEATHER_CITY 不填

        //SHOW_RSS  默认1

        //MY_RSS 不填

        //REMARK  不填

        //MENU_EXPAND 不填

        //WEBMAIL_CAPACITY 默认0

        //WEBMAIL_NUM 默认0

        //MY_STATUS 不填

        //SCORE 默认0

        //TDER_FLAG 不填

        //CONCERN_USER 不填

        //LIMIT_LOGIN 默认0,admin 1,设置为1
        sql.VALUES("LIMIT_LOGIN", addSQuote("1"));

        //PHOTO 不填

        //IM_RANGE 默认1

        //LEAVE_TIME 不填


        LOGGER.info(sql.toString());






    }


    private String getDepId(String dep1, String dep2, String dep3) {
        if (StringUtils.isNotEmpty(dep3)) {
            List<String> ret = selectDepID(dep3);
            if (ret.size() == 0) {
                return null;
            } else if (ret.size() == 1) {
                return ret.get(0);
            } else {
                if (StringUtils.isEmpty(dep2)) {
                    return null;
                }
                String depNameFromNewHr = getDepNameFromNewHr(dep2);
                List<String> dep2List = selectDepID(depNameFromNewHr);

                if (dep2List.size() > 0) {
                    return selectDepID(dep3, dep2List.get(0));
                } else {
                    return null;
                }
            }
        }

        if (StringUtils.isNotEmpty(dep2)) {
            String depNameFromNewHr = getDepNameFromNewHr(dep2);
            List<String> ret = selectDepID(depNameFromNewHr);
            if (ret.size() > 0) {
                return ret.get(0);
            } else {
                return null;
            }
        }

        if (StringUtils.isNotEmpty(dep1)) {
            String depNameFromNewHr = getDepNameFromNewHr(dep1);
            List<String> ret = selectDepID(depNameFromNewHr);

            if (ret.size() > 0) {
                return ret.get(0);
            } else {
                return null;
            }
        }

        return null;
    }


    private List<String> selectDepID(String depName) {
        String sql = "select DEPT_ID from department where DEPT_NAME = " + addSQuote(depName);
        try {
            List<String> ret = syncDataSource.getOldOaDataSource().query(sql, new RowMapper<String>() {
                @Override
                public String mapRow(ResultSet rs, int rowNum) throws SQLException {
                    return rs.getString("DEPT_ID");
                }
            });
            return ret;
        } catch (EmptyResultDataAccessException e) {
            return Collections.EMPTY_LIST;
        }

    }


    private String getDepNameFromNewHr(String depNumber) {
        String sql = "select fdept_name from t_hr_dept where fdept_number =" + SqlUtils.addSQuote(depNumber) + " and fdeleted ='f'";
        return syncDataSource.getNewDataSource().queryForObject(sql, String.class);
    }

    private String selectDepID(String depName, String dep2Id) {
        String sql = "select DEPT_ID from department where DEPT_NAME = " + addSQuote(depName) + " and DEPT_PARENT = " + SqlUtils.addSQuote(dep2Id) + " limit 1";
        try {
            return syncDataSource.getOldOaDataSource().queryForObject(sql, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }

    }
}
