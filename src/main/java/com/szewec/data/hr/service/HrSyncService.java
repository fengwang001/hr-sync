package com.szewec.data.hr.service;

import com.szewec.data.hr.config.SyncConfig;
import com.szewec.data.hr.config.SyncTableProperties;
import com.szewec.data.hr.context.AppContext;
import com.szewec.data.hr.util.DateUtils;
import com.szewec.data.hr.util.SqlUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Service;

import java.util.List;

@Service
public class HrSyncService {


    private static final Logger LOGGER = LoggerFactory.getLogger(HrSyncService.class);

    @Autowired
    private SyncConfig syncConfig;

    @Autowired
    @Qualifier("newHrJdbcTemplate")
    private JdbcTemplate jdbcTemplate;


    public void sync() {
        doSync();
    }


    public void doSync() {
        List<SyncTableProperties> tables = syncConfig.getTables();
        for (SyncTableProperties properties : tables) {
            syncTable(properties);
        }
    }



    public void syncTable(SyncTableProperties tableProperties) {
        String lastSyncTime = DateUtils.getNowTime();
        doSyncTable(tableProperties);
        doRecordLastSyncTime(tableProperties.getTableName(), lastSyncTime);
        LOGGER.info("同步" + tableProperties.getTableName() + "结束,最后更新时间"+lastSyncTime);
    }


    private void doSyncTable(SyncTableProperties tableProperties) {
        String tableName = tableProperties.getTableName();
        String lastSyncTime = tableProperties.getLastSyncTime();

        if (isInit(tableName)) {
            initSyncData(tableProperties);
        } else {
            lastSyncTime = getLastSyncTime(tableName);
        }


        SyncService syncService = AppContext.getBean(tableName, SyncService.class);

        tableProperties.setLastSyncTime(lastSyncTime);

        syncService.sync(tableProperties);
    }

    private void doRecordLastSyncTime(String tableName, String lastSyncTime) {
        SQL sql = new SQL();
        sql.UPDATE("hr_sync");
        sql.SET("last_sync_time = \'" + lastSyncTime + "\'");
        sql.WHERE("table_name = \'" + tableName + "\'");

        jdbcTemplate.execute(sql.toString());
    }

    private boolean isInit(String tableName) {
        String sql = "select count(1) from hr_sync where table_name = \'" + tableName + "\'";
        Long count = jdbcTemplate.queryForObject(sql, Long.class);
        return count == 0;
    }


    private void initSyncData(SyncTableProperties properties) {
        SQL sql = new SQL();
        sql.INSERT_INTO("hr_sync");
        sql.VALUES(SqlUtils.andDQuote("table_name"), SqlUtils.addSQuote(properties.getTableName()));
        sql.VALUES(SqlUtils.andDQuote("last_sync_time"), SqlUtils.addSQuote(properties.getLastSyncTime()));

        jdbcTemplate.execute(sql.toString());
    }


    private String getLastSyncTime(String tableName) {
        SQL sql = new SQL();
        sql.SELECT("last_sync_time");
        sql.FROM("hr_sync");
        sql.WHERE("table_name = \'" + tableName + "\'");

        return  jdbcTemplate.queryForObject(sql.toString(), String.class);
    }





}
