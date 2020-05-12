package com.szewec.data.hr.config;

import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import javax.sql.DataSource;

/**
 * @ClassName ProjectSyncConfig
 * @Author wangfeng
 * @Date 2020/3/24 11:05
 * @Version 1.0
 **/
@ConfigurationProperties(prefix = "data.project.sync")
public class ProjectSyncConfig {

    private String tableName;


    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }
}
