package com.szewec.data.hr.config;

import com.szewec.data.hr.constants.HrSyncConstants;
import com.szewec.data.hr.constants.ProjectSyncConstants;
import com.szewec.data.project.constants.ProjectConfig;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.jdbc.DataSourceBuilder;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;


@Configuration
public class DataSourceConfig {

    @Bean(name = HrSyncConstants.NEW_HR_DATA_SOURCE)
    @Qualifier(HrSyncConstants.NEW_HR_DATA_SOURCE)
    @ConfigurationProperties(prefix="spring.datasource.newhr")
    public DataSource newHrDataSource() {
        return DataSourceBuilder.create().build();
    }

    @Bean(name = HrSyncConstants.OLD_HR_DATA_SOURCE)
    @Qualifier(HrSyncConstants.OLD_HR_DATA_SOURCE)
    @Primary
    @ConfigurationProperties(prefix="spring.datasource.oldhr")
    public DataSource oldHrDataSource() {
        return DataSourceBuilder.create().build();
    }


    @Bean(name = HrSyncConstants.OA_DATA_SOURCE)
    @Qualifier(HrSyncConstants.OA_DATA_SOURCE)
    @ConfigurationProperties(prefix="spring.datasource.oa")
    public DataSource oaDataSource() {
        return DataSourceBuilder.create().build();
    }


    @Bean(name = HrSyncConstants.NEWHR_JDBCTEMPLATE)
    public JdbcTemplate newHrJdbcTemplate(
            @Qualifier(HrSyncConstants.NEW_HR_DATA_SOURCE) DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = HrSyncConstants.OLDHR_JDBCTEMPLATE)
    public JdbcTemplate oldHrJdbcTemplate(
            @Qualifier(HrSyncConstants.OLD_HR_DATA_SOURCE) DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


    @Bean(name = HrSyncConstants.OA_JDBCTEMPLATE)
    public JdbcTemplate oaJdbcTemplate(
            @Qualifier(HrSyncConstants.OA_DATA_SOURCE) DataSource dataSource) {
        return new JdbcTemplate(dataSource);
    }


    @Bean(name = ProjectConfig.NEW_FEE_PROJECT_DATA_SOURCE)
    @Qualifier(ProjectConfig.NEW_FEE_PROJECT_DATA_SOURCE)
    @ConfigurationProperties(prefix = "spring.datasource.newProject")
    public DataSource newFeeProjectDataSource(){return DataSourceBuilder.create().build(); }


    @Bean(name = ProjectConfig.OLD_PROJECT_DATA_SOURCE)
    @Qualifier(ProjectConfig.OLD_PROJECT_DATA_SOURCE)
    @ConfigurationProperties(prefix = "spring.datasource.accProject")
    public DataSource oldProjectDataSource(){return DataSourceBuilder.create().build(); }


    @Bean(name = ProjectConfig.NEWFEEPROJECT_JDBCTEMPLATE)
    public JdbcTemplate newFeeProjectTemplate(
            @Qualifier(ProjectConfig.NEW_FEE_PROJECT_DATA_SOURCE) DataSource dataSource ){
        return new JdbcTemplate(dataSource);
    }

    @Bean(name = ProjectConfig.OLDPROJECT_JDBCTEMPLATE)
    public JdbcTemplate oldProjectTemplate(@Qualifier(ProjectConfig.OLD_PROJECT_DATA_SOURCE) DataSource dataSource){
        return new JdbcTemplate(dataSource);
    }
}
