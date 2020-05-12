package com.szewec.data.hr.service;

import com.szewec.data.project.constants.ProjectConfig;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

import static com.szewec.data.hr.constants.HrSyncConstants.NEWHR_JDBCTEMPLATE;
import static com.szewec.data.hr.constants.HrSyncConstants.OA_JDBCTEMPLATE;
import static com.szewec.data.hr.constants.HrSyncConstants.OLDHR_JDBCTEMPLATE;

@Component
public class SyncDataSource {

    @Autowired
    @Qualifier(NEWHR_JDBCTEMPLATE)
    private JdbcTemplate newDataSource;

    @Autowired
    @Qualifier(OLDHR_JDBCTEMPLATE)
    private JdbcTemplate oldDataSource;

    @Autowired
    @Qualifier(OA_JDBCTEMPLATE)
    private JdbcTemplate oldOaDataSource;

    @Autowired
    @Qualifier(ProjectConfig.NEWFEEPROJECT_JDBCTEMPLATE)
    private JdbcTemplate newFeeProjectDataSource;

    @Autowired
    @Qualifier(ProjectConfig.OLDPROJECT_JDBCTEMPLATE)
    private JdbcTemplate oldProjectDataSource;

    public JdbcTemplate getNewDataSource() {
        return newDataSource;
    }

    public JdbcTemplate getOldDataSource() {
        return oldDataSource;
    }

    public JdbcTemplate getOldOaDataSource() {
        return oldOaDataSource;
    }

    public JdbcTemplate getNewFeeProjectDataSource() {
        return newFeeProjectDataSource;
    }

    public JdbcTemplate getOldProjectDataSource() {
        return oldProjectDataSource;
    }
}
