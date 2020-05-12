package com.szewec.data.hr.handler;

import com.szewec.data.hr.log.DBLogger;
import com.szewec.data.hr.service.SyncDataSource;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OldHrDBLogExecutor implements Executor {

    @Autowired
    private DBLogger logger;

    @Autowired
    private SyncDataSource syncDataSource;

    @Override
    public void execute(String sql) {
        logger.log("执行记录:", sql, "");

        try {
            int update = syncDataSource.getOldDataSource().update(sql);
            if (update == 0) {
                logger.log("执行异常,影响空行:", sql, "");
            }
        } catch (Exception e) {
            logger.log("执行异常:", sql, e.getMessage());
        }
    }
}
