package com.szewec.data.hr.handler;

import com.szewec.data.hr.service.SyncDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class OldHrExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OldHrExecutor.class);

    @Autowired
    private SyncDataSource syncDataSource;

    @Override
    public void execute(String sql) {
        LOGGER.info("执行记录:" + sql);

        try {
            int update = syncDataSource.getOldDataSource().update(sql);
            if (update == 0) {
                LOGGER.error("执行异常,影响空行:" + sql);
            }
        } catch (Exception e) {
            LOGGER.error("执行异常:" + sql, e);
        }
    }

}
