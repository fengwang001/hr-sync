package com.szewec.data.hr.handler;

import com.szewec.data.hr.log.DBLogger;
import com.szewec.data.hr.service.SyncDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;

/**
 * @ClassName NewFeeProjectExecutor
 * @Author wangfeng
 * @Date 2020/3/21 10:39
 * @Version 1.0
 **/
@Component
public class NewFeeProjectExecutor implements Executor {

    private static final Logger log = LoggerFactory.getLogger(NewFeeProjectExecutor.class);

    @Autowired
    private DBLogger logger;

    @Autowired
    private SyncDataSource syncDataSource;


    @Transactional(rollbackFor = Exception.class)
    @Override
    public void execute(String sql) {

        int insNo = sql.indexOf("INSERT ");
        String message = "";
        if(insNo == 0){
            log.info(sql);
            message = "新增";
        }else {
            message = "更新";
        }

        logger.log("项目同步数据",sql,message);
        try {
            int update = syncDataSource.getNewFeeProjectDataSource().update(sql);
            if(update == 0){
                logger.log("项目同步 执行异常，影响空行:", sql, "");
            }
        } catch (Exception e){
            logger.log("项目同步 执行异常:" , sql, e.getMessage());
        }

    }
}
