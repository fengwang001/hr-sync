package com.szewec.data.hr.task;

import com.szewec.data.helper.DataHelper;
import com.szewec.data.hr.log.DBLogger;
import com.szewec.data.hr.service.HrSyncService;
import com.szewec.data.hr.service.dept.Dep1SyncService;
import com.szewec.data.hr.service.dept.Dept2SyncService;
import com.szewec.data.hr.service.project.ProjectSyncService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
public class SyncTask {

    @Autowired
    private HrSyncService hrSyncService;

    @Autowired
    private Dept2SyncService dept2SyncService;

    @Autowired
    private DBLogger dbLogger;

    @Autowired
    private Dep1SyncService dep1SyncService;

    @Autowired
    private ProjectSyncService projectSyncService;

    //    @Scheduled(cron = "0 10 16 * * ?")
//    @Scheduled(cron = "0 36 17 * * ?")
//    @Scheduled(cron = "0 */2 * * * ?")
    @Scheduled(fixedDelay = 1200 * 1000L)  //TODO
//    @Scheduled(fixedDelay = 7 * 1000L)
    public void sync() {
        dep1SyncService.sync();
        dept2SyncService.sync();
        hrSyncService.sync();
        //historyEmployeeSyncService.sync();
    }

    @Scheduled(cron = "0 */35 * * * ?")
    public void projectSync(){
        projectSyncService.sync();
    }

}
