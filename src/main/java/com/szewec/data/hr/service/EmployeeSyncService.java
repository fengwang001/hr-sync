package com.szewec.data.hr.service;

import com.szewec.data.hr.config.SyncConfig;
import com.szewec.data.hr.config.SyncTableProperties;
import com.szewec.data.hr.handler.event.EventObject;
import com.szewec.data.hr.handler.event.listener.EventListener;
import com.szewec.data.hr.util.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;

import static com.szewec.data.hr.util.ConvertUtils.*;
import static com.szewec.data.hr.util.SqlUtils.addSSValQuote;

@Service("t_hr_employee")
public class EmployeeSyncService extends AbstractSyncService {

    private static final Logger LOGGER = LoggerFactory.getLogger(HrSyncService.class);

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    private SyncConfig syncConfig;

    @Autowired
    @Qualifier("oaEmployeeListener")
    private EventListener eventListener;


    @Autowired
    private EmployeeSyncExecutor employeeSyncExecutor;

    @Override
    public void sync(SyncTableProperties syncTableProperties) {
        syncNewData(syncTableProperties);
        syncUpdateData(syncTableProperties);
    }

    @Override
    public void syncNewData(SyncTableProperties syncTableProperties) {
        String sql = "SELECT\n" +
                "fid,fnumber,fname,fdepartment1,fdepartment2,fdepartment3,fdepartment3,fpost,femp_status,femp_type,fgender,fbirth_date,fage,\n" +
                "fnationality,fnative_place,freg_residence,freg_residence,freg_residence,freg_residence,fmarry_status,fhire_date,fto_regular_time,fend_date,fleave_reason,\n" +
                "fwork_time,fphone,foffice_phone,foffice_phone,femail,femail,fid_number,fphoto,fwork_rank,fwork_place,fcategory,fwork_content,fbelong_company,\n" +
                "fremark,fremark,fremark,fremark,fpost_level,fhome_address,fpost_address,fspouse_name,fspouse_id_number,femail,femail,\n" +
                "fhired_time,fpost,fconcurrent_post,ftribe,fcategory,fsub_category,fsub_category,fsequence,fcreate_time,fupdate_time\n" +
                "FROM " + syncTableProperties.getTableName() + " where fcreate_time > \'" + syncTableProperties.getLastSyncTime() + "\' and fnumber is not null and trim(fnumber)<>'' ";

        List<Map<String, Object>> rows = syncHelper.query(sql);


        doSyncNewData(syncTableProperties, rows);
    }


    public void syncUpdateData(SyncTableProperties syncTableProperties) {
        String sql = "SELECT\n" +
                "fid,fnumber,fname,fdepartment1,fdepartment2,fdepartment3,fdepartment3,fpost,femp_status,femp_type,fgender,fbirth_date,fage,\n" +
                "fnationality,fnative_place,freg_residence,freg_residence,freg_residence,freg_residence,fmarry_status,fhire_date,fto_regular_time,fend_date,fleave_reason,\n" +
                "fwork_time,fphone,foffice_phone,foffice_phone,femail,femail,fid_number,fphoto,fwork_rank,fwork_place,fcategory,fwork_content,fbelong_company,\n" +
                "fremark,fremark,fremark,fremark,fpost_level,fhome_address,fpost_address,fspouse_name,fspouse_id_number,femail,femail,\n" +
                "fhired_time,fpost,fconcurrent_post,ftribe,fcategory,fsub_category,fsub_category,fsequence,fcreate_time,fupdate_time\n" +
                "FROM " + syncTableProperties.getTableName() + " where fupdate_time > \'" + syncTableProperties.getLastSyncTime() + "\' and fnumber is not null and trim(fnumber)<>''";

        List<Map<String, Object>> rows = syncHelper.query(sql);
        doSyncUpdateData(syncTableProperties, rows);
    }


    @Override
    public void doSyncNewData(SyncTableProperties syncTableProperties, List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            employeeSyncExecutor.doSingleInsert(syncTableProperties, row);
        }
    }


    public void doSyncUpdateData(SyncTableProperties syncTableProperties, List<Map<String, Object>> rows) {
        for (Map<String, Object> row : rows) {
            employeeSyncExecutor.doSingleUpdate(syncTableProperties, row);
        }
    }
}
