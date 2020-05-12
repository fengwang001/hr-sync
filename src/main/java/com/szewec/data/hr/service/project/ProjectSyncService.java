package com.szewec.data.hr.service.project;

import com.szewec.data.hr.service.BaseSyncService;
import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.service.SyncHelper;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * @ClassName ProjectSyncService
 * @Author wangfeng
 * @Date 2020/3/20 16:51
 * @Version 1.0
 **/
@Component
public class ProjectSyncService implements BaseSyncService {

    private static final Logger logger = LoggerFactory.getLogger(ProjectSyncService.class);

    /**
     * 老系统 kfsr_bm 表 键是专业中心名称，值为专业中心负责人ID
     */
    public Map<String,String> kfsr_bm = new HashMap<>();

    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    private ProjectSyncExecutor syncExecutor;

    @Override
    public void sync() {

        /**
         *  获取专业中心信息
         */
        SQL sql = new SQL();
        sql.SELECT("bm","ms");
        sql.FROM("kfsr_bm");
        List<Map<String,Object>> kfsrList =syncHelper.projectQuery(sql.toString());
        for(Map<String,Object> bm: kfsrList){
            kfsr_bm.put((String) bm.get("bm"),(String) bm.get("ms"));
        }

        Map<String,Object> productMap = this.getProductTypeNumber();

        List<Map<String,Object>> projects = doQueryProjectList();
        for(Map<String, Object> project: projects){
            syncExecutor.doSingleInsert(project,kfsr_bm,productMap);
        }
    }

    private List<Map<String,Object>> doQueryProjectList(){
        SQL sql = new SQL();
        sql.SELECT("*");
        sql.FROM("fView_MainPrj");
        List<Map<String, Object>> dataList = syncHelper.projectQuery(sql.toString());
        return dataList;
    }

    /**
     * 获取产品类型
     * @return
     */
    private Map<String,Object> getProductTypeNumber(){
        SQL sql = new SQL();
        sql.SELECT("fname","fnumber");
        sql.FROM("t_cfg_product_type");
        List<Map<String,Object>> products =syncHelper.newProject(sql.toString());

        Map<String,Object> productMap = new HashMap<>();
        for (Map<String,Object> product: products){
            productMap.put((String)product.get("fname"),(String)product.get("fnumber"));
        }

        return  productMap;
    }
}
