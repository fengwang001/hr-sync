package com.szewec.data.hr.service.project;

import com.alibaba.fastjson.JSON;
import com.szewec.data.hr.config.ProjectSyncConfig;
import com.szewec.data.hr.handler.NewFeeProjectExecutor;
import com.szewec.data.hr.service.SyncDataSource;
import com.szewec.data.hr.service.SyncHelper;
import com.szewec.data.hr.util.StringUtils;
import org.apache.ibatis.jdbc.SQL;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import static com.szewec.data.hr.util.SqlUtils.addSSValQuote2;

/**
 * @ClassName ProjectSyncExecutor
 * @Author wangfeng
 * @Date 2020/3/20 18:35
 * @Version 1.0
 **/
@Component
public class ProjectSyncExecutor {

    private static final Logger logger = LoggerFactory.getLogger(ProjectSyncExecutor.class);

//    private static final String DATATABLE = "t_fee_project_1";

    private  DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss") ;
    /**
     * 老系统 kfsr_bm 表 键是专业中心名称，值为专业中心负责人ID
     */
    public Map<String,String> kfsr_bm = new HashMap<>();

    @Autowired
    private SyncDataSource syncDataSource;

    @Autowired
    private NewFeeProjectExecutor executor;

    @Autowired
    private SyncHelper syncHelper;

    @Autowired
    private ProjectSyncConfig projectSyncConfig;


    private int i = 0;

    private int j = 0;

    @Async("projectAsync")
    public void doSingleInsert(Map<String, Object> row, Map<String,String> bm,Map<String,Object> productMap){
        String sql = beforeExecuteNewSync(row,bm,productMap);
        if(sql != null) {
            executor.execute(sql);

//            logger.info("项目同步一条" + i++);
        }
    }

    @Async("projectAsync")
    public void doSingleUpdate(Map<String,Object> row, Map<String,String> bm,Map<String,Object> productMap){
        String sql = doSyncUpdateRowData(row,bm,productMap);
        if(sql != null) {
            executor.execute(sql);

            logger.info("执行了一条sql" );
        }
    }

    private String beforeExecuteNewSync(Map<String, Object> row,Map<String,String> bm,Map<String,Object> productMap) {
        String projectNo = (String) row.get("项目编号");

        String sql = "select * from "+ projectSyncConfig.getTableName() +" where fnumber ="+ addSSValQuote2(projectNo) ;


        List<Map<String,Object>> projectMap = syncHelper.newProject(sql);

        String newSql = null;
        if(projectMap == null || projectMap.size() < 1){
            newSql = doSyncNewRowData(row,bm,productMap);
            logger.info("项目新增一条" + j++);
        }else {
            if (checkProjectObj(projectMap.get(0),row,productMap)){ // 判断是否修改
                newSql = doSyncUpdateRowData(row,bm,productMap);
                logger.info("项目更新一条" + i++);
            }
        }

        return newSql;
    }

    public String doSyncNewRowData(Map<String,Object> row,Map<String,String> bm,Map<String,Object> productMap){
        SQL sql = new SQL();
        sql.INSERT_INTO(projectSyncConfig.getTableName());
        sql.VALUES("fid", addSSValQuote2(UUID.randomUUID().toString().replaceAll("\\-","")));

        // 平台ID
        sql.VALUES("ftenant_id", addSSValQuote2("c813befb3cca486583a8dbed5ce991f4"));

        sql.VALUES("fnumber", addSSValQuote2((String) row.get("项目编号")));
        sql.VALUES("fname",addSSValQuote2((String)row.get("name")));

        if(StringUtils.isNotEmpty((String)row.get("一级外委"))){
            String outter = (String)row.get("一级外委");
            if(outter.equals("是")){
                sql.VALUES("foutter",addSSValQuote2("1"));
            }else {
                sql.VALUES("foutter",addSSValQuote2("0"));
            }

        }

        if(StringUtils.isNotEmpty((String)row.get("一级外委管理部门"))){
            sql.VALUES("foutter_manage_dept",addSSValQuote2((String)row.get("一级外委管理部门")));
        }

        if(StringUtils.isNotEmpty((String)row.get("产品类型"))){
            // 产品类型
            sql.VALUES("fproduct_type",addSSValQuote2((String)row.get("产品类型")));
            // 产品名称
            sql.VALUES("fproduct_type_name",addSSValQuote2((String)row.get("产品类型")));
            // 产品类型编码
            sql.VALUES("fproduct_type_number",addSSValQuote2((String)productMap.get((String)row.get("产品类型"))));
        }

        if(StringUtils.isNotEmpty((String)row.get("客户一"))){
            // 签约单位
            sql.VALUES("fcontract_comp",addSSValQuote2((String)row.get("客户一")));
        }

        if(StringUtils.isNotEmpty((String)row.get("客户二"))){
            // 建设管理单位
            sql.VALUES("fbuild_manage_comp",addSSValQuote2((String)row.get("客户二")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目经理"))){
            // 项目经理名称
            sql.VALUES("fproject_manager_name",addSSValQuote2((String)row.get("项目经理")));
        }

        if(StringUtils.isNotEmpty((String)row.get("BigClientLeaderName"))){
            // 大客户分管领导名称
            sql.VALUES("fbig_customer_leader_name",addSSValQuote2((String)row.get("BigClientLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("ProductLeaderName"))){
            // 版块分管领导名称
            sql.VALUES("fproduct_type_leader_name",addSSValQuote2((String)row.get("ProductLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("DistrictLeaderName"))){
            // 片区分管领导名称
            sql.VALUES("fdistrict_leader_name",addSSValQuote2((String)row.get("DistrictLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("ProfessionalCenterName"))){
            // 专业中心
            sql.VALUES("fbusiness_center",addSSValQuote2((String)row.get("ProfessionalCenterName")));

            // 专业中心负责人 id
            String msId =  bm.get((String)row.get("ProfessionalCenterName"));

            SQL hrSql = new SQL();
            hrSql.SELECT("姓名");
            hrSql.FROM("[个人基本信息]");
            hrSql.WHERE("ID =" + addSSValQuote2(msId));
            List<Map<String,Object>> hrList = syncHelper.oldHrQuery(hrSql.toString());
            if(hrList != null && hrList.size()>0){
                sql.VALUES("fbusiness_center_leader_name", addSSValQuote2((String)hrList.get(0).get("姓名")));
            }
        }

        if(StringUtils.isNotEmpty((String)row.get("ProdLeaderName"))){
            // 生产决策分管领导
            sql.VALUES("fproduction_making_leader",addSSValQuote2((String)row.get("ProdLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("MarketLeaderName"))){
            // 市场决策分管领导
            sql.VALUES("fmarket_making_leader",addSSValQuote2((String)row.get("MarketLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("DevelopMainDeptName"))){
            // 开发主导部门
            sql.VALUES("fdominated_dept",addSSValQuote2((String)row.get("DevelopMainDeptName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("DevelopMainPersonName"))){
            // 开发主导人
            sql.VALUES("fdominated_person",addSSValQuote2((String)row.get("DevelopMainPersonName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("所属分区"))){
            // 所在区域
            sql.VALUES("fregion",addSSValQuote2((String)row.get("所属分区")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目分级"))){
            // 项目分级
            sql.VALUES("fpro_classification",addSSValQuote2((String)row.get("项目分级")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目阶段"))){
            // 项目阶段
            sql.VALUES("fpro_phase",addSSValQuote2((String)row.get("项目阶段")));
        }

        if(StringUtils.isNotEmpty((String)row.get("产值核算合同额"))){

            // 产值核算合同额
            sql.VALUES("foutput_balance_amount",addSSValQuote2((String)row.get("产值核算合同额")));
        }

        if(StringUtils.isNotEmpty((String)row.get("签约合同额"))){
            // 签约合同额
            sql.VALUES("fsign_contract_amount",addSSValQuote2((String)row.get("签约合同额")));
        }

        if(StringUtils.isNotEmpty((String)row.get("回款核算合同额"))){
            // 回款核算合同额
            sql.VALUES("freceive_balance_amount",addSSValQuote2((String)row.get("回款核算合同额")));
        }

        if(StringUtils.isNotEmpty((String)row.get("内部结算手续"))){
            // 内部结算手续
            sql.VALUES("finside_balance",addSSValQuote2((String)row.get("内部结算手续")));
        }

        if(StringUtils.isNotEmpty((String)row.get("外部结算手续"))){
            // 外部结算手续
            sql.VALUES("fextemal_balance",addSSValQuote2((String)row.get("外部结算手续")));
        }

        if(StringUtils.isNotEmpty((String)row.get("结算合同额"))){
            // 结算合同额
            sql.VALUES("fbalance_contract_amount",addSSValQuote2((String)row.get("结算合同额")));
        }

        if(StringUtils.isNotEmpty((String)row.get("可回款"))){

            // 可汇款金额
            sql.VALUES("freceivable_income",addSSValQuote2((String)row.get("可回款")));
        }

        if(StringUtils.isNotEmpty((String)row.get("已回款"))){
            // 已回款金额
            sql.VALUES("freceived_income",addSSValQuote2((String)row.get("已回款")));
        }
        if(StringUtils.isNotEmpty((String)row.get("待回款"))){
            // 待回款金额
            sql.VALUES("fremainder",addSSValQuote2((String)row.get("待回款")));
        }

        if(StringUtils.isNotEmpty((String)row.get("合同形式"))){
            // 合同形式
            sql.VALUES("fcontract_shape",addSSValQuote2((String)row.get("合同形式")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目立项时间"))){
            // 项目立项时间
            sql.VALUES("fpro_approve_time", addSSValQuote2((String)row.get("项目立项时间")));
        }

        if(StringUtils.isNotEmpty((String)row.get("所在省市"))){
            // 所在省市
            sql.VALUES("flocation_city",addSSValQuote2((String)row.get("所在省市")));
        }

        if(StringUtils.isNotEmpty((String)row.get("合同付款条款"))){
            // 合同付款条款
            sql.VALUES("fcontract_payment",addSSValQuote2((String)row.get("合同付款条款")));
        }

        if(StringUtils.isNotEmpty((String)row.get("kaifahte"))){
            // 开发合同额
            sql.VALUES("fdev_contract_amount",addSSValQuote2((String)row.get("kaifahte")));
        }

        if(StringUtils.isNotEmpty((String)row.get("zdprice"))){
            // 暂定金额
            sql.VALUES("fprovisional_amount",addSSValQuote2((String)row.get("zdprice")));
        }


        if(StringUtils.isNotEmpty((String)row.get("合同价格类型"))){
            // 合同价格类型
            sql.VALUES("fcontract_price_type",addSSValQuote2((String)row.get("合同价格类型")));
        }

        if(StringUtils.isNotEmpty((String)row.get("Prj_BH_old"))){
            // 旧项目编号
            sql.VALUES("fold_pro_number",addSSValQuote2((String)row.get("Prj_BH_old")));
        }

        if(StringUtils.isNotEmpty((String)row.get("jianan"))){
            // 建安费
            sql.VALUES("fconstruction_expenses",addSSValQuote2((String)row.get("jianan")));
        }

        if(StringUtils.isNotEmpty((String)row.get("jizhun"))){
            // 基准服务费
            sql.VALUES("fbenchmark_service_charge",addSSValQuote2((String)row.get("jizhun")));
        }

        if(StringUtils.isNotEmpty((String)row.get("生产管理部门"))){
            // 生产管理部门名称
            sql.VALUES("fmanage_department_name",addSSValQuote2((String)row.get("生产管理部门")));
        }

        if(StringUtils.isNotEmpty((String)row.get("生产管理部门负责人"))){
            // 生产管理部门负责人名称
            sql.VALUES("fmanage_department_leader_name",addSSValQuote2((String)row.get("生产管理部门负责人")));
        }

        if(StringUtils.isNotEmpty((String)row.get("生产组织部门"))){
            // 生产组织部门
            sql.VALUES("fproduction_dept",addSSValQuote2((String)row.get("生产组织部门")));
        }

        if(StringUtils.isNotEmpty((String)row.get("HeTong_Time"))){
            // 合同签订时间
            sql.VALUES("fcontract_sign_time",addSSValQuote2((String)row.get("HeTong_Time")));
        }

        if(StringUtils.isNotEmpty((String)row.get("zlshbm"))){
            // 质量审核部门
            sql.VALUES("fquality_manage_dept",addSSValQuote2((String)row.get("zlshbm")));
        }

        if(StringUtils.isNotEmpty((String)row.get("kaifaxzbz"))){
            // 开发性质备注
            sql.VALUES("fdev_nature_remark",addSSValQuote2((String)row.get("kaifaxzbz")));
        }

        if(StringUtils.isNotEmpty((String)row.get("kaifaxz"))){
            // 开发性质
            sql.VALUES("fdev_nature",addSSValQuote2((String)row.get("kaifaxz")));
        }

        if(StringUtils.isNotEmpty((String)row.get("所属市场线分公司"))){
            // 所属市场线分公司
            sql.VALUES("fmarket_manager_dept",addSSValQuote2((String)row.get("所属市场线分公司")));
        }


        return  sql.toString();
    }

    public String doSyncUpdateRowData(Map<String,Object> row, Map<String,String> bm,Map<String,Object> productMap){
        SQL sql = new SQL();
        sql.UPDATE(projectSyncConfig.getTableName());

        sql.SET("fname = " + addSSValQuote2((String)row.get("name")));

        if(StringUtils.isNotEmpty((String)row.get("一级外委"))){
            String outter = (String)row.get("一级外委");
            if(outter.equals("是")){
                sql.SET("foutter = " + addSSValQuote2("1"));
            }else {
                sql.SET("foutter = " + addSSValQuote2("0"));
            }
        }

        if(StringUtils.isNotEmpty((String)row.get("一级外委管理部门"))){

            sql.SET("foutter_manage_dept = " + addSSValQuote2((String)row.get("一级外委管理部门")));
        }

        if(StringUtils.isNotEmpty((String)row.get("产品类型"))){
            // 产品类型
            sql.SET("fproduct_type = " + addSSValQuote2((String)row.get("产品类型")));
            // 产品名称
            sql.SET("fproduct_type_name =" + addSSValQuote2((String)row.get("产品类型")));
            // 产品类型编码
            sql.SET("fproduct_type_number =" + addSSValQuote2((String)productMap.get((String)row.get("产品类型"))));
        }

        if(StringUtils.isNotEmpty((String)row.get("客户一"))){
            // 签约单位
            sql.SET("fcontract_comp =" + addSSValQuote2((String)row.get("客户一")));
        }

        if(StringUtils.isNotEmpty((String)row.get("客户二"))){
            // 建设管理单位
            sql.SET("fbuild_manage_comp =" + addSSValQuote2((String)row.get("客户二")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目经理"))){
            // 项目经理名称
            sql.SET("fproject_manager_name =" + addSSValQuote2((String)row.get("项目经理")));
        }

        if(StringUtils.isNotEmpty((String)row.get("BigClientLeaderName"))){
            // 大客户分管领导名称
            sql.SET("fbig_customer_leader_name =" + addSSValQuote2((String)row.get("BigClientLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("ProductLeaderName"))){
            // 版块分管领导名称
            sql.SET("fproduct_type_leader_name =" + addSSValQuote2((String)row.get("ProductLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("DistrictLeaderName"))){
            // 片区分管领导名称
            sql.SET("fdistrict_leader_name =" + addSSValQuote2((String)row.get("DistrictLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("ProfessionalCenterName"))){
            // 专业中心
            sql.SET("fbusiness_center =" + addSSValQuote2((String)row.get("ProfessionalCenterName")));

            // 专业中心负责人 id
            String msId =  bm.get((String)row.get("ProfessionalCenterName"));

            SQL hrSql = new SQL();
            hrSql.SELECT("姓名");
            hrSql.FROM("[个人基本信息]");
            hrSql.WHERE("ID =" + addSSValQuote2(msId));
            List<Map<String,Object>> hrList = syncHelper.oldHrQuery(hrSql.toString());

            if(hrList != null && hrList.size()>0){
                sql.SET("fbusiness_center_leader_name =" + addSSValQuote2((String)hrList.get(0).get("姓名")));
            }
        }

        if(StringUtils.isNotEmpty((String)row.get("ProdLeaderName"))){
            // 生产决策分管领导
            sql.SET("fproduction_making_leader =" + addSSValQuote2((String)row.get("ProdLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("MarketLeaderName"))){
            // 市场决策分管领导
            sql.SET("fmarket_making_leader =" + addSSValQuote2((String)row.get("MarketLeaderName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("DevelopMainDeptName"))){
            // 开发主导部门
            sql.SET("fdominated_dept =" + addSSValQuote2((String)row.get("DevelopMainDeptName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("DevelopMainPersonName"))){
            // 开发主导人
            sql.SET("fdominated_person =" + addSSValQuote2((String)row.get("DevelopMainPersonName")));
        }

        if(StringUtils.isNotEmpty((String)row.get("所属分区"))){
            // 所在区域
            sql.SET("fregion =" + addSSValQuote2((String)row.get("所属分区")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目分级"))){
            // 项目分级
            sql.SET("fpro_classification =" + addSSValQuote2((String)row.get("项目分级")));
        }

//        if(StringUtils.isNotEmpty((String)row.get("项目阶段"))){
//            // 项目阶段
//            sql.SET("fpro_phase =" + addSSValQuote2((String)row.get("项目阶段")));
//        }

        if(StringUtils.isNotEmpty((String)row.get("产值核算合同额"))){

            // 产值核算合同额
            sql.SET("foutput_balance_amount =" + addSSValQuote2((String)row.get("产值核算合同额")));
        }

        if(StringUtils.isNotEmpty((String)row.get("签约合同额"))){
            // 签约合同额
            sql.SET("fsign_contract_amount =" + addSSValQuote2((String)row.get("签约合同额")));
        }
//
        if(StringUtils.isNotEmpty((String)row.get("回款核算合同额"))){
            // 回款核算合同额
            sql.SET("freceive_balance_amount =" + addSSValQuote2((String)row.get("回款核算合同额")));
        }

        if(StringUtils.isNotEmpty((String)row.get("内部结算手续"))){
            // 内部结算手续
            sql.SET("finside_balance =" + addSSValQuote2((String)row.get("内部结算手续")));
        }

        if(StringUtils.isNotEmpty((String)row.get("外部结算手续"))){
            // 外部结算手续
            sql.SET("fextemal_balance =" + addSSValQuote2((String)row.get("外部结算手续")));
        }

//        if(StringUtils.isNotEmpty((String)row.get("结算合同额"))){
//            // 结算合同额
//            sql.SET("fbalance_contract_amount =" + addSSValQuote2((String)row.get("结算合同额")));
//        }

//        if(StringUtils.isNotEmpty((String)row.get("可回款"))){
//
//            // 可汇款金额
//            sql.SET("freceivable_income =" + addSSValQuote2((String)row.get("可回款")));
//        }
//
//        if(StringUtils.isNotEmpty((String)row.get("已回款"))){
//            // 已回款金额
//            sql.SET("freceived_income =" + addSSValQuote2((String)row.get("已回款")));
//        }
//        if(StringUtils.isNotEmpty((String)row.get("待回款"))){
//            // 待回款金额
//            sql.SET("fremainder =" + addSSValQuote2((String)row.get("待回款")));
//        }

        if(StringUtils.isNotEmpty((String)row.get("合同形式"))){
            // 合同形式
            sql.SET("fcontract_shape =" + addSSValQuote2((String)row.get("合同形式")));
        }

        if(StringUtils.isNotEmpty((String)row.get("项目立项时间"))){
            // 项目立项时间
            sql.SET("fpro_approve_time =" + addSSValQuote2((String)row.get("项目立项时间")));
        }

        if(StringUtils.isNotEmpty((String)row.get("所在省市"))){
            // 所在省市
            sql.SET("flocation_city =" + addSSValQuote2((String)row.get("所在省市")));
        }

//        if(StringUtils.isNotEmpty((String)row.get("合同付款条款"))){
//            // 合同付款条款
//            sql.SET("fcontract_payment =" + addSSValQuote2((String)row.get("合同付款条款")));
//        }
//
//        if(StringUtils.isNotEmpty((String)row.get("kaifahte"))){
//            // 开发合同额
//            sql.SET("fdev_contract_amount =" + addSSValQuote2((String)row.get("kaifahte")));
//        }
//
//        if(StringUtils.isNotEmpty((String)row.get("zdprice"))){
//            // 暂定金额
//            sql.SET("fprovisional_amount =" + addSSValQuote2((String)row.get("zdprice")));
//        }

        if(StringUtils.isNotEmpty((String)row.get("合同价格类型"))){
            // 合同价格类型
            sql.SET("fcontract_price_type =" + addSSValQuote2((String)row.get("合同价格类型")));
        }

        if(StringUtils.isNotEmpty((String)row.get("Prj_BH_old"))){
            // 旧项目编号
            sql.SET("fold_pro_number =" + addSSValQuote2((String)row.get("Prj_BH_old")));
        }

//        if(StringUtils.isNotEmpty((String)row.get("jianan"))){
//            // 建安费
//            sql.SET("fconstruction_expenses =" + addSSValQuote2((String)row.get("jianan")));
//        }

//        if(StringUtils.isNotEmpty((String)row.get("jizhun"))){
//            // 基准服务费
//            sql.SET("fbenchmark_service_charge =" + addSSValQuote2((String)row.get("jizhun")));
//        }

        if(StringUtils.isNotEmpty((String)row.get("生产管理部门"))){
            // 生产管理部门名称
            sql.SET("fmanage_department_name =" + addSSValQuote2((String)row.get("生产管理部门")));
        }

        if(StringUtils.isNotEmpty((String)row.get("生产管理部门负责人"))){
            // 生产管理部门负责人名称
            sql.SET("fmanage_department_leader_name =" + addSSValQuote2((String)row.get("生产管理部门负责人")));
        }

        if(StringUtils.isNotEmpty((String)row.get("生产组织部门"))){
            // 生产组织部门
            sql.SET("fproduction_dept =" + addSSValQuote2((String)row.get("生产组织部门")));
        }

        if(StringUtils.isNotEmpty((String)row.get("HeTong_Time"))){
            // 合同签订时间
            sql.SET("fcontract_sign_time =" + addSSValQuote2((String)row.get("HeTong_Time")));
        }

        if(StringUtils.isNotEmpty((String)row.get("zlshbm"))){
            // 质量审核部门
            sql.SET("fquality_manage_dept =" + addSSValQuote2((String)row.get("zlshbm")));
        }

        if(StringUtils.isNotEmpty((String)row.get("kaifaxzbz"))){
            // 开发性质备注
            sql.SET("fdev_nature_remark =" + addSSValQuote2((String)row.get("kaifaxzbz")));
        }

        if(StringUtils.isNotEmpty((String)row.get("kaifaxz"))){
            // 开发性质
            sql.SET("fdev_nature =" + addSSValQuote2((String)row.get("kaifaxz")));
        }

        if(StringUtils.isNotEmpty((String)row.get("所属市场线分公司"))){
            // 所属市场线分公司
            sql.SET("fmarket_manager_dept =" + addSSValQuote2((String)row.get("所属市场线分公司")));
        }
        sql.SET("fupdate_time =" +addSSValQuote2(formatter.format(LocalDateTime.now()))  );

        sql.WHERE("fnumber =" + addSSValQuote2((String) row.get("项目编号")));
        return  sql.toString();
    }

    /**
     * 检查字段是否修改
     * @param feePro
     * @param row
     * @return
     */
    private boolean checkProjectObj(Map<String,Object> feePro, Map<String,Object> row,Map<String,Object> productMap){


        if(!checkObject(feePro.get("fname"),row.get("name"))){
            logger.info("fname 不同："+ feePro.get("fname") +"--" + row.get("name"));
            return true;
        }

        if(!checkObject(feePro.get("foutter_manage_dept"),row.get("一级外委管理部门"))){
            logger.info("foutter_manage_dept 不同："+ feePro.get("foutter_manage_dept") +"--" + row.get("一级外委管理部门"));
            return true;
        }
       if(!checkObject(feePro.get("fproduct_type"),row.get("产品类型"))){
            logger.info("fproduct_type 不同："+ feePro.get("fproduct_type") +"--" + row.get("产品类型"));

            return true;
        }
        if(!checkObject(feePro.get("fproduct_type_number"),productMap.get((String)row.get("产品类型")))){
            logger.info("fproduct_type_number 不同："+ feePro.get("fproduct_type_number") +"--" + row.get("产品类型"));

            return true;
        }
       if(!checkObject(feePro.get("fcontract_comp"),row.get("客户一"))){
            logger.info("fcontract_comp 不同："+ feePro.get("fcontract_comp") +"--" + row.get("客户一"));

            return true;
        }
       if(!checkObject(feePro.get("fbuild_manage_comp"),row.get("客户二"))){
            logger.info("fbuild_manage_comp 不同："+ feePro.get("fbuild_manage_comp") +"--" + row.get("客户二"));
            return true;
        }
       if(!checkObject(feePro.get("fproject_manager_name"),row.get("项目经理"))){
           logger.info((String) feePro.get("fnumber"));
            logger.info("fproject_manager_name 不同："+ feePro.get("fproject_manager_name") +"--" + row.get("项目经理"));
            return true;
        }
       if(!checkObject(feePro.get("fbig_customer_leader_name"),row.get("BigClientLeaderName"))){
            logger.info("fbig_customer_leader_name 不同："+ feePro.get("fbig_customer_leader_name") +"--" + row.get("BigClientLeaderName"));
            return true;
        }
       if(!checkObject(feePro.get("fproduct_type_leader_name"),row.get("ProductLeaderName"))){
            logger.info("不同："+ 1);
            return true;
        }

//         片区分管领导名称
       if(!checkObject(feePro.get("fdistrict_leader_name"),row.get("DistrictLeaderName"))){
            logger.info("不同："+ 1);
            return true;
        }

            // 专业中心
       if(!checkObject(feePro.get("fbusiness_center"),row.get("ProfessionalCenterName"))){
            logger.info("不同："+ 2);
            return true;
        }

            // 生产决策分管领导
       if(!checkObject(feePro.get("fproduction_making_leader"),row.get("ProdLeaderName"))){
            logger.info("不同："+ 3);
            return true;
        }

            // 市场决策分管领导
       if(!checkObject(feePro.get("fmarket_making_leader"),row.get("MarketLeaderName"))){
            logger.info("不同："+ 4);
            return true;
        }

            // 开发主导部门
       if(!checkObject(feePro.get("fdominated_dept"),row.get("DevelopMainDeptName"))){
            logger.info("不同："+ 5);
            return true;
        }

            // 开发主导人
       if(!checkObject(feePro.get("fdominated_person"),row.get("DevelopMainPersonName"))){
            logger.info("不同："+ 6);
            return true;
        }

            // 所在区域
       if(!checkObject(feePro.get("fregion"),row.get("所属分区"))){
            logger.info("不同："+ 7);
            return true;
        }

            // 项目分级
       if(!checkObject(feePro.get("fpro_classification"),row.get("项目分级"))){
            logger.info("不同："+ 8);
            return true;
        }


            // 内部结算手续
       if(!checkObject(feePro.get("finside_balance"),row.get("内部结算手续"))){
            logger.info("不同："+ 9);
            return true;
        }

            // 外部结算手续
       if(!checkObject(feePro.get("fextemal_balance"),row.get("外部结算手续"))){
            logger.info("不同："+ 10);
            return true;
        }

            // 合同形式
       if(!checkObject(feePro.get("fcontract_shape"),row.get("合同形式"))){
            logger.info("不同："+ 11);
            return true;
        }

            // 项目立项时间
//       if(!checkObject(feePro.get("fpro_approve_time"),row.get("项目立项时间"))){
//            return true;
//        }

            // 所在省市
       if(!checkObject(feePro.get("flocation_city"),row.get("所在省市"))){
            logger.info("不同："+ 12);
            return true;
        }


            // 合同价格类型
       if(!checkObject(feePro.get("fcontract_price_type"),row.get("合同价格类型"))){
            logger.info("不同："+ 13);
            return true;
        }

            // 旧项目编号
       if(!checkObject(feePro.get("fold_pro_number"),row.get("Prj_BH_old"))){
            logger.info("不同："+ 14);
            return true;
        }


            // 生产管理部门名称
       if(!checkObject(feePro.get("fmanage_department_name"),row.get("生产管理部门"))){
            logger.info("不同："+ 15);
            return true;
        }

            // 生产管理部门负责人名称
       if(!checkObject(feePro.get("fmanage_department_leader_name"),row.get("生产管理部门负责人"))){
            logger.info("不同："+ 16);
            return true;
        }

            // 生产组织部门
       if(!checkObject(feePro.get("fproduction_dept"),row.get("生产组织部门"))){
            logger.info("不同："+ 16);
            return true;
        }

            // 合同签订时间
//       if(!checkObject(feePro.get("fcontract_sign_time"),row.get("HeTong_Time"))){
//            return true;
//        }

            // 质量审核部门
       if(!checkObject(feePro.get("fquality_manage_dept"),row.get("zlshbm"))){
            logger.info("不同："+ 17);
            return true;
        }

            // 开发性质备注
       if(!checkObject(feePro.get("fdev_nature_remark"),row.get("kaifaxzbz"))){
            logger.info("不同："+ 18);
            return true;
        }

            // 开发性质
       if(!checkObject(feePro.get("fdev_nature"),row.get("kaifaxz"))){
            logger.info("不同："+ 19);
            return true;
        }

            // 所属市场线分公司
       if(!checkObject(feePro.get("fmarket_manager_dept"),row.get("所属市场线分公司"))){
            logger.info("不同："+ 20);
            return true;
        }

        // 回款核算合同额
        if(!checkObject(feePro.get("freceive_balance_amount"),row.get("回款核算合同额"))){
            logger.info("回款核算合同额不同");
            return true;
        }

        // 产值核算合同额
        if(!checkObject(feePro.get("foutput_balance_amount"),row.get("产值核算合同额"))){
            logger.info("产值核算合同额不同");
            return true;
        }

        // 签约合同额
        if(!checkObject(feePro.get("fsign_contract_amount"),row.get("签约合同额"))){
            logger.info("签约合同额");
            return true;
        }
        return false;
    }

    /**
     * 检查两个对象是否相同
     * @return
     */
    public boolean checkObject(Object object1,Object object2){
        if(object1== null && object2 == null ){
            return true;
        }if(object1 == null && object2 != null && StringUtils.isNotEmpty(object2.toString())){
            return false;
        }

        if(StringUtils.isEmpty((String)object2)){
            return true;
        }

        try{

            Float f1 = Float.parseFloat(object1.toString());
            Float f2 = Float.parseFloat(object2.toString());
            if(Math.abs(f1-f2)==0 ){
                return true;
            }
        }catch(NumberFormatException e){
            // 不为数值
            if(object2.equals(object1)){
                return true;
            }
        }

        return false;
    }
}
