package com.szewec.data.hr.util;

import com.szewec.data.hr.context.AppContext;
import com.szewec.data.hr.service.SyncDataSource;
import org.springframework.dao.EmptyResultDataAccessException;

import java.util.Map;

public class ConvertUtils {


    public static String convertCode2Name(String code) {
        if("201".equals(code)){
            return "公司领导";
        }
        if("202".equals(code)){
            return "总经理助理";
        }if("203".equals(code)){
            return "公司专家";
        }if("204".equals(code)){
            return "产品总监";
        }if("205".equals(code)){
            return "部门正职";
        }
        if("206".equals(code)){
            return "部门副职";
        }
        if("207".equals(code)){
            return "组长";
        }
        if("208".equals(code)){
            return "副组长(主持工作)";
        }
        if("209".equals(code)){
            return "副组长";
        }
        if("210".equals(code)){
            return "小组负责人";
        }
        if("211".equals(code)){
            return "部门副职(主持工作)";
        }
        if("221".equals(code)){
            return "在职";
        }
        if("222".equals(code)){
            return "离职";
        }
        if("223".equals(code)){
            return "退休";
        }
        if ("224".equals(code)) {
            return "停薪留职";
        }
        if("241".equals(code)){
            return "职员";
        }
        // 2018-12-26 老系统流动人员映射为雇员 liyao
        if("242".equals(code)){
//            return "流动人员(Q)";
            return "雇员";
        }
        if("243".equals(code)){
            return "实习生";
        }
        if("244".equals(code)){
//            return "流动人员(W)";
            return "雇员";
        }
        if("245".equals(code)){
//            return "流动人员";
            return "雇员";
        }
        if("261".equals(code)){
            return "未婚";
        }
        if("262".equals(code)){
            return "已婚";
        }
        if("263".equals(code)){
            return "离异";
        }

        if("264".equals(code)){
            return "丧偶";
        }
        if("281".equals(code)){
            return "顾问公司";
        }
        if("282".equals(code)){
            return "检测公司";
        }
        if("283".equals(code)){
            return "信息公司";
        }
        if("301".equals(code)){
            return "工程技术";
        }
        if("302".equals(code)){
            return "咨询服务";
        }
        if("303".equals(code)){
            return "技术(IT)";
        }

        if("304".equals(code)){
            return "财务";
        }
        if("305".equals(code)){
            return "人力资源";
        }
        if("306".equals(code)){
            return "行政";
        }
        if("307".equals(code)){
            return "管理";
        }
        if("321".equals(code)){
            return "市场族";
        }
        if("322".equals(code)){
            return "职能族";
        }
        if("323".equals(code)){
            return "土木族";
        }
        if("324".equals(code)){
            return "IT族";
        }
        if("341".equals(code)){
            return "M+T";
        }
        if("342".equals(code)){
            return "M";
        }
        if("343".equals(code)){
            return "T";
        }
        if("401".equals(code)){
            return "汉族";
        }
        if("402".equals(code)){
            return "蒙古族";
        }
        if("403".equals(code)){
            return "回族";
        }
        if("404".equals(code)){
            return "藏族";
        }
        if("405".equals(code)){
            return "维吾尔族";
        }
        if("406".equals(code)){
            return "苗族";
        }
        if("407".equals(code)){
            return "彝族";
        }
        if("408".equals(code)){
            return "壮族";
        }
        if("409".equals(code)){
            return "布依族";
        }

        if("410".equals(code)){
            return "朝鲜族";
        }
        if("411".equals(code)){
            return "满族";
        }
        if("412".equals(code)){
            return "侗族";
        }
        if("413".equals(code)){
            return "瑶族";
        }
        if("414".equals(code)){
            return "白族";
        }
        if("415".equals(code)){
            return "土家族";
        }
        if("416".equals(code)){
            return "哈尼族";
        }
        if("417".equals(code)){
            return "哈萨克族";
        }
        if("418".equals(code)){
            return "傣族";
        }
        if("419".equals(code)){
            return "黎族";
        }

        if("420".equals(code)){
            return "傈僳族";
        }
        if("421".equals(code)){
            return "佤族";
        }
        if("422".equals(code)){
            return "畲族";
        }
        if("423".equals(code)){
            return "高山族";
        }
        if("424".equals(code)){
            return "拉祜族";
        }
        if("425".equals(code)){
            return "水族";
        }
        if("426".equals(code)){
            return "东乡族";
        }
        if("427".equals(code)){
            return "纳西族";
        }
        if("428".equals(code)){
            return "景颇族";
        }
        if("429".equals(code)){
            return "柯尔克孜族";
        }
        if("430".equals(code)){
            return "土族";
        }
        if("431".equals(code)){
            return "达斡尔族";
        }if("432".equals(code)){
            return "仫佬族";
        }
        if("433".equals(code)){
            return "羌族";
        }
        if("434".equals(code)){
            return "布朗族";
        }
        if("435".equals(code)){
            return "撒拉族";
        }
        if("436".equals(code)){
            return "毛难族";
        }
        if("437".equals(code)){
            return "仡佬族";
        }
        if("438".equals(code)){
            return "锡伯族";
        }
        if("439".equals(code)){
            return "阿昌族";
        }
        if("440".equals(code)){
            return "普米族";
        }
        if("441".equals(code)){
            return "塔吉克族";
        }
        if("442".equals(code)){
            return "怒族";
        }
        if("443".equals(code)){
            return "乌孜别克族";
        }
        if("444".equals(code)){
            return "俄罗斯族";
        }
        if("445".equals(code)){
            return "鄂温克族";
        }
        if("446".equals(code)){
            return "崩龙族";
        }
        if("447".equals(code)){
            return "保安族";
        }if("448".equals(code)){
            return "裕固族";
        }
        if("449".equals(code)){
            return "京族";
        }
        if("450".equals(code)){
            return "塔塔尔族";
        }
        if("451".equals(code)){
            return " 独龙族";
        }if("452".equals(code)){
            return " 鄂伦春族";
        }
        if("453".equals(code)){
            return " 赫哲族";
        }
        if("454".equals(code)){
            return " 门巴族";
        }
        if("455".equals(code)){
            return " 珞巴族";
        }
        if("456".equals(code)){
            return " 基诺族";
        }

        //2018-12-26 如果不是数字映射，则原值返回，比如
        //其他人员
        //劳务工
        return code;
    }


    public static String convertDep12Old(String code){
//        if ("612".equals(code)) {
//            return "83";
//        }
//        if ("501".equals(code)) {
//            return "102";
//        }
//        if ("306".equals(code)) {
//            return "32";
//        }
//        if ("405".equals(code)) {
//            return "151";
//        }
//        if ("305".equals(code)) {
//            return "144";
//        }
//        if ("602".equals(code)) {
//            return "117";
//        }
//        if ("610".equals(code)) {
//            return "145";
//        }
//        if ("607".equals(code)) {
//            return "148";
//        }
//        if ("204".equals(code)) {
//            return "95";
//        }
//        if ("102".equals(code)) {
//            return "127";
//        }
//        if ("301".equals(code)) {
//            return "109";
//        }
//        if ("304".equals(code)) {
//            return "143";
//        }
//        if ("403".equals(code)) {
//            return "128";
//        }
//        if ("206".equals(code)) {
//            return "131";
//        }
//        if ("404".equals(code)) {
//            return "129";
//        }
//        if ("402".equals(code)) {
//            return "113";
//        }
//        if ("701".equals(code)) {
//            return "142";
//        }
//        if ("504".equals(code)) {
//            return "105";
//        }
//        //2018-12-26 解决IT研发三部映射为IT研发二部问题 liyao
//        if ("503".equals(code)) {
//            return "104";
//        }
//        //
//
//        if ("205".equals(code)) {
//            return "100";
//        }
//        if ("611".equals(code)) {
//            return "130";
//        }
//        if ("606".equals(code)) {
//            return "122";
//        }
//        if ("609".equals(code)) {
//            return "123";
//        }
//        if ("605".equals(code)) {
//            return "119";
//        }
//        if ("202".equals(code)) {
//            return "2";
//        }
//        if ("505".equals(code)) {
//            return "106";
//        }
//        if ("203".equals(code)) {
//            return "99";
//        }
//        if ("201".equals(code)) {
//            return "98";
//        }
//        if ("207".equals(code)) {
//            return "101";
//        }
//        if ("101".equals(code)) {
//            return "1";
//        }
//        if ("603".equals(code)) {
//            return "120";
//        }
//        if ("601".equals(code)) {
//            return "116";
//        }
//        if ("103".equals(code)) {
//            return "150";
//        }
//        if ("502".equals(code)) {
//            return "103";
//        }
//        if ("604".equals(code)) {
//            return "121";
//        }
//        if ("608".equals(code)) {
//            return "147";
//        }
//        if ("303".equals(code)) {
//            return "111";
//        }
//        if ("401".equals(code)) {
//            return "112";
//        }
//        if ("302".equals(code)) {
//            return "110";
//        }
//        //区域公司
//        if ("801".equals(code)) {
//            return "133";
//        }
//
//        if ("1001".equals(code)) {
//            return "155";
//        }
//
//        if ("1101".equals(code)) {
//            return "158";
//        }
//
//        if ("1102".equals(code)) {
//            return "159";
//        }
//
//        if ("1103".equals(code)) {
//            return "160";
//        }
//
//        if ("1001".equals(code)) {
//            return "161";
//        }
//
//
//        if ("1201".equals(code)) {
//            return "162";
//        }
//
//
//        if ("407".equals(code)) {
//            return "162";
//        }
//
        String name = doConvertDep12Old(code);
        if (StringUtils.isNotEmpty(name)) {
            return name;
        } else {
            //4代表空
            return "4";
        }
    }

    public static String convertDep22Old(String code){

       return doConvertDep22Old(code);
//        if("201001".equals(code)){
//            return "241";
//        }
//        if("201002".equals(code)){
//            return "239";
//        }
//        if("201003".equals(code)){
//            return "240";
//        }
//        if("202001".equals(code)){
//            return "407";
//        }
//        if("202002".equals(code)){
//            return "324";
//        }
//        if("202003".equals(code)){
//            return "246";
//        }
//        if("202004".equals(code)){
//            return "325";
//        }
//        if("203001".equals(code)){
//            return "247";
//        }
//        if("203002".equals(code)){
//            return "326";
//        }
//        if("203003".equals(code)){
//            return "327";
//        }
//        if("204001".equals(code)){
//            return "328";
//        }
//        if("204002".equals(code)){
//            return "409";
//        }
//        if("204003".equals(code)){
//            return "251";
//        }
//        if("204004".equals(code)){
//            return "252";
//        }
//        if("205001".equals(code)){
//            return "254";
//        }
//        if("205002".equals(code)){
//            return "408";
//        }
//        if("205003".equals(code)){
//            return "255";
//        }
//        if("205004".equals(code)){
//            return "253";
//        }
//        if("301001".equals(code)){
//            return "256";
//        }
//        if("302008".equals(code)){
//            return "262";
//        }
//        if("302011".equals(code)){
//            return "258";
//        }
//        if("302012".equals(code)){
//            return "259";
//        }
//        if("303009".equals(code)){
//            return "263";
//        }
//        if("303010".equals(code)){
//            return "264";
//        }
//        if("303014".equals(code)){
//            return "253";
//        }
//        if("304002".equals(code)){
//            return "393";
//        }
//        if("304013".equals(code)){
//            return "394";
//        }
//        if("305006".equals(code)){
//            return "395";
//        }
//        if("305007".equals(code)){
//            return "396";
//        }
//        if("305015".equals(code)){
//            return "397";
//        }
//        if("403001".equals(code)){
//            return "389";
//        }
//        if("403002".equals(code)){
//            return "390";
//        }
//        if("403003".equals(code)){
//            return "391";
//        }
//        if("404001".equals(code)){
//            return "370";
//        }
//        if("404002".equals(code)){
//            return "371";
//        }
//        if("404003".equals(code)){
//            return "372";
//        }
//        if("501001".equals(code)){
//            return "386";
//        }
//        if("502002".equals(code)){
//            return "388";
//        }
//        if("503003".equals(code)){
//            return "387";
//        }
//        if("601001".equals(code)){
//            return "271";
//        }
//        if("601002".equals(code)){
//            return "272";
//        }
//        if("601003".equals(code)){
//            return "367";
//        }
//        if("602001".equals(code)){
//            return "400";
//        }
//        if("602002".equals(code)){
//            return "273";
//        }
//        if("602003".equals(code)){
//            return "274";
//        }
//        if("602004".equals(code)){
//            return "427";
//        }
//        if("602005".equals(code)){
//            return "336";
//        }
//        if("602006".equals(code)){
//            return "276";
//        }
//        if("602007".equals(code)){
//            return "277";
//        }
//        if("603001".equals(code)){
//            return "296";
//        }
//        if("603002".equals(code)){
//            return "297";
//        }
//        if("603003".equals(code)){
//            return "348";
//        }
//        if("603004".equals(code)){
//            return "349";
//        }
//        if("604001".equals(code)){
//            return "298";
//        }
//        if("604002".equals(code)){
//            return "299";
//        }
//        if("604003".equals(code)){
//            return "300";
//        }
//        if("604004".equals(code)){
//            return "402";
//        }
//        if("605001".equals(code)){
//            return "280";
//        }
//        if("605002".equals(code)){
//            return "281";
//        }
//        if("605003".equals(code)){
//            return "282";
//        }
//        if("605004".equals(code)){
//            return "283";
//        }
//        if("605005".equals(code)){
//            return "285";
//        }
//        if("605006".equals(code)){
//            return "287";
//        }
//        if("605009".equals(code)){
//            return "289";
//        }
//        if("605010".equals(code)){
//            return "290";
//        }
//        if("605013".equals(code)){
//            return "293";
//        }
//        if("605014".equals(code)){
//            return "294";
//        }
//        if("605015".equals(code)){
//            return "339";
//        }
//        if("605016".equals(code)){
//            return "350";
//        }
//        if("605017".equals(code)){
//            return "351";
//        }
//        if("605018".equals(code)){
//            return "352";
//        }
//        if("605099".equals(code)){
//            return "286";
//        }
//        if("606003".equals(code)){
//            return "305";
//        }
//        if("606005".equals(code)){
//            return "307";
//        }
//        if("606006".equals(code)){
//            return "308";
//        }
//        if("606007".equals(code)){
//            return "309";
//        }
//        if("606098".equals(code)){
//            return "401";
//        }
//        if("606099".equals(code)){
//            return "301";
//        }
//        if("607001".equals(code)){
//            return "412";
//        }
//        if("607002".equals(code)){
//            return "413";
//        }
//        if("607004".equals(code)){
//            return "414";
//        }
//        if("607009".equals(code)){
//            return "416";
//        }
//        if("607012".equals(code)){
//            return "418";
//        }
//        if("608001".equals(code)){
//            return "410";
//        }
//        if("608002".equals(code)){
//            return "411";
//        }
//        if("609001".equals(code)){
//            return "317";
//        }
//        if("609002".equals(code)){
//            return "318";
//        }
//        if("610003".equals(code)){
//            return "405";
//        }
//        if("610004".equals(code)){
//            return "406";
//        }
//        if("611001".equals(code)){
//            return "344";
//        }
//        if("611002".equals(code)){
//            return "343";
//        }
//        if("611003".equals(code)){
//            return "362";
//        }
//        if("612001".equals(code)){
//            return "403";
//        }
//        if("612002".equals(code)){
//            return "404";
//        }
//        if("701001".equals(code)){
//            return "384";
//        }
//        if("701002".equals(code)){
//            return "385";
//        }
//        return null;
    }


    public static String convertGender2Old(String val) {
        return "0".equals(val) ? "男" : "女";
    }


    public static boolean isEmpLeavlStatus(String val) {
        return val.equals("222");
    }


    /**
     * 根据新部门二编号查询新部门一编号，部门二名称
     * 将新部门一编号转换为老系统部门一编号
     * 根据老系统部门一编号和部门二名称查询老系统部门二编号
     * @param dep2Code
     * @return
     */
    private static String doConvertDep22Old(String dep2Code) {
        //dep2Code 中文
        if (StringUtils.isContainChinese(dep2Code)) {
            return null;
        }

        SyncDataSource syncDataSource = AppContext.getBean(SyncDataSource.class);
        String sql = "select fdept_number,fdept_name,fpdept_number from t_hr_dept where fdept_number =" + SqlUtils.addSQuote(dep2Code);
        Map<String, Object> row = syncDataSource.getNewDataSource().queryForMap(sql);

        String dep1Number = (String) row.get("fpdept_number");
        //转回老部门一编号
        dep1Number = convertDep12Old(dep1Number);
        String dep2Name = (String) row.get("fdept_name");
        String queryOldSql = "SELECT top(1) ID from 部门二 where [隶属一部门ID] = " + SqlUtils.addSQuote(dep1Number) + "and 名称 = " + SqlUtils.addSQuote(dep2Name) + " and isshow = " + SqlUtils.addSQuote("1");
        try {
            return syncDataSource.getOldDataSource().queryForObject(queryOldSql, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }


    private static String doConvertDep12Old(String dep1Code) {
        if ("待录用人员".equals(dep1Code)) {
            return null;
        }

        SyncDataSource syncDataSource = AppContext.getBean(SyncDataSource.class);
        Map<String, Object> row;
        try {
            String sql = "select fdept_number,fdept_name,fpdept_number from t_hr_dept where fdept_number =" + SqlUtils.addSQuote(dep1Code);
            row = syncDataSource.getNewDataSource().queryForMap(sql);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }

        String dep1Name = (String) row.get("fdept_name");

        String queryOldSql = "SELECT top(1) ID from 部门一 where  名称 = " + SqlUtils.addSQuote(dep1Name);
        try {
            return syncDataSource.getOldDataSource().queryForObject(queryOldSql, String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }
}
