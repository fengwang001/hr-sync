package com.szewec.data.hr.service;

import com.szewec.data.hr.util.SqlUtils;
import org.apache.ibatis.jdbc.SQL;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.stereotype.Component;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@Component
public class SyncHelper {

    @Autowired
    private SyncDataSource dataSource;

    public String getEmpNameFromNewSys(String newEmpId) {
        SQL sql = new SQL();
        sql.SELECT("fname");
        sql.FROM("t_hr_employee");
        sql.WHERE("fid =" + SqlUtils.addSQuote(newEmpId));

        try {
            return dataSource.getNewDataSource().queryForObject(sql.toString(), String.class);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }


    public List<Map<String, Object>> query(String sql) {
        List<Map<String, Object>> rows = dataSource.getNewDataSource().query(sql, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet resultSet, int i) throws SQLException {
                Map<String, Object> map = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                    String columnName = metaData.getColumnName(j);
                    String columnValue = resultSet.getString(columnName);

                    map.put(columnName, columnValue);
                }
                return map;
            }
        });
        return rows;
    }

    /**
     * 获取核算系统数据
     * @param sql
     * @return
     */
    public List<Map<String, Object>> projectQuery(String sql) {
        List<Map<String, Object>> rows = dataSource.getOldProjectDataSource().query(sql, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet resultSet, int i) throws SQLException {
                Map<String, Object> map = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                    String columnName = metaData.getColumnName(j);
                    String columnValue = resultSet.getString(columnName);

                    map.put(columnName, columnValue);
                }
                return map;
            }
        });
        return rows;
    }

    public List<Map<String, Object>> oldHrQuery(String sql) {
        List<Map<String, Object>> rows = dataSource.getOldDataSource().query(sql, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet resultSet, int i) throws SQLException {
                Map<String, Object> map = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                    String columnName = metaData.getColumnName(j);
                    String columnValue = resultSet.getString(columnName);

                    map.put(columnName, columnValue);
                }
                return map;
            }
        });
        return rows;
    }

    public List<Map<String, Object>> newProject(String sql) {
        List<Map<String, Object>> rows = dataSource.getNewFeeProjectDataSource().query(sql, new RowMapper<Map<String, Object>>() {
            @Override
            public Map<String, Object> mapRow(ResultSet resultSet, int i) throws SQLException {
                Map<String, Object> map = new HashMap<>();
                ResultSetMetaData metaData = resultSet.getMetaData();
                for (int j = 1; j <= metaData.getColumnCount(); j++) {
                    String columnName = metaData.getColumnName(j);
                    String columnValue = resultSet.getString(columnName);

                    map.put(columnName, columnValue);
                }
                return map;
            }
        });
        return rows;
    }
}
