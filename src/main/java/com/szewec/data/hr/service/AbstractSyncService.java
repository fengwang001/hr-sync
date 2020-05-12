package com.szewec.data.hr.service;

import com.szewec.data.hr.config.SyncTableProperties;

import java.util.List;
import java.util.Map;

/**
 * 抽象同步服务
 */
public abstract class AbstractSyncService implements SyncService {


    /**
     * 同步新增数据
     * @param syncTableProperties
     */
    public abstract void syncNewData(SyncTableProperties syncTableProperties);


    /**
     * 同步新增数据
     * @param syncTableProperties
     * @param rows
     */
    public abstract void doSyncNewData(SyncTableProperties syncTableProperties, List<Map<String, Object>> rows);


    /**
     * 同步更新数据
     * @param syncTableProperties
     */
    public abstract void syncUpdateData(SyncTableProperties syncTableProperties);

    /**
     * 同步更新数据
     * @param syncTableProperties
     * @param rows
     */
    public abstract void doSyncUpdateData(SyncTableProperties syncTableProperties, List<Map<String, Object>> rows);


}
