package com.szewec.data.hr.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "data.sync")
public class SyncConfig {

    private List<SyncTableProperties> tables;

    /**
     * 是否启动OA同步
     */
    private boolean enableOASync;

    public boolean isEnableOASync() {
        return enableOASync;
    }

    public void setEnableOASync(boolean enableOASync) {
        this.enableOASync = enableOASync;
    }

    public List<SyncTableProperties> getTables() {
        return tables;
    }

    public void setTables(List<SyncTableProperties> tables) {
        this.tables = tables;
    }
}
