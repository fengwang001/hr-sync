package com.szewec.data;

import com.szewec.data.hr.config.ProjectSyncConfig;
import com.szewec.data.hr.config.SyncConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableConfigurationProperties({SyncConfig.class, ProjectSyncConfig.class})
@EnableScheduling
@EnableAsync
public class DataApplication {

    private static final Logger LOGGER = LoggerFactory.getLogger(DataApplication.class);

    public static void main(String[] args) {
        SpringApplication.run(DataApplication.class, args);
    }


}

