package com.szewec.data.hr.handler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class OldHrLogExecutor implements Executor {

    private static final Logger LOGGER = LoggerFactory.getLogger(OldHrLogExecutor.class);

    @Override
    public void execute(String sql) {
        LOGGER.info(sql);
    }
}
