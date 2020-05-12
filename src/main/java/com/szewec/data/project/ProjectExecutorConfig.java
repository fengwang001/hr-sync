package com.szewec.data.project;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;

/**
 * @ClassName ProjectExecutorConfig
 * @Author wangfeng
 * @Date 2020/3/20 15:09
 * @Version 1.0
 **/
@Configuration
@EnableAsync
public class ProjectExecutorConfig {

    /** Set the ThreadPoolExecutor's core pool size. */
    private int corePoolSize = 8;
    /** Set the ThreadPoolExecutor's maximum pool size. */
    private int maxPoolSize = 8;
    /** Set the capacity for the ThreadPoolExecutor's BlockingQueue. */
    private int queueCapacity = 1000;

    @Bean("projectAsync")
    public Executor employeeAsync() {
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(corePoolSize);
        executor.setMaxPoolSize(maxPoolSize);
        executor.setQueueCapacity(queueCapacity);
        executor.setThreadNamePrefix("ProjectExecutor-");

        // rejection-policy：当pool已经达到max size的时候，如何处理新任务
        // CALLER_RUNS：不在新线程中执行任务，而是有调用者所在的线程来执行
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.initialize();
        return executor;
    }
}
