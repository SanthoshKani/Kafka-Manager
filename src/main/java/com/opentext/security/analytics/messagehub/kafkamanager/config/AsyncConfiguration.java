// Copyright 2026 Open Text
package com.opentext.security.analytics.messagehub.kafkamanager.config;

import com.opentext.security.analytics.messagehub.kafkamanager.utils.ExceptionHandlingAsyncTaskExecutor;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tag;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadPoolExecutor;
import org.jspecify.annotations.Nullable;
import org.springframework.aop.interceptor.AsyncUncaughtExceptionHandler;
import org.springframework.aop.interceptor.SimpleAsyncUncaughtExceptionHandler;
import org.springframework.boot.autoconfigure.task.TaskExecutionProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.annotation.AsyncConfigurer;
import org.springframework.scheduling.annotation.EnableAsync;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

@Configuration
@EnableAsync
@EnableScheduling
class AsyncConfiguration implements AsyncConfigurer {
    private static final String EXECUTOR_NAME = "async";
    private static final String TAG_NAME = "name";

    private final TaskExecutionProperties taskExecutionProperties;
    private final MeterRegistry meterRegistry;

    AsyncConfiguration(TaskExecutionProperties taskExecutionProperties, MeterRegistry meterRegistry) {
        this.taskExecutionProperties = taskExecutionProperties;
        this.meterRegistry = meterRegistry;
    }

    @Override
    public @Nullable Executor getAsyncExecutor() {
        var executor = new ThreadPoolTaskExecutor();
        executor.setCorePoolSize(taskExecutionProperties.getPool().getCoreSize());
        executor.setMaxPoolSize(taskExecutionProperties.getPool().getMaxSize());
        executor.setQueueCapacity(taskExecutionProperties.getPool().getQueueCapacity());
        executor.setThreadNamePrefix(taskExecutionProperties.getThreadNamePrefix());
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(60);
        executor.initialize();

        // Register thread pool metrics
        // ThreadPoolExecutor is managed by Spring, not closed here
        meterRegistry.gauge(
                "executor.active",
                java.util.List.of(Tag.of(TAG_NAME, EXECUTOR_NAME)),
                executor.getThreadPoolExecutor(),
                ThreadPoolExecutor::getActiveCount);
        meterRegistry.gauge(
                "executor.pool.size",
                java.util.List.of(Tag.of(TAG_NAME, EXECUTOR_NAME)),
                executor.getThreadPoolExecutor(),
                ThreadPoolExecutor::getPoolSize);
        meterRegistry.gauge(
                "executor.queue.size",
                java.util.List.of(Tag.of(TAG_NAME, EXECUTOR_NAME)),
                executor.getThreadPoolExecutor(),
                e -> e.getQueue().size());
        meterRegistry.gauge(
                "executor.completed",
                java.util.List.of(Tag.of(TAG_NAME, EXECUTOR_NAME)),
                executor.getThreadPoolExecutor(),
                ThreadPoolExecutor::getCompletedTaskCount);

        return new ExceptionHandlingAsyncTaskExecutor(executor);
    }

    @Override
    public @Nullable AsyncUncaughtExceptionHandler getAsyncUncaughtExceptionHandler() {
        return new SimpleAsyncUncaughtExceptionHandler();
    }
}
