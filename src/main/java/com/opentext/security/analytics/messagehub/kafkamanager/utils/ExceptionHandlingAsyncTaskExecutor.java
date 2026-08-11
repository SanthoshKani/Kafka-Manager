// Copyright 2026 Open Text
package com.opentext.security.analytics.messagehub.kafkamanager.utils;

import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Future;
import org.jspecify.annotations.NonNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.DisposableBean;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.core.task.AsyncTaskExecutor;

/**
 * <p>ExceptionHandlingAsyncTaskExecutor class.</p>
 */
public class ExceptionHandlingAsyncTaskExecutor implements AsyncTaskExecutor, InitializingBean, DisposableBean {

    static final String EXCEPTION_MESSAGE = "Caught async exception";

    private final Logger log = LoggerFactory.getLogger(ExceptionHandlingAsyncTaskExecutor.class);

    private final AsyncTaskExecutor executor;

    /**
     * <p>Constructor for ExceptionHandlingAsyncTaskExecutor.</p>
     *
     * @param executor a {@link org.springframework.core.task.AsyncTaskExecutor} object.
     */
    public ExceptionHandlingAsyncTaskExecutor(AsyncTaskExecutor executor) {
        this.executor = executor;
    }

    @Override
    public void execute(@NonNull Runnable task, long startTimeout) {
        executor.execute(createWrappedRunnable(task), startTimeout);
    }

    @Override
    public @NonNull Future<?> submit(@NonNull Runnable task) {
        return executor.submit(createWrappedRunnable(task));
    }

    @Override
    public <T> @NonNull Future<T> submit(@NonNull Callable<T> task) {
        return executor.submit(createCallable(task));
    }

    @Override
    public @NonNull CompletableFuture<Void> submitCompletable(@NonNull Runnable task) {
        return executor.submitCompletable(createWrappedRunnable(task));
    }

    @Override
    public <T> @NonNull CompletableFuture<T> submitCompletable(@NonNull Callable<T> task) {
        return executor.submitCompletable(createCallable(task));
    }

    @Override
    public void destroy() throws Exception {
        if (executor instanceof DisposableBean bean) {
            bean.destroy();
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        if (executor instanceof InitializingBean bean) {
            bean.afterPropertiesSet();
        }
    }

    @Override
    public void execute(@NonNull Runnable task) {
        executor.execute(createWrappedRunnable(task));
    }

    private <T> Callable<T> createCallable(Callable<T> task) {
        return () -> {
            try {
                return task.call();
            } catch (Exception e) {
                handle(e);
                throw e;
            }
        };
    }

    private Runnable createWrappedRunnable(Runnable task) {
        return () -> {
            try {
                task.run();
            } catch (Exception e) {
                handle(e);
            }
        };
    }

    protected void handle(Exception e) {
        log.error(EXCEPTION_MESSAGE, e);
    }
}
