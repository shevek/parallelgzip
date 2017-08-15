package org.anarres.parallelgzip;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 *
 * @author shevek
 */
public class ParallelGZIPEnvironment {

    private static class ThreadFactoryHolder {

        private static final ThreadFactory THREAD_FACTORY = new ThreadFactory() {
            private final ThreadFactory defaultThreadFactory = Executors.defaultThreadFactory();
            private final AtomicLong counter = new AtomicLong(0);

            @Override
            public Thread newThread(@Nonnull Runnable r) {
                Thread thread = defaultThreadFactory.newThread(r);
                thread.setName("parallelgzip-" + counter.getAndIncrement());
                thread.setDaemon(true);
                return thread;
            }
        };
    }

    @Nonnull
    public static ThreadPoolExecutor newThreadPoolExecutor(@Nonnegative int nthreads) {
        ThreadPoolExecutor executor = new ThreadPoolExecutor(nthreads, nthreads,
                1L, TimeUnit.SECONDS,
                new ArrayBlockingQueue<Runnable>(nthreads * 20),
                ThreadFactoryHolder.THREAD_FACTORY,
                new ThreadPoolExecutor.CallerRunsPolicy());
        executor.allowCoreThreadTimeOut(true);
        return executor;
    }

    private static class ThreadPoolHolder {

        private static final ExecutorService EXECUTOR = newThreadPoolExecutor(Runtime.getRuntime().availableProcessors());
    }

    @Nonnull
    public static ExecutorService getSharedThreadPool() {
        return ThreadPoolHolder.EXECUTOR;
    }
}
