package org.anarres.parallelgzip;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import javax.annotation.Nonnull;

/**
 *
 * @author shevek
 */
public class ParallelGZIPEnvironment {

    private static class ThreadPoolHolder {

        private static final ExecutorService EXECUTOR;

        static {
            ThreadFactory threadFactory = new ThreadFactory() {
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
            // int nthreads = Math.max(Runtime.getRuntime().availableProcessors() - 1, 1);
            int nthreads = Runtime.getRuntime().availableProcessors();
            ThreadPoolExecutor executor = new ThreadPoolExecutor(nthreads, nthreads,
                    1L, TimeUnit.SECONDS,
                    new ArrayBlockingQueue<Runnable>(nthreads * 20),
                    threadFactory,
                    new ThreadPoolExecutor.CallerRunsPolicy());
            executor.allowCoreThreadTimeOut(true);
            EXECUTOR = executor;
        }
    }

    @Nonnull
    public static ExecutorService getSharedThreadPool() {
        return ThreadPoolHolder.EXECUTOR;
    }
}
