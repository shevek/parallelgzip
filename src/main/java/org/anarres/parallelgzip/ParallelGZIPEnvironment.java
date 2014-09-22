package org.anarres.parallelgzip;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.annotation.Nonnull;

/**
 *
 * @author shevek
 */
public class ParallelGZIPEnvironment {

    private static class ThreadPoolHolder {

        private static final ExecutorService EXECUTOR = Executors.newCachedThreadPool();
    }

    @Nonnull
    public static ExecutorService getSharedThreadPool() {
        return ThreadPoolHolder.EXECUTOR;
    }
}
