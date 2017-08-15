/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Random;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import static org.junit.Assert.*;

/**
 *
 * @author shevek
 */
public class ParallelGZIPOutputStreamTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelGZIPOutputStreamTest.class);

    private static class ByteArrayOutputBuffer extends ByteArrayOutputStream {

        public ByteArrayOutputBuffer(int size) {
            super(size);
        }

        @Nonnull
        public ByteArrayInputStream toInput() {
            return new ByteArrayInputStream(buf, 0, count);
        }
    }

    private void testPerformance(int len) throws Exception {
        Random r = new Random();
        byte[] data = new byte[len];
        r.nextBytes(data);
        LOG.info("Data is " + data.length + " bytes.");

        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer(data.length);    // Reallocation will occur on the first iteration.

        final int serialCount = 5;
        long serialTotal = 0;
        for (int i = 0; i < serialCount; i++) {
            out.reset();
            Stopwatch stopwatch = Stopwatch.createStarted();
            GZIPOutputStream gzip = new GZIPOutputStream(out);
            gzip.write(data);
            gzip.close();
            gzip.close();   // Again, for testing.
            long orig = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            serialTotal += orig;
            LOG.info("size=" + data.length + "; serial=" + orig);
        }

        double serialTime = serialTotal / (double) serialCount;
        for (int i = 0; i < 20; i++) {
            out.reset();
            Stopwatch stopwatch = Stopwatch.createStarted();
            ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out);
            gzip.write(data);
            gzip.close();
            long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            double perc = serialTime * 100d / elapsed;
            LOG.info("size=" + data.length + "; parallel=" + elapsed + "; perf=" + (int) perc + "%");
        }

        ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
        byte[] copy = ByteStreams.toByteArray(in);
        assertArrayEquals(data, copy);
    }

    @Test
    public void testPerformance() throws Exception {
        testPerformance(0);
        testPerformance(1);
        testPerformance(4);
        testPerformance(16);
        testPerformance(64 * 1024 - 1);
        testPerformance(64 * 1024);
        testPerformance(64 * 1024 + 1);
        testPerformance(4096 * 1024 + 17);
        testPerformance(16384 * 1024 + 17);
        testPerformance(65536 * 1024 + 17);
    }

    // This routine has been updated to be a lot more of a fuzzer.
    private void testThreads(@Nonnull ByteArrayOutputBuffer out, @Nonnegative int nthreads) throws Exception {
        Random r = new Random();

        ThreadPoolExecutor executor = ParallelGZIPEnvironment.newThreadPoolExecutor(nthreads);
        try {
            for (int i = 0; i < 3; i++) {
                out.reset();
                // The randomness throws the perf results off a bit, but fuzzes the block sizes.
                byte[] data = new byte[256 * 1024 * 1024 + r.nextInt(1048576)];
                r.nextBytes(data);
                LOG.info("Data is " + data.length + " bytes.");
                {
                    Stopwatch stopwatch = Stopwatch.createStarted();
                    ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out, executor);
                    gzip.write(data);
                    gzip.close();
                    long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                    LOG.info("nthreads=" + nthreads + "; parallel=" + elapsed);
                    gzip = null;
                }
                ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
                byte[] copy = ByteStreams.toByteArray(in);
                assertArrayEquals(data, copy);
            }
        } finally {
            executor.shutdown();
            executor.awaitTermination(10, TimeUnit.SECONDS);
        }
    }

    @Test
    public void testThreads() throws Exception {
        LOG.info("AvailableProcessors = " + Runtime.getRuntime().availableProcessors());
        // Reallocation will occur on the first iteration.
        // Sharing this will help the tests run without killing the JVM.
        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer(280 * 1000 * 1000);
        for (int i = 1; i < 32; i += 3)
            testThreads(out, i);
    }
}
