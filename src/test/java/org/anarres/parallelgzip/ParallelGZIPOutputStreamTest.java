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
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnull;
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

        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer();    // Reallocation will occur on the first iteration.

        // availableProcessors does not return hyperthreads; let's assume some overhead.
        int nthreads = Runtime.getRuntime().availableProcessors() + 1;

        for (int i = 0; i < 10; i++) {
            out.reset();
            long orig;
            {
                Stopwatch stopwatch = Stopwatch.createStarted();
                GZIPOutputStream gzip = new GZIPOutputStream(out);
                gzip.write(data);
                gzip.close();
                orig = stopwatch.elapsed(TimeUnit.MILLISECONDS);
            }

            out.reset();

            {
                Stopwatch stopwatch = Stopwatch.createStarted();
                ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out, nthreads);
                gzip.write(data);
                gzip.close();
                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                double perc = orig * 100 / (double) elapsed;
                LOG.info("size=" + data.length + "; serial=" + orig + "; parallel=" + elapsed + "; perf=" + (int) perc + "%");
            }
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

    private void testThreads(int nthreads) throws Exception {
        Random r = new Random();
        byte[] data = new byte[4 * 1024 * 1024];
        r.nextBytes(data);
        LOG.info("Data is " + data.length + " bytes.");

        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer();    // Reallocation will occur on the first iteration.

        for (int i = 0; i < 10; i++) {
            out.reset();
            {
                Stopwatch stopwatch = Stopwatch.createStarted();
                ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out, nthreads);
                gzip.write(data);
                gzip.close();
                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                LOG.info("nthreads=" + nthreads + "; parallel=" + elapsed);
            }
        }

        ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
        byte[] copy = ByteStreams.toByteArray(in);
        assertArrayEquals(data, copy);
    }

    @Test
    public void testThreads() throws Exception {
        LOG.info("AvailableProcessors = " + Runtime.getRuntime().availableProcessors());
        for (int i = 1; i < 8; i++)
            testThreads(i);
    }
}
