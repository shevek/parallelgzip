/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import com.google.common.io.Closer;
import com.google.common.io.Files;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import java.util.zip.GZIPOutputStream;
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

        @Nonnull
        public ByteArrayInputStream toInput() {
            return new ByteArrayInputStream(buf, 0, count);
        }
    }

    private void testCopy(int len) throws Exception {
        Random r = new Random();
        byte[] data = new byte[len];
        r.nextBytes(data);
        LOG.info("Data is " + data.length + " bytes.");

        ByteArrayOutputBuffer out = new ByteArrayOutputBuffer();    // Reallocation will occur on the first iteration.

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
                ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out);
                gzip.write(data);
                gzip.close();
                long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
                double perc = orig * 100 / (double) elapsed;
                LOG.info("Orig=" + orig + "; parallel=" + elapsed + "; perf=" + (int) perc + "%");
            }
        }

        ParallelGZIPInputStream in = new ParallelGZIPInputStream(out.toInput());
        byte[] copy = ByteStreams.toByteArray(in);
        assertArrayEquals(data, copy);
    }

    @Test
    public void testCopy() throws Exception {
        testCopy(0);
        testCopy(1);
        testCopy(4);
        testCopy(16);
        testCopy(64 * 1024 - 1);
        testCopy(64 * 1024);
        testCopy(64 * 1024 + 1);
        testCopy(4096 * 1024 + 17);
        testCopy(16384 * 1024 + 17);
        // testCopy(1024 * 1024 * 1024 + 17);
    }

    @Ignore
    @Test
    public void testCompress() throws Exception {
        File in = new File("/home/shevek/sda.img");
        File out = new File("/home/shevek/sda.img.gz");

        Closer closer = Closer.create();
        try {
            OutputStream os = closer.register(Files.asByteSink(out).openBufferedStream());
            ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(os);
            Files.asByteSource(in).copyTo(gzip);
            gzip.close();
        } finally {
            closer.close();
        }
    }
}