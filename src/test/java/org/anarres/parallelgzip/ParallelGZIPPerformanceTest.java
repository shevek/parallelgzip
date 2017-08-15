/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import com.google.common.base.Stopwatch;
import com.google.common.io.ByteStreams;
import java.io.OutputStream;
import java.util.Random;
import java.util.concurrent.TimeUnit;
import org.junit.Ignore;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author shevek
 */
@Ignore
public class ParallelGZIPPerformanceTest {

    private static final Logger LOG = LoggerFactory.getLogger(ParallelGZIPPerformanceTest.class);

    @Test
    public void testThreads() throws Exception {
        LOG.info("AvailableProcessors = " + Runtime.getRuntime().availableProcessors());
        Random r = new Random();
        byte[] data = new byte[1024 * 1024];
        r.nextBytes(data);
        for (int i = 0; i < data.length; i++)
            data[i] = (byte) (data[i] & 0x7f);  // Strip the top bit to make it amenable to Huffman compression.

        OutputStream out = ByteStreams.nullOutputStream();
        ParallelGZIPOutputStream gzip = new ParallelGZIPOutputStream(out);
        Stopwatch stopwatch = Stopwatch.createStarted();

        for (int i = 0; i < 1024 * 20; i++) {
            gzip.write(data);
        }
        gzip.close();

        long elapsed = stopwatch.elapsed(TimeUnit.MILLISECONDS);
        LOG.info("elapsed=" + elapsed);
    }
}
