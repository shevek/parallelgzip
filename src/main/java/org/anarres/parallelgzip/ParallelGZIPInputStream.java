/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

/**
 *
 * @author shevek
 */
public class ParallelGZIPInputStream extends GZIPInputStream {

    public ParallelGZIPInputStream(InputStream in, int size) throws IOException {
        super(in, size);
    }

    public ParallelGZIPInputStream(InputStream in) throws IOException {
        super(in);
    }
}
