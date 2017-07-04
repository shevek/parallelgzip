/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import java.io.ByteArrayOutputStream;
import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;
import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;

/**
 * A multi-threaded version of {@link GZIPOutputStream}.
 *
 * @author shevek
 */
public class ParallelGZIPOutputStream extends FilterOutputStream {

    // private static final Logger LOG = LoggerFactory.getLogger(ParallelGZIPOutputStream.class);
    private final static int GZIP_MAGIC = 0x8b1f;

    @Nonnull
    private static Deflater newDeflater() {
        return new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    }

    @Nonnull
    private static DeflaterOutputStream newDeflaterOutputStream(@Nonnull OutputStream out, @Nonnull Deflater deflater) {
        return new DeflaterOutputStream(out, deflater, 512, true);
    }

    private static class Block implements Callable<byte[]> {

        private static class State {

            private final Deflater def = newDeflater();
            private final ByteArrayOutputStream buf = new ByteArrayOutputStream(SIZE);
            private final DeflaterOutputStream str = newDeflaterOutputStream(buf, def);
        }
        /** This ThreadLocal avoids the recycling of a lot of memory, causing lumpy performance. */
        private static final ThreadLocal<State> STATE = new ThreadLocal<State>() {
            @Override
            protected State initialValue() {
                return new State();
            }
        };
        public static final int SIZE = 64 * 1024;
        // private final int index;
        private final byte[] in = new byte[SIZE];
        private int in_length = 0;

        /*
         public Block(@Nonnegative int index) {
         this.index = index;
         }
         */
        // Only on worker thread
        @Override
        public byte[] call() throws Exception {
            // LOG.info("Processing " + this + " on " + Thread.currentThread());

            State state = STATE.get();
            // ByteArrayOutputStream buf = new ByteArrayOutputStream(in.length);   // Overestimate output size required.
            // DeflaterOutputStream def = newDeflaterOutputStream(buf);
            state.def.reset();
            state.buf.reset();
            state.str.write(in, 0, in_length);
            state.str.flush();

            // return Arrays.copyOf(in, in_length);
            return state.buf.toByteArray();
        }

        @Override
        public String toString() {
            return "Block" /* + index */ + "(" + in_length + "/" + in.length + " bytes)";
        }
    }
    // TODO: Share, daemonize.
    private final ExecutorService executor;
    private final CRC32 crc = new CRC32();
    private final int emitQueueSize;
    private final BlockingQueue<Future<byte[]>> emitQueue;
    private Block block = new Block(/* 0 */);
    /** Used as a sentinel for 'closed'. */
    private long bytesWritten = 0;

    // Master thread only
    public ParallelGZIPOutputStream(@Nonnull OutputStream out, @Nonnull ExecutorService executor, @Nonnegative int nthreads) throws IOException {
        super(out);
        this.executor = executor;
        // Some blocks compress faster than others; allow a long enough queue to keep all CPUs busy at least for a bit.
        this.emitQueueSize = nthreads * 3;
        this.emitQueue = new ArrayBlockingQueue<Future<byte[]>>(emitQueueSize);
        writeHeader();
    }

    /**
     * Creates a ParallelGZIPOutputStream
     * using {@link ParallelGZIPEnvironment#getSharedThreadPool()}.
     *
     * @param out the eventual output stream for the compressed data.
     * @throws IOException if it all goes wrong.
     */
    public ParallelGZIPOutputStream(@Nonnull OutputStream out, @Nonnegative int nthreads) throws IOException {
        this(out, ParallelGZIPEnvironment.getSharedThreadPool(), nthreads);
    }

    /**
     * Creates a ParallelGZIPOutputStream
     * using {@link ParallelGZIPEnvironment#getSharedThreadPool()}
     * and {@link Runtime#availableProcessors()}.
     *
     * @param out the eventual output stream for the compressed data.
     * @throws IOException if it all goes wrong.
     */
    public ParallelGZIPOutputStream(@Nonnull OutputStream out) throws IOException {
        this(out, Runtime.getRuntime().availableProcessors());
    }

    /*
     * @see http://www.gzip.org/zlib/rfc-gzip.html#file-format
     */
    private void writeHeader() throws IOException {
        out.write(new byte[]{
            (byte) GZIP_MAGIC, // ID1: Magic number (little-endian short)
            (byte) (GZIP_MAGIC >> 8), // ID2: Magic number (little-endian short)
            Deflater.DEFLATED, // CM: Compression method
            0, // FLG: Flags (byte)
            0, 0, 0, 0, // MTIME: Modification time (int)
            0, // XFL: Extra flags
            3 // OS: Operating system (3 = Linux)
        });
    }

    // Master thread only
    @Override
    public void write(int b) throws IOException {
        byte[] single = new byte[1];
        single[0] = (byte) (b & 0xFF);
        write(single);
    }

    // Master thread only
    @Override
    public void write(byte[] b) throws IOException {
        write(b, 0, b.length);
    }

    // Master thread only
    @Override
    public void write(byte[] b, int off, int len) throws IOException {
        crc.update(b, off, len);
        bytesWritten += len;
        while (len > 0) {
            // assert block.in_length < block.in.length
            int capacity = block.in.length - block.in_length;
            if (len >= capacity) {
                System.arraycopy(b, off, block.in, block.in_length, capacity);
                block.in_length += capacity;   // == block.in.length
                off += capacity;
                len -= capacity;
                submit();
            } else {
                System.arraycopy(b, off, block.in, block.in_length, len);
                block.in_length += len;
                // off += len;
                // len = 0;
                break;
            }
        }
    }

    // Master thread only
    private void submit() throws IOException {
        emitUntil(emitQueueSize - 1);
        emitQueue.add(executor.submit(block));
        block = new Block(/* block.index + 1 */);
    }

    // Emit If Available - submit always
    // Emit At Least one - submit when executor is full
    // Emit All Remaining - flush(), close()
    // Master thread only
    private void tryEmit() throws IOException, InterruptedException, ExecutionException {
        for (;;) {
            Future<byte[]> future = emitQueue.peek();
            // LOG.info("Peeked future " + future);
            if (future == null)
                return;
            if (!future.isDone())
                return;
            // It's an ordered queue. This MUST be the same element as above.
            emitQueue.remove();
            out.write(future.get());
        }
    }

    // Master thread only
    /** Emits any opportunistically available blocks. Furthermore, emits blocks until the number of executing tasks is less than taskCountAllowed. */
    private void emitUntil(@Nonnegative int taskCountAllowed) throws IOException {
        try {
            while (emitQueue.size() > taskCountAllowed) {
                // LOG.info("Waiting for taskCount=" + emitQueue.size() + " -> " + taskCountAllowed);
                Future<byte[]> future = emitQueue.remove(); // Valid because emitQueue.size() > 0
                out.write(future.get());  // Blocks until this task is done.
            }
            // We may have achieved more opportunistically available blocks
            // while waiting for a block above. Let's emit them here.
            tryEmit();
        } catch (ExecutionException e) {
            throw new IOException(e);
        } catch (InterruptedException e) {
            throw new InterruptedIOException();
        }
    }

    // Master thread only
    @Override
    public void flush() throws IOException {
        // LOG.info("Flush: " + block);
        if (block.in_length > 0)
            submit();
        emitUntil(0);
        super.flush();
    }

    // Master thread only
    @Override
    public void close() throws IOException {
        // LOG.info("Closing: bytesWritten=" + bytesWritten);
        if (bytesWritten >= 0) {
            flush();

            newDeflaterOutputStream(out, newDeflater()).finish();

            ByteBuffer buf = ByteBuffer.allocate(8);
            buf.order(ByteOrder.LITTLE_ENDIAN);
            // LOG.info("CRC is " + crc.getValue());
            buf.putInt((int) crc.getValue());
            buf.putInt((int) (bytesWritten % 4294967296L));
            out.write(buf.array()); // allocate() guarantees a backing array.
            // LOG.info("trailer is " + Arrays.toString(buf.array()));

            out.flush();
            out.close();

            bytesWritten = Integer.MIN_VALUE;
            // } else {
            // LOG.warn("Already closed.");
        }
    }
}
