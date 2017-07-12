/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package org.anarres.parallelgzip;

import javax.annotation.Nonnegative;
import javax.annotation.Nonnull;
import java.io.*;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.concurrent.*;
import java.util.zip.CRC32;
import java.util.zip.Deflater;
import java.util.zip.DeflaterOutputStream;
import java.util.zip.GZIPOutputStream;

/**
 * A multi-threaded version of {@link GZIPOutputStream}.
 *
 * @author shevek
 */
public class ParallelGZIPOutputStream extends FilterOutputStream {

    // private static final Logger LOG = LoggerFactory.getLogger(ParallelGZIPOutputStream.class);
    private static final int GZIP_MAGIC = 0x8b1f;
    private static final int SIZE = 64 * 1024;

    @Nonnull
    private static Deflater newDeflater() {
        return new Deflater(Deflater.DEFAULT_COMPRESSION, true);
    }

    @Nonnull
    private static DeflaterOutputStream newDeflaterOutputStream(@Nonnull OutputStream out, @Nonnull Deflater deflater) {
        return new DeflaterOutputStream(out, deflater, 512, true);
    }

    private static class State {
        private final Deflater def = newDeflater();
        private final ByteArrayOutputStreamExposed buf = new ByteArrayOutputStreamExposed(SIZE);
        private final DeflaterOutputStream str = newDeflaterOutputStream(buf, def);
    }

    /** This ThreadLocal avoids the recycling of a lot of memory, causing lumpy performance. */
    private static final ThreadLocal<State> STATE = new ThreadLocal<State>() {
        @Override
        protected State initialValue() {
            return new State();
        }
    };

    /* Allow write into byte[] directly */
    private static class ByteArrayOutputStreamExposed extends ByteArrayOutputStream {
        private ByteArrayOutputStreamExposed(int size) {
            super(size);
        }

        private void writeTo(byte[] buf) {
            System.arraycopy(this.buf, 0, buf, 0, count);
        }
    }

    private static class Block implements Callable<Block> {
        // private final int index;
        private final byte[] buf = new byte[SIZE * 2];
        private int bufLength = 0;

        /*
         public Block(@Nonnegative int index) {
         this.index = index;
         }
         */
        // Only on worker thread
        @Override
        public Block call() throws IOException {
            // LOG.info("Processing " + this + " on " + Thread.currentThread());

            State state = STATE.get();
            // ByteArrayOutputStream buf = new ByteArrayOutputStream(in.length);   // Overestimate output size required.
            // DeflaterOutputStream def = newDeflaterOutputStream(buf);
            state.def.reset();
            state.buf.reset();
            state.str.write(buf, 0, bufLength);
            state.str.flush();

            bufLength = state.buf.size();
            assert bufLength <= buf.length;
            state.buf.writeTo(buf);

            // return Arrays.copyOf(in, in_length);
            return this;
        }

        @Override
        public String toString() {
            return "Block" /* + index */ + "(" + bufLength + "/" + buf.length + " bytes)";
        }
    }
    // TODO: Share, daemonize.
    private final ExecutorService executor;
    private final int nthreads;
    private final CRC32 crc = new CRC32();
    private final BlockingQueue<Future<Block>> emitQueue;
    private ArrayList<Block> blocks = new ArrayList<>();  // list of reusable blocks
    private Block block = new Block();
    private byte[] blockBuf = block.buf;
    /** Used as a sentinel for 'closed'. */
    private long bytesWritten = 0;

    // Master thread only
    public ParallelGZIPOutputStream(@Nonnull OutputStream out, @Nonnull ExecutorService executor, @Nonnegative int nthreads) throws IOException {
        super(out);
        this.executor = executor;
        this.nthreads = nthreads;
        this.emitQueue = new ArrayBlockingQueue<Future<Block>>(nthreads);
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
            int capacity = SIZE - block.bufLength;
            if (len >= capacity) {
                System.arraycopy(b, off, blockBuf, block.bufLength, capacity);
                block.bufLength += capacity;   // == block.in.length
                off += capacity;
                len -= capacity;
                submit();
            } else {
                System.arraycopy(b, off, blockBuf, block.bufLength, len);
                block.bufLength += len;
                // off += len;
                // len = 0;
                break;
            }
        }
    }

    // Master thread only
    private void submit() throws IOException {
        emitUntil(nthreads - 1);
        emitQueue.add(executor.submit(block));
        block = blocks.isEmpty() ? new Block() : blocks.remove(blocks.size() - 1);
        blockBuf = block.buf;
    }

    // Emit If Available - submit always
    // Emit At Least one - submit when executor is full
    // Emit All Remaining - flush(), close()
    // Master thread only
    private void tryEmit() throws IOException, InterruptedException, ExecutionException {
        for (;;) {
            Future<Block> future = emitQueue.peek();
            // LOG.info("Peeked future " + future);
            if (future == null)
                return;
            if (!future.isDone())
                return;
            // It's an ordered queue. This MUST be the same element as above.
            Block b = emitQueue.remove().get();
            out.write(b.buf, 0, b.bufLength);
            b.bufLength = 0;
            blocks.add(b);
        }
    }

    // Master thread only
    /** Emits any opportunistically available blocks. Furthermore, emits blocks until the number of executing tasks is less than taskCountAllowed. */
    private void emitUntil(@Nonnegative int taskCountAllowed) throws IOException {
        try {
            while (emitQueue.size() > taskCountAllowed) {
                // LOG.info("Waiting for taskCount=" + emitQueue.size() + " -> " + taskCountAllowed);
                Block b = emitQueue.remove().get();  // Valid because emitQueue.size() > 0
                out.write(b.buf, 0, b.bufLength);  // Blocks until this task is done.
                b.bufLength = 0;
                blocks.add(b);
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
        if (block.bufLength > 0)
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

            blocks.clear();
        }
    }
}
