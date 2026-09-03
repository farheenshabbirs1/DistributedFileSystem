package com.farheenshaikh.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

/**
 * Splits files into fixed-size chunks for storage, and reassembles them on read. Both
 * directions fan out across chunks in parallel (one task per chunk, on a small thread pool),
 * so a multi-chunk file spreads its writes/reads across many storage nodes concurrently
 * instead of paying one round trip per chunk sequentially.
 */
public final class FileChunker {

    private FileChunker() {
    }

    public static List<String> putFile(Controller controller, String fileName, byte[] fileData, int chunkSize)
            throws IOException {
        List<byte[]> slices = new ArrayList<>();
        List<String> chunkIds = new ArrayList<>();
        int seq = 0;
        for (int offset = 0; offset < fileData.length; offset += chunkSize) {
            int end = Math.min(offset + chunkSize, fileData.length);
            slices.add(Arrays.copyOfRange(fileData, offset, end));
            chunkIds.add(fileName + "-chunk-" + seq++);
        }

        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, slices.size()));
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (int i = 0; i < slices.size(); i++) {
                String chunkId = chunkIds.get(i);
                byte[] slice = slices.get(i);
                futures.add(pool.submit(() -> {
                    controller.writeChunk(chunkId, slice);
                    return null;
                }));
            }
            awaitAll(futures, "write");
        } finally {
            pool.shutdown();
        }
        return chunkIds;
    }

    public static byte[] getFile(Controller controller, List<String> chunkIds) throws IOException {
        byte[][] results = new byte[chunkIds.size()][];

        ExecutorService pool = Executors.newFixedThreadPool(Math.max(1, chunkIds.size()));
        try {
            List<Future<Void>> futures = new ArrayList<>();
            for (int i = 0; i < chunkIds.size(); i++) {
                int index = i;
                String chunkId = chunkIds.get(i);
                futures.add(pool.submit(() -> {
                    results[index] = controller.readChunk(chunkId);
                    return null;
                }));
            }
            awaitAll(futures, "read");
        } finally {
            pool.shutdown();
        }

        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (byte[] chunk : results) {
            out.write(chunk);
        }
        return out.toByteArray();
    }

    private static void awaitAll(List<Future<Void>> futures, String verb) throws IOException {
        for (Future<Void> future : futures) {
            try {
                future.get();
            } catch (ExecutionException e) {
                Throwable cause = e.getCause();
                if (cause instanceof IOException) {
                    throw (IOException) cause;
                }
                throw new IOException("chunk " + verb + " failed", cause);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("chunk " + verb + " interrupted", e);
            }
        }
    }
}
