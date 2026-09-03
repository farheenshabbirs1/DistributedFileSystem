package com.farheenshaikh.dfs;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/** Splits files into fixed-size chunks for storage, and reassembles them on read. */
public final class FileChunker {

    private FileChunker() {
    }

    public static List<String> putFile(Controller controller, String fileName, byte[] fileData, int chunkSize) {
        List<String> chunkIds = new ArrayList<>();
        int seq = 0;
        for (int offset = 0; offset < fileData.length; offset += chunkSize) {
            int end = Math.min(offset + chunkSize, fileData.length);
            byte[] slice = Arrays.copyOfRange(fileData, offset, end);
            String chunkId = fileName + "-chunk-" + seq++;
            controller.writeChunk(chunkId, slice);
            chunkIds.add(chunkId);
        }
        return chunkIds;
    }

    public static byte[] getFile(Controller controller, List<String> chunkIds) throws IOException {
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        for (String chunkId : chunkIds) {
            out.write(controller.readChunk(chunkId));
        }
        return out.toByteArray();
    }
}
