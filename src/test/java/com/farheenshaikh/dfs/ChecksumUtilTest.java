package com.farheenshaikh.dfs;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;

class ChecksumUtilTest {

    @Test
    void sameInputProducesSameChecksum() {
        byte[] data = "hello world".getBytes();
        assertEquals(ChecksumUtil.sha256(data), ChecksumUtil.sha256(data.clone()));
    }

    @Test
    void differentInputProducesDifferentChecksum() {
        assertNotEquals(ChecksumUtil.sha256("hello".getBytes()), ChecksumUtil.sha256("world".getBytes()));
    }
}
