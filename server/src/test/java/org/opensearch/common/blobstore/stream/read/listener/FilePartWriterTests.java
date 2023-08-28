/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.opensearch.common.blobstore.stream.read.listener.ListenerTestUtils.TestCompletionListener;

public class FilePartWriterTests extends OpenSearchTestCase {

    private Path path;
    private static ThreadPool threadPool;

    @BeforeClass
    public static void setup() {
        threadPool = new TestThreadPool(FilePartWriterTests.class.getName());
    }

    @AfterClass
    public static void cleanup() {
        threadPool.shutdown();
    }

    @Before
    public void init() throws Exception {
        path = createTempDir("FilePartWriterTests");
    }

    public void testFilePartWriter() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 100;
        int partNumber = 1;
        InputStream inputStream = new ListenerTestUtils.TestInputStream(contentLength);
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, inputStream.available(), 0);
        AtomicBoolean anyStreamFailed = new AtomicBoolean();
        TestCompletionListener<Integer> fileCompletionListener = new TestCompletionListener<>();

        FilePartWriter filePartWriter = new FilePartWriter(
            partNumber,
            inputStreamContainer,
            segmentFilePath,
            anyStreamFailed,
            fileCompletionListener
        );
        threadPool.executor(ThreadPool.Names.GENERIC).submit(filePartWriter).get();

        assertTrue(Files.exists(segmentFilePath));
        assertEquals(contentLength, Files.size(segmentFilePath));
        assertEquals(1, fileCompletionListener.getResponseCount());
        assertEquals(Integer.valueOf(partNumber), fileCompletionListener.getResponse());
    }

    public void testFilePartWriterWithOffset() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 100;
        int offset = 10;
        int partNumber = 1;
        InputStream inputStream = new ListenerTestUtils.TestInputStream(contentLength);
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, inputStream.available(), offset);
        AtomicBoolean anyStreamFailed = new AtomicBoolean();
        TestCompletionListener<Integer> fileCompletionListener = new TestCompletionListener<>();

        FilePartWriter filePartWriter = new FilePartWriter(
            partNumber,
            inputStreamContainer,
            segmentFilePath,
            anyStreamFailed,
            fileCompletionListener
        );
        threadPool.executor(ThreadPool.Names.GENERIC).submit(filePartWriter).get();

        assertTrue(Files.exists(segmentFilePath));
        assertEquals(contentLength + offset, Files.size(segmentFilePath));
        assertEquals(1, fileCompletionListener.getResponseCount());
        assertEquals(Integer.valueOf(partNumber), fileCompletionListener.getResponse());
    }

    public void testFilePartWriterLargeInput() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 50 * 1024 * 1024;
        int partNumber = 1;
        InputStream inputStream = new ListenerTestUtils.TestInputStream(contentLength);
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, contentLength, 0);
        AtomicBoolean anyStreamFailed = new AtomicBoolean();
        TestCompletionListener<Integer> fileCompletionListener = new TestCompletionListener<>();

        FilePartWriter filePartWriter = new FilePartWriter(
            partNumber,
            inputStreamContainer,
            segmentFilePath,
            anyStreamFailed,
            fileCompletionListener
        );
        threadPool.executor(ThreadPool.Names.GENERIC).submit(filePartWriter).get();

        assertTrue(Files.exists(segmentFilePath));
        assertEquals(contentLength, Files.size(segmentFilePath));

        assertEquals(1, fileCompletionListener.getResponseCount());
        assertEquals(Integer.valueOf(partNumber), fileCompletionListener.getResponse());
    }

    public void testFilePartWriterException() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 50 * 1024 * 1024;
        int partNumber = 1;
        InputStream inputStream = new ListenerTestUtils.TestInputStream(contentLength);
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, contentLength, 0);
        AtomicBoolean anyStreamFailed = new AtomicBoolean();
        TestCompletionListener<Integer> fileCompletionListener = new TestCompletionListener<>();

        IOException ioException = new IOException();
        FilePartWriter filePartWriter = new FilePartWriter(
            partNumber,
            inputStreamContainer,
            segmentFilePath,
            anyStreamFailed,
            fileCompletionListener
        );
        assertFalse(anyStreamFailed.get());
        filePartWriter.processFailure(ioException);

        assertTrue(anyStreamFailed.get());
        assertFalse(Files.exists(segmentFilePath));

        // Fail stream again to simulate another stream failure for same file
        filePartWriter.processFailure(ioException);

        assertTrue(anyStreamFailed.get());
        assertFalse(Files.exists(segmentFilePath));

        assertEquals(0, fileCompletionListener.getResponseCount());
        assertEquals(1, fileCompletionListener.getFailureCount());
        assertEquals(ioException, fileCompletionListener.getException());
    }

    public void testFilePartWriterStreamFailed() throws Exception {
        Path segmentFilePath = path.resolve(UUID.randomUUID().toString());
        int contentLength = 100;
        int partNumber = 1;
        InputStream inputStream = new ListenerTestUtils.TestInputStream(contentLength);
        InputStreamContainer inputStreamContainer = new InputStreamContainer(inputStream, inputStream.available(), 0);
        AtomicBoolean anyStreamFailed = new AtomicBoolean(true);
        TestCompletionListener<Integer> fileCompletionListener = new TestCompletionListener<>();

        FilePartWriter filePartWriter = new FilePartWriter(
            partNumber,
            inputStreamContainer,
            segmentFilePath,
            anyStreamFailed,
            fileCompletionListener
        );
        threadPool.executor(ThreadPool.Names.GENERIC).submit(filePartWriter).get();

        assertFalse(Files.exists(segmentFilePath));
        assertEquals(0, fileCompletionListener.getResponseCount());
    }
}
