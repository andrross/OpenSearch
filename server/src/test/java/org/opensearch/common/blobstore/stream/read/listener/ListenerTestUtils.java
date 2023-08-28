/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.common.Randomness;
import org.opensearch.core.action.ActionListener;

import java.io.IOException;
import java.io.InputStream;

/**
 * Utility class containing common functionality for read listener based tests
 */
public class ListenerTestUtils {

    /**
     * TestInputStream generates a variable length, random input stream for test use cases.
     */
    static class TestInputStream extends InputStream {
        private int remainingBytes;

        public TestInputStream(int length) {
            remainingBytes = length;
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            if (remainingBytes <= 0) return -1;
            int bytesToRead = len;
            bytesToRead = Math.min(remainingBytes, bytesToRead);
            remainingBytes -= bytesToRead;

            byte[] bytes = new byte[bytesToRead];
            Randomness.get().nextBytes(bytes);
            System.arraycopy(bytes, 0, b, off, bytesToRead);

            return bytesToRead;
        }

        @Override
        public int read() throws IOException {
            if (remainingBytes <= 0) return -1;
            remainingBytes--;
            byte[] data = new byte[1];
            Randomness.get().nextBytes(data);
            return (int) data[0];
        }

        @Override
        public int available() {
            return remainingBytes;
        }
    }

    /**
     * TestCompletionListener acts as a verification instance for wrapping listener based calls.
     */
    static class TestCompletionListener<T> implements ActionListener<T> {
        private int responseCount;
        private int failureCount;
        private T response;
        private Exception exception;

        @Override
        public void onResponse(T response) {
            this.response = response;
            responseCount++;
        }

        @Override
        public void onFailure(Exception e) {
            exception = e;
            failureCount++;
        }

        public int getResponseCount() {
            return responseCount;
        }

        public int getFailureCount() {
            return failureCount;
        }

        public T getResponse() {
            return response;
        }

        public Exception getException() {
            return exception;
        }
    }
}
