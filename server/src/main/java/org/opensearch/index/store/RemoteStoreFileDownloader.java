/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.Nullable;
import org.opensearch.core.action.ActionListener;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collection;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

/**
 * Helper class to downloads files from a {@link RemoteSegmentStoreDirectory}
 * instance to a local {@link Directory} instance in parallel depending on thread
 * pool size and recovery settings.
 */
public final class RemoteStoreFileDownloader {
    private final Logger logger;
    private final ThreadPool threadPool;

    public RemoteStoreFileDownloader(Logger logger, ThreadPool threadPool) {
        this.logger = logger;
        this.threadPool = threadPool;
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory.
     * @param storeDirectory The local directory to copy segment files to
     * @param sourceRemoteDirectory The remote directory to copy segment files from
     * @param toDownloadSegments The list of segment files to download
     */
    public void download(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        Collection<String> toDownloadSegments
    ) {
        downloadInternal(storeDirectory, sourceRemoteDirectory, null, toDownloadSegments, () -> {});
    }

    /**
     * Copies the given segments from the remote segment store to the given
     * local directory, while also copying the segments _to_ another remote directory.
     * @param storeDirectory The local directory to copy segment files to
     * @param sourceRemoteDirectory The remote directory to copy segment files from
     * @param targetRemoteDirectory The second remote directory that segment files are
     *                              copied to after being copied to the local directory
     * @param toDownloadSegments The list of segment files to download
     * @param onFileCompletion A generic runnable that is invoked after each file download.
     *                         Must be thread safe as this may be invoked concurrently from
     *                         different threads.
     */
    public void download(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        RemoteSegmentStoreDirectory targetRemoteDirectory,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) {
        downloadInternal(storeDirectory, sourceRemoteDirectory, targetRemoteDirectory, toDownloadSegments, onFileCompletion);
    }

    private void downloadInternal(
        Directory storeDirectory,
        RemoteSegmentStoreDirectory sourceRemoteDirectory,
        @Nullable Directory targetRemoteDirectory,
        Collection<String> toDownloadSegments,
        Runnable onFileCompletion
    ) {
        final Queue<String> queue = new ConcurrentLinkedQueue<>(toDownloadSegments);
        // Choose the minimum of:
        // - number of files to download
        // - max thread pool size
        // - "indices.recovery.max_concurrent_remote_store_streams" setting
        final int threads = Math.min(
            toDownloadSegments.size(),
            Math.min(
                threadPool.info(ThreadPool.Names.REMOTE_RECOVERY).getMax(),
                sourceRemoteDirectory.getRecoverySettings().getMaxConcurrentRemoteStoreStreams()
            )
        );
        logger.trace("Starting download of {} files with {} threads", queue.size(), threads);
        final PlainActionFuture<Collection<Void>> listener = PlainActionFuture.newFuture();
        final ActionListener<Void> allFilesListener = ActionListener.delegateResponse(
            new GroupedActionListener<>(listener, queue.size()),
            (l, e) -> {
                // Short-circuit the GroupedActionListener on any failure and notify the delegate listener directly
                queue.clear();
                l.onFailure(e);
            }
        );
        for (int i = 0; i < threads; i++) {
            threadPool.executor(ThreadPool.Names.REMOTE_RECOVERY)
                .submit(
                    () -> copyOneFile(
                        storeDirectory,
                        sourceRemoteDirectory,
                        targetRemoteDirectory,
                        queue,
                        onFileCompletion,
                        allFilesListener
                    )
                );
        }
        listener.actionGet();
    }

    private void copyOneFile(
        Directory storeDirectory,
        Directory sourceRemoteDirectory,
        @Nullable Directory targetRemoteDirectory,
        Queue<String> queue,
        Runnable onFileCompletion,
        ActionListener<Void> listener
    ) {
        final String source = queue.poll();
        if (source != null) {
            logger.trace("Downloading file {}", source);
            try {
                storeDirectory.copyFrom(sourceRemoteDirectory, source, source, IOContext.DEFAULT);
                onFileCompletion.run();
                if (targetRemoteDirectory != null) {
                    targetRemoteDirectory.copyFrom(storeDirectory, source, source, IOContext.DEFAULT);
                }
                listener.onResponse(null);
            } catch (IOException e) {
                listener.onFailure(e);
            }
            copyOneFile(storeDirectory, sourceRemoteDirectory, targetRemoteDirectory, queue, onFileCompletion, listener);
        }
    }
}
