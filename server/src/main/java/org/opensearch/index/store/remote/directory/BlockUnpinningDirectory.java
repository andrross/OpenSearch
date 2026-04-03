/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.remote.directory;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.opensearch.index.store.remote.file.AbstractBlockIndexInput;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A directory wrapper that tracks all {@link AbstractBlockIndexInput} instances
 * opened through it (including their slices and clones) so their file cache
 * blocks can be unpinned after searchable snapshot restore completes.
 * <p>
 * After calling {@link #unpinAndStopTracking()}, all tracked blocks are released
 * (moved to LRU-evictable in the file cache) and no further tracking occurs.
 * The underlying {@link IndexInput} instances remain functional — they will
 * re-fetch blocks from local disk on the next read.
 * <p>
 * This class is not thread safe. It is intended to be created and used from
 * a single thread during the searchable snapshot restore procedure (i.e.
 * inside the {@code ReadOnlyEngine} constructor). After
 * {@link #unpinAndStopTracking()} is called, the wrapper becomes a
 * passthrough and is safe to use from any thread via the reader it backs.
 *
 * @opensearch.internal
 */
public final class BlockUnpinningDirectory extends FilterDirectory {
    private List<AbstractBlockIndexInput> tracked = new ArrayList<>();

    public BlockUnpinningDirectory(Directory in) {
        super(in);
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        IndexInput input = super.openInput(name, context);
        if (tracked != null && input instanceof AbstractBlockIndexInput) {
            AbstractBlockIndexInput blockInput = (AbstractBlockIndexInput) input;
            blockInput.setBlockTracker(this::track);
            tracked.add(blockInput);
        }
        return input;
    }

    private void track(AbstractBlockIndexInput input) {
        if (tracked != null) {
            tracked.add(input);
        }
    }

    /**
     * Unpins all file cache blocks held by inputs opened through this directory,
     * then stops tracking. Safe to call multiple times.
     */
    public void unpinAndStopTracking() {
        if (tracked == null) return;
        for (AbstractBlockIndexInput input : tracked) {
            input.unpinBlock();
        }
        tracked = null;
    }
}
