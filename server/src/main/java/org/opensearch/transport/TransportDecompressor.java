/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.transport;

import org.opensearch.common.bytes.ReleasableBytesReference;
import org.opensearch.common.recycler.Recycler;
import org.opensearch.common.util.PageCacheRecycler;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.bytes.CompositeBytesReference;
import org.opensearch.core.compress.Compressor;
import org.opensearch.core.compress.CompressorRegistry;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Decompresses data over the transport wire
 *
 * @opensearch.internal
 */
public class TransportDecompressor implements Closeable {

    public static Compressor COMPRESSOR;

    public synchronized static void setCompressor(String compressor) {
        COMPRESSOR = CompressorRegistry.getCompressor(compressor);
    }

    private final PageCacheRecycler recycler;
    private final Collection<ReleasableBytesReference> compressedBytes = new ArrayList<>();

    public TransportDecompressor(PageCacheRecycler recycler) {
        this.recycler = recycler;
    }

    public Collection<ReleasableBytesReference> decompress() throws IOException {
        final List<ReleasableBytesReference> results = new ArrayList<>();
        final BytesReference ref = CompositeBytesReference.of(compressedBytes.toArray(new ReleasableBytesReference[0]));
        try (InputStream is = COMPRESSOR.threadLocalInputStream(ref.streamInput())) {
            while (is.available() > 0) {
                final Recycler.V<byte[]> page = recycler.bytePage(false);
                final int bytesRead = is.read(page.v(), 0, PageCacheRecycler.BYTE_PAGE_SIZE);
                results.add(new ReleasableBytesReference(new BytesArray(page.v(), 0, bytesRead), page));
            }
        }
        close();
        return results;
    }

//    public ReleasableBytesReference pollDecompressedPage() {
//        if (pages.isEmpty()) {
//            return null;
//        } else if (pages.size() == 1) {
//            Recycler.V<byte[]> page = pages.pollFirst();
//            ReleasableBytesReference reference = new ReleasableBytesReference(new BytesArray(page.v(), 0, pageOffset), page);
//            pageOffset = 0;
//            return reference;
//        } else {
//            Recycler.V<byte[]> page = pages.pollFirst();
//            return new ReleasableBytesReference(new BytesArray(page.v()), page);
//        }
//    }

    @Override
    public void close() {
        for (ReleasableBytesReference ref : compressedBytes) {
            ref.close();
        }
        compressedBytes.clear();
    }

    public void accept(ReleasableBytesReference retainedContent) {
        compressedBytes.add(retainedContent.retain());
    }
}
