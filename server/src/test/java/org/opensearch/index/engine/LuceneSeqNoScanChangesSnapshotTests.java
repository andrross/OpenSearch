/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.common.CheckedSupplier;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.translog.Translog;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.hasSize;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class LuceneSeqNoScanChangesSnapshotTests extends EngineTestCase {
    private MapperService mapperService;

    @Before
    public void createMapper() throws Exception {
        mapperService = createMapperService();
    }

    @Override
    protected Settings indexSettings() {
        return Settings.builder()
            .put(super.indexSettings())
            .put(IndexSettings.INDEX_SOFT_DELETES_SETTING.getKey(), true) // always enable soft-deletes
            .build();
    }

    public void testEmptyEngine() throws Exception {
        final long fromSeqNo = randomNonNegativeLong();
        final long toSeqNo = randomLongBetween(fromSeqNo, Long.MAX_VALUE);
        engine.refresh("test");
        try (Translog.Snapshot snapshot = newScanSnapshot(fromSeqNo, toSeqNo, true, randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(
                error.getMessage(),
                containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
            );
        }
        try (Translog.Snapshot snapshot = newScanSnapshot(fromSeqNo, toSeqNo, false, randomBoolean())) {
            assertThat(drainAll(snapshot), hasSize(0));
        }
    }

    public void testAppendOnlyContiguous() throws Exception {
        final int numOps = between(1, 500);
        final Map<Long, ParsedDocument> expectedDocs = indexAppendOnly(numOps);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            assertThat(snapshot.totalOperations(), equalTo(numOps));
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
            assertThat(snapshot.skippedOperations(), equalTo(0));
        }
        // a sub-range: docs outside the range must be skipped without being emitted
        final long fromSeqNo = randomLongBetween(0, numOps - 1);
        final long toSeqNo = randomLongBetween(fromSeqNo, numOps - 1);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(fromSeqNo, toSeqNo, true, true)) {
            assertThat(snapshot.totalOperations(), equalTo(Math.toIntExact(toSeqNo - fromSeqNo + 1)));
            assertOpsMatch(drainAll(snapshot), expectedDocs, fromSeqNo, toSeqNo);
        }
    }

    public void testInterleavedSegments() throws Exception {
        final int stripes = between(2, 4);
        final int numOps = stripes * between(10, 100);
        final Map<Long, ParsedDocument> expectedDocs = indexInterleavedSegments(numOps, stripes);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            assertThat(snapshot.totalOperations(), equalTo(numOps));
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
        }
    }

    public void testDescendingSeqNoOrder() throws Exception {
        // docID order is exactly the reverse of seqNo order: the pathological layout for a forward scan,
        // forcing a relocation for every operation
        final int numOps = between(2, 100);
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int i = 0; i < numOps; i++) {
            final long seqNo = numOps - 1 - i;
            final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
            engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
            expectedDocs.put(seqNo, doc);
        }
        engine.refresh("test");
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            assertThat(snapshot.totalOperations(), equalTo(numOps));
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
        }
    }

    /**
     * A nested document is indexed into Lucene as multiple documents. While the root document has both sequence
     * number and primary term, non-root documents don't have primary term but only sequence numbers. This test
     * verifies that {@link LuceneSeqNoScanChangesSnapshot} correctly skips non-root documents and returns at most
     * one operation per sequence number, across a random history of indexes, deletes, and noops.
     */
    public void testSkipNonRootOfNestedDocuments() throws Exception {
        final Map<Long, Long> seqNoToTerm = new HashMap<>();
        final List<Engine.Operation> operations = generateHistoryOnReplica(
            between(1, 100),
            randomBoolean(),
            randomBoolean(),
            randomBoolean()
        );
        for (Engine.Operation op : operations) {
            if (engine.getLocalCheckpointTracker().hasProcessed(op.seqNo()) == false) {
                seqNoToTerm.put(op.seqNo(), op.primaryTerm());
            }
            applyOperation(engine, op);
            if (rarely()) {
                engine.refresh("test");
            }
            if (rarely()) {
                engine.flush();
            }
        }
        final long maxSeqNo = engine.getLocalCheckpointTracker().getMaxSeqNo();
        engine.refresh("test");
        final boolean accurateCount = randomBoolean();
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, maxSeqNo, false, accurateCount)) {
            if (accurateCount) {
                assertThat(snapshot.totalOperations(), equalTo(seqNoToTerm.size()));
            } else {
                assertThat(snapshot.totalOperations(), equalTo(-1));
            }
            final List<Translog.Operation> ops = drainAll(snapshot);
            assertThat(ops, hasSize(seqNoToTerm.size()));
            for (Translog.Operation op : ops) {
                assertThat(op.toString(), op.primaryTerm(), equalTo(seqNoToTerm.get(op.seqNo())));
            }
        }
    }

    public void testMissingSeqNo() throws Exception {
        // seqNos [0, numOps) with a hole at missingSeqNo
        final int numOps = between(3, 100);
        final long missingSeqNo = between(1, numOps - 2); // interior hole so ops exist on both sides
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (long seqNo = 0; seqNo < numOps; seqNo++) {
            if (seqNo == missingSeqNo) {
                continue;
            }
            final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
            engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
            expectedDocs.put(seqNo, doc);
        }
        if (randomBoolean()) {
            engine.flush();
        }
        engine.refresh("test");
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(error.getMessage(), containsString("missing seqno [" + missingSeqNo + "]"));
        }
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, false, true)) {
            final List<Translog.Operation> ops = drainAll(snapshot);
            assertThat(ops, hasSize(numOps - 1));
            for (Translog.Operation op : ops) {
                assertNotEquals(missingSeqNo, op.seqNo());
                assertNotNull(expectedDocs.get(op.seqNo()));
            }
        }
    }

    public void testScatteredLayoutWithTinyBudget() throws Exception {
        // a shuffled permutation in one segment (the layout index sorting produces) with a tiny buffer budget:
        // chase mode with constant relocations, likely tripping the reader fallback; the emitted operations must
        // be identical to an unconstrained drain either way
        final int numOps = between(100, 300);
        final List<Long> seqNos = new ArrayList<>();
        for (long seqNo = 0; seqNo < numOps; seqNo++) {
            seqNos.add(seqNo);
        }
        Collections.shuffle(seqNos, random());
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (long seqNo : seqNos) {
            final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
            engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
            expectedDocs.put(seqNo, doc);
        }
        engine.refresh("test");
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try (LuceneSeqNoScanChangesSnapshot snapshot = new LuceneSeqNoScanChangesSnapshot(searcher, 0, numOps - 1, true, true, 1)) {
            searcher = null;
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
        } finally {
            IOUtils.close(searcher);
        }
    }

    public void testFallsBackToDefaultReaderOnDescendingLayout() throws Exception {
        // descending seqNos give exactly one relocation and one read per operation (the tail only ever sees
        // already-emitted seqNos), so the reads-per-relocation heuristic must deterministically trip the fallback
        // once the window fills
        final int numOps = between(LuceneSeqNoScanChangesSnapshot.READER_MODE_WINDOW_RELOCATIONS + 4, 100);
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int i = 0; i < numOps; i++) {
            final long seqNo = numOps - 1 - i;
            final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
            engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
            expectedDocs.put(seqNo, doc);
        }
        engine.refresh("test");
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
            assertTrue("chase mode must fall back to the default reader", snapshot.isUsingDefaultStoredFieldsReader());
        }
    }

    public void testKeepsSequentialReaderOnContiguousLayout() throws Exception {
        final int numOps = between(100, 500);
        final Map<Long, ParsedDocument> expectedDocs = indexAppendOnly(numOps);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
            assertFalse("a contiguous layout must keep the sequential reader", snapshot.isUsingDefaultStoredFieldsReader());
        }
    }

    public void testCachedReaderCountIsBounded() throws Exception {
        // more segments than the reader-cache cap: the drain alternates between all of them constantly, and must
        // stay correct while the cache stays LRU-bounded
        final int stripes = LuceneSeqNoScanChangesSnapshot.MAX_CACHED_STORED_FIELDS_READERS + between(2, 6);
        final int numOps = stripes * between(5, 20);
        final Map<Long, ParsedDocument> expectedDocs = indexInterleavedSegments(numOps, stripes);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
            assertThat(
                snapshot.cachedStoredFieldsReaderCount(),
                lessThanOrEqualTo(LuceneSeqNoScanChangesSnapshot.MAX_CACHED_STORED_FIELDS_READERS)
            );
        }
    }

    public void testSmallBufferBudget() throws Exception {
        // a one-byte budget forces a relocation for nearly every ahead-of-frontier operation, stressing the
        // budget-break path; the output must be identical to an unconstrained drain
        final int stripes = between(2, 4);
        final int numOps = stripes * between(10, 50);
        final Map<Long, ParsedDocument> expectedDocs = indexInterleavedSegments(numOps, stripes);
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try (LuceneSeqNoScanChangesSnapshot snapshot = new LuceneSeqNoScanChangesSnapshot(searcher, 0, numOps - 1, true, true, 1)) {
            searcher = null;
            assertOpsMatch(drainAll(snapshot), expectedDocs, 0, numOps - 1);
        } finally {
            IOUtils.close(searcher);
        }
    }

    public void testReadOnDifferentThreads() throws Exception {
        // every next() runs on a fresh thread, forcing the snapshot to drop its thread-bound stored fields
        // readers and doc values iterators each call; join() gives the happens-before that consumers get from
        // locking
        final int stripes = between(1, 3);
        final int numOps = stripes * between(10, 30);
        final Map<Long, ParsedDocument> expectedDocs = indexInterleavedSegments(numOps, stripes);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            final List<Translog.Operation> ops = new ArrayList<>();
            Translog.Operation op = callOnNewThread(snapshot::next);
            while (op != null) {
                ops.add(op);
                op = callOnNewThread(snapshot::next);
            }
            assertOpsMatch(ops, expectedDocs, 0, numOps - 1);
        }
    }

    public void testTombstones() throws Exception {
        // interleave indexes and deletes so the snapshot must materialize Delete operations from tombstone docs
        final int numOps = between(2, 100);
        final Map<Long, String> deletes = new HashMap<>();
        final Map<Long, ParsedDocument> indexes = new HashMap<>();
        for (long seqNo = 0; seqNo < numOps; seqNo++) {
            final String id = "id-" + randomIntBetween(0, 5);
            if (randomBoolean()) {
                final ParsedDocument doc = createParsedDoc(id, null);
                engine.index(replicaIndexForDoc(doc, seqNo + 1, seqNo, false));
                indexes.put(seqNo, doc);
            } else {
                engine.delete(replicaDeleteForDoc(id, seqNo + 1, seqNo, randomNonNegativeLong()));
                deletes.put(seqNo, id);
            }
            if (rarely()) {
                engine.flush();
            }
        }
        engine.refresh("test");
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(0, numOps - 1, true, true)) {
            final List<Translog.Operation> ops = drainAll(snapshot);
            assertThat(ops, hasSize(numOps));
            for (Translog.Operation op : ops) {
                if (deletes.containsKey(op.seqNo())) {
                    assertThat(op.toString(), op, instanceOf(Translog.Delete.class));
                    assertThat(((Translog.Delete) op).id(), equalTo(deletes.get(op.seqNo())));
                } else {
                    assertThat(op.toString(), op, instanceOf(Translog.Index.class));
                    assertThat(((Translog.Index) op).id(), equalTo(indexes.get(op.seqNo()).id()));
                }
            }
        }
    }

    public void testInvalidArguments() throws Exception {
        engine.refresh("test");
        {
            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try {
                IllegalArgumentException error = expectThrows(
                    IllegalArgumentException.class,
                    () -> new LuceneSeqNoScanChangesSnapshot(searcher, 5, 1, randomBoolean(), randomBoolean())
                );
                assertThat(error.getMessage(), containsString("Invalid range"));
            } finally {
                IOUtils.close(searcher);
            }
        }
        {
            Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
            try {
                IllegalArgumentException error = expectThrows(
                    IllegalArgumentException.class,
                    () -> new LuceneSeqNoScanChangesSnapshot(searcher, 0, 1, randomBoolean(), randomBoolean(), 0)
                );
                assertThat(error.getMessage(), containsString("Buffer budget must be positive"));
            } finally {
                IOUtils.close(searcher);
            }
        }
    }

    public void testOverFlow() throws Exception {
        final int numOps = between(1, 20);
        indexAppendOnly(numOps);
        final long fromSeqNo = randomLongBetween(0, 5);
        final long toSeqNo = randomLongBetween(Long.MAX_VALUE - 5, Long.MAX_VALUE);
        try (Translog.Snapshot snapshot = newScanSnapshot(fromSeqNo, toSeqNo, true, randomBoolean())) {
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> drainAll(snapshot));
            assertThat(
                error.getMessage(),
                containsString("Not all operations between from_seqno [" + fromSeqNo + "] and to_seqno [" + toSeqNo + "] found")
            );
        }
        // without the full range requirement the snapshot must terminate despite the enormous range
        try (Translog.Snapshot snapshot = newScanSnapshot(fromSeqNo, toSeqNo, false, randomBoolean())) {
            final List<Translog.Operation> ops = drainAll(snapshot);
            assertThat(ops, hasSize(Math.toIntExact(Math.max(0, numOps - fromSeqNo))));
        }
    }

    public void testRandomHistoryConcurrentFlushes() throws Exception {
        // random append-only history with random refreshes/flushes: whatever segment layout results, the scan
        // must return every operation exactly once in order
        final int numOps = scaledRandomIntBetween(10, 1000);
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int i = 0; i < numOps; i++) {
            final ParsedDocument doc = createParsedDoc("id-" + i, null);
            final Engine.IndexResult result = engine.index(indexForDoc(doc));
            expectedDocs.put(result.getSeqNo(), doc);
            if (rarely()) {
                if (randomBoolean()) {
                    engine.flush();
                } else {
                    engine.refresh("test");
                }
            }
        }
        engine.refresh("test");
        final long fromSeqNo = randomLongBetween(0, numOps - 1);
        final long toSeqNo = randomLongBetween(fromSeqNo, numOps - 1);
        try (LuceneSeqNoScanChangesSnapshot snapshot = newScanSnapshot(fromSeqNo, toSeqNo, true, true)) {
            assertOpsMatch(drainAll(snapshot), expectedDocs, fromSeqNo, toSeqNo);
        }
    }

    private LuceneSeqNoScanChangesSnapshot newScanSnapshot(long fromSeqNo, long toSeqNo, boolean requiredFullRange, boolean accurateCount)
        throws IOException {
        Engine.Searcher searcher = engine.acquireSearcher("test", Engine.SearcherScope.INTERNAL);
        try {
            final LuceneSeqNoScanChangesSnapshot snapshot = new LuceneSeqNoScanChangesSnapshot(
                searcher,
                fromSeqNo,
                toSeqNo,
                requiredFullRange,
                accurateCount
            );
            searcher = null;
            return snapshot;
        } finally {
            IOUtils.close(searcher);
        }
    }

    private Map<Long, ParsedDocument> indexAppendOnly(int numOps) throws IOException {
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int i = 0; i < numOps; i++) {
            final ParsedDocument doc = createParsedDoc("id-" + i, null);
            final Engine.IndexResult result = engine.index(indexForDoc(doc));
            expectedDocs.put(result.getSeqNo(), doc);
        }
        engine.refresh("test");
        return expectedDocs;
    }

    /**
     * Builds the layout produced by concurrently flushed segments: {@code stripes} segments whose seqNos
     * interleave, so that reading in seqNo order must alternate between segments.
     */
    private Map<Long, ParsedDocument> indexInterleavedSegments(int numOps, int stripes) throws IOException {
        final Map<Long, ParsedDocument> expectedDocs = new HashMap<>();
        for (int stripe = 0; stripe < stripes; stripe++) {
            for (long seqNo = stripe; seqNo < numOps; seqNo += stripes) {
                final ParsedDocument doc = createParsedDoc("id-" + seqNo, null);
                engine.index(replicaIndexForDoc(doc, 1, seqNo, false));
                expectedDocs.put(seqNo, doc);
            }
            engine.flush();
        }
        engine.refresh("test");
        return expectedDocs;
    }

    private List<Translog.Operation> drainAll(Translog.Snapshot snapshot) throws IOException {
        final List<Translog.Operation> operations = new ArrayList<>();
        Translog.Operation op;
        long lastSeqNo = -1;
        while ((op = snapshot.next()) != null) {
            assertThat("operations must be emitted in strictly ascending seqNo order", op.seqNo(), greaterThan(lastSeqNo));
            lastSeqNo = op.seqNo();
            operations.add(op);
        }
        return operations;
    }

    private void assertOpsMatch(List<Translog.Operation> ops, Map<Long, ParsedDocument> expectedDocs, long fromSeqNo, long toSeqNo) {
        assertThat(ops, hasSize(Math.toIntExact(toSeqNo - fromSeqNo + 1)));
        long expectedSeqNo = fromSeqNo;
        for (Translog.Operation op : ops) {
            assertThat(op.seqNo(), equalTo(expectedSeqNo));
            assertThat(op.toString(), op, instanceOf(Translog.Index.class));
            final Translog.Index index = (Translog.Index) op;
            final ParsedDocument expected = expectedDocs.get(op.seqNo());
            assertNotNull("unexpected seqNo [" + op.seqNo() + "]", expected);
            assertThat(index.id(), equalTo(expected.id()));
            assertThat(index.source(), equalTo(expected.source()));
            expectedSeqNo++;
        }
    }

    private static <T> T callOnNewThread(CheckedSupplier<T, Exception> supplier) throws Exception {
        final AtomicReference<T> result = new AtomicReference<>();
        final AtomicReference<Exception> failure = new AtomicReference<>();
        final Thread thread = new Thread(() -> {
            try {
                result.set(supplier.get());
            } catch (Exception e) {
                failure.set(e);
            }
        });
        thread.start();
        thread.join();
        if (failure.get() != null) {
            throw failure.get();
        }
        return result.get();
    }
}
