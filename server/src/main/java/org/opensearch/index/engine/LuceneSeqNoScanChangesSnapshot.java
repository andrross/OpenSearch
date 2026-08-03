/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.apache.lucene.document.LongPoint;
import org.apache.lucene.index.FilterLeafReader;
import org.apache.lucene.index.LeafReader;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.index.NumericDocValues;
import org.apache.lucene.index.PointValues;
import org.apache.lucene.index.StoredFields;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.lucene.index.SequentialStoredFieldsLeafReader;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.fieldvisitor.FieldsVisitor;
import org.opensearch.index.mapper.SeqNoFieldMapper;
import org.opensearch.index.mapper.SourceFieldMapper;
import org.opensearch.index.mapper.VersionFieldMapper;
import org.opensearch.index.translog.Translog;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A {@link Translog.Snapshot} from changes in a Lucene index that emits operations in strict seqNo order by
 * scanning segments in docID (storage) order and chasing the "frontier" (the next seqNo to emit), instead of
 * searching and sorting by seqNo the way {@link LuceneChangesSnapshot} does.
 *
 * <p>The motivation is stored fields I/O. Stored fields are compressed in blocks and the merge-instance
 * ("sequential") stored fields reader decompresses a block once and serves any doc in it for free, while the
 * default reader pays a decompression per document. docID order is the order blocks are laid out on disk, but
 * it only approximates seqNo order: concurrently flushed segments interleave seqNo ranges, and merges can
 * reorder whole flush cohorts within a segment. This implementation reconciles the two orders as follows:
 *
 * <ul>
 *   <li><b>Frontier</b>: all seqNos below the frontier have been emitted. Operations are only ever handed to
 *       the consumer at the frontier, which makes the output order strict by construction.
 *   <li><b>Relocation</b>: when the frontier operation is not at hand, its (segment, docID) position is found
 *       with an exact-match probe of the {@code _seq_no} BKD points index and a scan run starts there.
 *   <li><b>Scan runs</b>: a run walks a segment forward in docID order. Docs matching the frontier are emitted
 *       directly; docs a little ahead of the frontier are buffered so that the block they share with frontier
 *       docs is only decompressed once; everything else is skipped using only doc values, without touching
 *       stored fields.
 *   <li><b>Bounds</b>: the buffer of ahead-of-frontier operations is limited by a byte budget (exceeded by at
 *       most one operation), a run stops a bounded number of docs after it stops finding frontier matches, and
 *       a run also stops after a bounded number of consecutive skips. Ending a run is always safe because the
 *       main loop relocates to the frontier, and a relocated run always makes progress on its first doc.
 *   <li><b>Reader fallback</b>: layouts where seqNo order is uncorrelated with docID order (index sorting
 *       produces these) degrade to one relocation per operation once the buffer fills, and the merge-instance
 *       reader then decompresses a whole block per operation. A windowed reads-per-relocation heuristic detects
 *       this and switches to the default stored fields reader, whose partial per-doc decompression is several
 *       times cheaper; the algorithm itself is unchanged and the switch reverses with hysteresis when locality
 *       returns.
 * </ul>
 *
 * <p>Unlike {@link LuceneChangesSnapshot} there is no search phase and no per-batch sorting. On a contiguous
 * index this degenerates to a single sequential sweep; on interleaved or merge-reordered layouts it pays one
 * BKD probe per cohort boundary while still reading every stored fields block approximately once.
 *
 * @opensearch.internal
 */
final class LuceneSeqNoScanChangesSnapshot implements Translog.Snapshot {

    /** Default cap on the bytes of operations buffered ahead of the frontier. May be exceeded by one operation. */
    static final long DEFAULT_BUFFER_BUDGET_BYTES = 4 * 1024 * 1024;

    /**
     * How many docs a scan run continues past the last frontier match before relocating. Approximates "finish
     * the stored fields block we already decompressed"; Lucene does not expose block boundaries, and an estimate
     * only costs an extra block decompression (too large) or an extra relocation (too small), not correctness.
     */
    static final int TAIL_LOOKAHEAD_DOCS = 256;

    /**
     * How many consecutive docs a scan run may skip (out of range, duplicates, nested children) before giving up
     * and relocating. Bounds the doc-values-only crawl through regions that hold nothing emittable.
     */
    static final int MAX_CONSECUTIVE_SKIPS = 1024;

    /**
     * The reader-mode heuristic is evaluated every this many relocations: over the window, if fewer than
     * {@link #FALLBACK_MAX_READS_PER_RELOCATION} operations were read per relocation the snapshot is in "chase
     * mode" (each relocation yields roughly one operation, so the merge-instance reader eagerly decompresses a
     * whole block per operation) and it falls back to the default stored fields reader, whose per-doc partial
     * decompression is several times cheaper. If reads per relocation recover to
     * {@link #RESUME_MIN_READS_PER_RELOCATION} the sequential reader is resumed; the gap between the two
     * thresholds is hysteresis so the mode does not flap on mixed layouts.
     */
    static final int READER_MODE_WINDOW_RELOCATIONS = 16;
    static final int FALLBACK_MAX_READS_PER_RELOCATION = 3;
    static final int RESUME_MIN_READS_PER_RELOCATION = 8;

    /**
     * Maximum number of cached per-leaf stored fields readers. Each merge-instance reader pins up to one
     * decompressed block (roughly the codec's chunk size, or the largest document for sliced chunks), memory the
     * buffer budget does not account for; this cap bounds it regardless of segment count. Evicting a reader is
     * always safe and only costs a re-decompression if the scan returns to that leaf.
     */
    static final int MAX_CACHED_STORED_FIELDS_READERS = 8;

    private static final int TAIL_INACTIVE = -1;

    private final long fromSeqNo, toSeqNo;
    private final boolean requiredFullRange;
    private final long bufferBudgetBytes;
    private final Closeable onClose;

    private final List<LeafReaderContext> leaves;
    private final long[] leafMinSeqNo;
    private final long[] leafMaxSeqNo;
    private final int totalOps;

    private long frontier;
    private boolean exhausted;
    private int skippedOperations;

    /** Operations read ahead of the frontier, keyed by seqNo. Total size is bounded by {@link #bufferBudgetBytes}. */
    private final Map<Long, Translog.Operation> buffer = new HashMap<>();
    private long bufferBytes;

    /**
     * Stored fields readers cached per leaf ord: merge-instance readers normally (each pins at most one
     * decompressed block), default readers when {@link #useDefaultReader} is set. LRU-bounded at
     * {@link #MAX_CACHED_STORED_FIELDS_READERS} entries, and entries for leaves wholly below the frontier are
     * pruned at relocation since the scan can never read from them again. These (and the doc values iterators
     * inside a {@link ScanRun}) may only be consumed by the thread that created them, so they are dropped
     * whenever {@link #next()} is called from a different thread.
     */
    private final Map<Integer, StoredFields> storedFieldsReaders = new LinkedHashMap<>(16, 0.75f, true) {
        private static final long serialVersionUID = 1L;

        @Override
        protected boolean removeEldestEntry(Map.Entry<Integer, StoredFields> eldest) {
            return size() > MAX_CACHED_STORED_FIELDS_READERS;
        }
    };
    private Thread ownerThread;
    private ScanRun run;

    /** Whether reads go through the default stored fields reader instead of the merge instance. See the window constants. */
    private boolean useDefaultReader;
    private int windowRelocations;
    private int windowReads;

    /**
     * Creates a new "translog" snapshot from Lucene for reading operations whose seq# is in the specified range,
     * emitted in strict ascending seqNo order.
     *
     * @param engineSearcher    the internal engine searcher which will be taken over if the snapshot is opened successfully
     * @param fromSeqNo         the min requesting seq# - inclusive
     * @param toSeqNo           the maximum requesting seq# - inclusive
     * @param requiredFullRange if true, the snapshot will strictly check for the existence of operations between fromSeqNo and toSeqNo
     * @param accurateCount     if true, {@link #totalOperations()} returns an accurate count; otherwise it returns -1
     */
    LuceneSeqNoScanChangesSnapshot(
        Engine.Searcher engineSearcher,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount
    ) throws IOException {
        this(engineSearcher, fromSeqNo, toSeqNo, requiredFullRange, accurateCount, DEFAULT_BUFFER_BUDGET_BYTES);
    }

    LuceneSeqNoScanChangesSnapshot(
        Engine.Searcher engineSearcher,
        long fromSeqNo,
        long toSeqNo,
        boolean requiredFullRange,
        boolean accurateCount,
        long bufferBudgetBytes
    ) throws IOException {
        if (fromSeqNo < 0 || toSeqNo < 0 || fromSeqNo > toSeqNo) {
            throw new IllegalArgumentException("Invalid range; from_seqno [" + fromSeqNo + "], to_seqno [" + toSeqNo + "]");
        }
        if (bufferBudgetBytes <= 0) {
            throw new IllegalArgumentException("Buffer budget must be positive [" + bufferBudgetBytes + "]");
        }
        final AtomicBoolean closed = new AtomicBoolean();
        this.onClose = () -> {
            if (closed.compareAndSet(false, true)) {
                IOUtils.close(engineSearcher);
            }
        };
        this.fromSeqNo = fromSeqNo;
        this.toSeqNo = toSeqNo;
        this.requiredFullRange = requiredFullRange;
        this.bufferBudgetBytes = bufferBudgetBytes;
        this.frontier = fromSeqNo;
        this.leaves = engineSearcher.getDirectoryReader().leaves();
        this.leafMinSeqNo = new long[leaves.size()];
        this.leafMaxSeqNo = new long[leaves.size()];
        for (int i = 0; i < leaves.size(); i++) {
            final PointValues points = leaves.get(i).reader().getPointValues(SeqNoFieldMapper.NAME);
            if (points == null || points.size() == 0) {
                leafMinSeqNo[i] = Long.MAX_VALUE;
                leafMaxSeqNo[i] = Long.MIN_VALUE;
            } else {
                leafMinSeqNo[i] = LongPoint.decodeDimension(points.getMinPackedValue(), 0);
                leafMaxSeqNo[i] = LongPoint.decodeDimension(points.getMaxPackedValue(), 0);
            }
        }
        this.totalOps = accurateCount ? LuceneChangesSnapshot.countNumberOfHistoryOperations(engineSearcher, fromSeqNo, toSeqNo) : -1;
    }

    @Override
    public void close() throws IOException {
        run = null;
        storedFieldsReaders.clear();
        onClose.close();
    }

    @Override
    public int totalOperations() {
        return totalOps;
    }

    @Override
    public int skippedOperations() {
        return skippedOperations;
    }

    @Override
    public Translog.Operation next() throws IOException {
        ensureThreadOwnership();
        while (exhausted == false && frontier <= toSeqNo) {
            final Translog.Operation buffered = buffer.remove(frontier);
            if (buffered != null) {
                bufferBytes -= buffered.estimateSize();
                advanceFrontierPast(frontier);
                return buffered;
            }
            if (run == null && startRunAtFrontier() == false) {
                onFrontierMissing();
                continue;
            }
            final Translog.Operation op = advanceRun();
            if (op != null) {
                return op;
            }
        }
        return null;
    }

    /**
     * Steps the active scan run forward until it emits the frontier operation (returned) or ends (returns null,
     * after which the main loop drains the buffer or relocates). Operations ahead of the frontier are buffered
     * along the way, within the byte budget.
     */
    private Translog.Operation advanceRun() throws IOException {
        final ScanRun r = run;
        while (r.doc < r.maxDoc) {
            final int d = r.doc++;
            final long seqNo = r.seqNoOfRootDoc(d);
            if (seqNo == frontier) {
                r.tailRemaining = TAIL_INACTIVE;
                r.consecutiveSkips = 0;
                windowReads++;
                final Translog.Operation op = readOp(r, d, seqNo);
                advanceFrontierPast(seqNo);
                if (op == null) {
                    // source pruned and full range not required: consumed without emitting
                    skippedOperations++;
                    if (exhausted || frontier > toSeqNo) {
                        endRun();
                        return null;
                    }
                    continue;
                }
                return op;
            }
            if (seqNo > frontier && seqNo <= toSeqNo && buffer.containsKey(seqNo) == false) {
                if (bufferBytes >= bufferBudgetBytes) {
                    // budget reached: relocate to the frontier, which emits immediately and lets the buffer drain
                    endRun();
                    return null;
                }
                windowReads++;
                final Translog.Operation op = readOp(r, d, seqNo);
                r.consecutiveSkips = 0;
                if (op != null) {
                    buffer.put(seqNo, op);
                    bufferBytes += op.estimateSize();
                    if (r.tailRemaining == TAIL_INACTIVE) {
                        r.tailRemaining = TAIL_LOOKAHEAD_DOCS;
                    }
                }
            } else {
                // nested child (-1), out of range, already emitted, or duplicate of a buffered operation
                if (seqNo >= fromSeqNo && seqNo <= toSeqNo) {
                    skippedOperations++;
                }
                r.consecutiveSkips++;
                if (r.consecutiveSkips >= MAX_CONSECUTIVE_SKIPS) {
                    endRun();
                    return null;
                }
            }
            if (r.tailRemaining != TAIL_INACTIVE) {
                r.tailRemaining--;
                if (r.tailRemaining <= 0) {
                    endRun();
                    return null;
                }
            }
        }
        endRun();
        return null;
    }

    private void endRun() {
        run = null;
    }

    private void advanceFrontierPast(long seqNo) {
        assert seqNo == frontier : "advancing past [" + seqNo + "] but frontier is [" + frontier + "]";
        if (seqNo == Long.MAX_VALUE) {
            exhausted = true;
        } else {
            frontier = seqNo + 1;
        }
    }

    /**
     * Called when no root document with the frontier seqNo exists in the snapshot. With {@code requiredFullRange}
     * this is a missing-history failure; otherwise the frontier skips forward to the lowest existing seqNo.
     */
    private void onFrontierMissing() throws IOException {
        if (requiredFullRange) {
            throw new MissingHistoryOperationsException(
                "Not all operations between from_seqno ["
                    + fromSeqNo
                    + "] and to_seqno ["
                    + toSeqNo
                    + "] found; missing seqno ["
                    + frontier
                    + "]"
            );
        }
        if (frontier == Long.MAX_VALUE) {
            exhausted = true;
            return;
        }
        final long next = lowestExistingSeqNoAbove(frontier);
        if (next == -1) {
            exhausted = true;
        } else {
            frontier = next;
        }
    }

    /** Locates the frontier operation with a BKD probe and starts a scan run at its position. */
    private boolean startRunAtFrontier() throws IOException {
        for (int i = 0; i < leaves.size(); i++) {
            if (frontier < leafMinSeqNo[i] || frontier > leafMaxSeqNo[i]) {
                continue;
            }
            final int docID = findRootDocWithSeqNo(leaves.get(i), frontier);
            if (docID != -1) {
                recordRelocation();
                run = new ScanRun(leaves.get(i), docID);
                return true;
            }
        }
        return false;
    }

    /**
     * Finds a root (non-nested) document with exactly the given seqNo in the leaf, or -1 if there is none.
     * Nested child documents carry the parent's seqNo point but no primary term doc value, so matches are
     * verified against the primary term before being accepted.
     */
    private static int findRootDocWithSeqNo(LeafReaderContext leaf, long seqNo) throws IOException {
        final PointValues points = leaf.reader().getPointValues(SeqNoFieldMapper.NAME);
        if (points == null) {
            return -1;
        }
        final byte[] target = new byte[Long.BYTES];
        LongPoint.encodeDimension(seqNo, target, 0);
        final List<Integer> matches = new ArrayList<>();
        points.intersect(new PointValues.IntersectVisitor() {
            @Override
            public void visit(int docID) {
                matches.add(docID);
            }

            @Override
            public void visit(int docID, byte[] packedValue) {
                if (Arrays.equals(packedValue, 0, Long.BYTES, target, 0, Long.BYTES)) {
                    matches.add(docID);
                }
            }

            @Override
            public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                if (Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, target, 0, Long.BYTES) < 0
                    || Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, target, 0, Long.BYTES) > 0) {
                    return PointValues.Relation.CELL_OUTSIDE_QUERY;
                }
                if (Arrays.compareUnsigned(minPackedValue, 0, Long.BYTES, target, 0, Long.BYTES) == 0
                    && Arrays.compareUnsigned(maxPackedValue, 0, Long.BYTES, target, 0, Long.BYTES) == 0) {
                    return PointValues.Relation.CELL_INSIDE_QUERY;
                }
                return PointValues.Relation.CELL_CROSSES_QUERY;
            }
        });
        if (matches.isEmpty()) {
            return -1;
        }
        matches.sort(Comparator.naturalOrder());
        final NumericDocValues primaryTermDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME);
        if (primaryTermDV == null) {
            return -1;
        }
        for (int docID : matches) {
            if (primaryTermDV.advanceExact(docID)) {
                return docID;
            }
        }
        return -1;
    }

    /**
     * Finds the lowest seqNo strictly above the given one that exists in the snapshot, or -1 if there is none up
     * to {@code toSeqNo}. Probes exponentially widening windows so that sparse gaps cost a handful of narrow BKD
     * visits instead of one visit over the entire remaining range.
     */
    private long lowestExistingSeqNoAbove(long seqNo) throws IOException {
        long lo = seqNo + 1;
        long width = 64;
        while (lo > 0 && lo <= toSeqNo) {
            final long hi = toSeqNo - lo < width - 1 ? toSeqNo : lo + width - 1;
            final long found = minExistingSeqNoInRange(lo, hi);
            if (found != -1) {
                return found;
            }
            if (hi == toSeqNo) {
                return -1;
            }
            lo = hi + 1;
            if (width <= 1L << 30) {
                width *= 2;
            }
        }
        return -1;
    }

    private long minExistingSeqNoInRange(long lo, long hi) throws IOException {
        long best = -1;
        for (int i = 0; i < leaves.size(); i++) {
            if (hi < leafMinSeqNo[i] || lo > leafMaxSeqNo[i]) {
                continue;
            }
            final PointValues points = leaves.get(i).reader().getPointValues(SeqNoFieldMapper.NAME);
            if (points == null) {
                continue;
            }
            final long[] leafBest = { -1 };
            points.intersect(new PointValues.IntersectVisitor() {
                @Override
                public void visit(int docID) {
                    // unreachable: compare() never returns CELL_INSIDE_QUERY, so values always come to visit(docID, packedValue)
                }

                @Override
                public void visit(int docID, byte[] packedValue) {
                    final long value = LongPoint.decodeDimension(packedValue, 0);
                    if (value >= lo && value <= hi && (leafBest[0] == -1 || value < leafBest[0])) {
                        leafBest[0] = value;
                    }
                }

                @Override
                public PointValues.Relation compare(byte[] minPackedValue, byte[] maxPackedValue) {
                    final long min = LongPoint.decodeDimension(minPackedValue, 0);
                    final long max = LongPoint.decodeDimension(maxPackedValue, 0);
                    if (max < lo || min > hi) {
                        return PointValues.Relation.CELL_OUTSIDE_QUERY;
                    }
                    // report CROSSES even for contained cells so the values are pushed to the visitor
                    return PointValues.Relation.CELL_CROSSES_QUERY;
                }
            });
            if (leafBest[0] != -1 && (best == -1 || leafBest[0] < best)) {
                best = leafBest[0];
            }
        }
        return best;
    }

    /**
     * Materializes the operation for the given doc. Returns null when the doc has neither source nor
     * recovery source and the full range is not required (the source has been pruned by retention).
     */
    private Translog.Operation readOp(ScanRun r, int segmentDocID, long seqNo) throws IOException {
        final long primaryTerm = r.primaryTermDV.longValue();
        assert primaryTerm > 0 : "nested child document must be excluded";
        if (r.versionDV.advanceExact(segmentDocID) == false) {
            throw new IllegalStateException("DocValues for field [" + VersionFieldMapper.NAME + "] is not found");
        }
        final long version = r.versionDV.longValue();
        final boolean isTombstone = r.tombstoneDV != null && r.tombstoneDV.advanceExact(segmentDocID) && r.tombstoneDV.longValue() > 0;
        final boolean hasRecoverySource = r.recoverySourceDV != null && r.recoverySourceDV.advanceExact(segmentDocID);
        final String sourceField = hasRecoverySource ? SourceFieldMapper.RECOVERY_SOURCE_NAME : SourceFieldMapper.NAME;
        final FieldsVisitor fields = new FieldsVisitor(true, sourceField);
        storedFieldsFor(r.leaf).document(segmentDocID, fields);

        final Translog.Operation op;
        if (isTombstone && fields.id() == null) {
            op = new Translog.NoOp(seqNo, primaryTerm, fields.source().utf8ToString());
            assert version == 1L : "Noop tombstone should have version 1L; actual version [" + version + "]";
            assert assertDocSoftDeleted(r.leaf.reader(), segmentDocID) : "Noop but soft_deletes field is not set [" + op + "]";
        } else if (isTombstone) {
            op = new Translog.Delete(fields.id(), seqNo, primaryTerm, version);
            assert assertDocSoftDeleted(r.leaf.reader(), segmentDocID) : "Delete op but soft_deletes field is not set [" + op + "]";
        } else {
            final BytesReference source = fields.source();
            if (source == null) {
                if (requiredFullRange) {
                    throw new MissingHistoryOperationsException(
                        "source not found for seqno=" + seqNo + " from_seqno=" + fromSeqNo + " to_seqno=" + toSeqNo
                    );
                }
                return null;
            }
            // TODO: pass the latest timestamp from engine.
            final long autoGeneratedIdTimestamp = -1;
            op = new Translog.Index(
                fields.id(),
                seqNo,
                primaryTerm,
                version,
                source.toBytesRef().bytes,
                fields.routing(),
                autoGeneratedIdTimestamp
            );
        }
        return op;
    }

    private StoredFields storedFieldsFor(LeafReaderContext leaf) throws IOException {
        StoredFields storedFields = storedFieldsReaders.get(leaf.ord);
        if (storedFields == null) {
            storedFields = useDefaultReader ? leaf.reader().storedFields() : sequentialStoredFields(leaf.reader());
            storedFieldsReaders.put(leaf.ord, storedFields);
        }
        return storedFields;
    }

    /**
     * Counts a relocation toward the reader-mode window and re-evaluates the mode when the window is full. Called
     * on every successful relocation, so a snapshot that never relocates (a contiguous layout) never re-evaluates
     * and keeps the sequential reader.
     */
    private void recordRelocation() {
        pruneReadersBelowFrontier();
        windowRelocations++;
        if (windowRelocations < READER_MODE_WINDOW_RELOCATIONS) {
            return;
        }
        if (useDefaultReader == false && windowReads < FALLBACK_MAX_READS_PER_RELOCATION * windowRelocations) {
            useDefaultReader = true;
            storedFieldsReaders.clear();
        } else if (useDefaultReader && windowReads >= RESUME_MIN_READS_PER_RELOCATION * windowRelocations) {
            useDefaultReader = false;
            storedFieldsReaders.clear();
        }
        windowRelocations = 0;
        windowReads = 0;
    }

    /**
     * Drops cached readers for leaves whose entire seqNo range is below the frontier: the scan can never read
     * from them again, so their pinned blocks are dead weight.
     */
    private void pruneReadersBelowFrontier() {
        storedFieldsReaders.keySet().removeIf(ord -> leafMaxSeqNo[ord] < frontier);
    }

    // for testing
    boolean isUsingDefaultStoredFieldsReader() {
        return useDefaultReader;
    }

    // for testing
    int cachedStoredFieldsReaderCount() {
        return storedFieldsReaders.size();
    }

    /**
     * Returns a merge-optimized (sequential) stored fields reader for the leaf
     */
    private static StoredFields sequentialStoredFields(LeafReader leaf) throws IOException {
        LeafReader reader = leaf;
        while (true) {
            if (reader instanceof SequentialStoredFieldsLeafReader) {
                return ((SequentialStoredFieldsLeafReader) reader).getSequentialStoredFieldsReader();
            }
            if (reader instanceof FilterLeafReader == false) {
                return leaf.storedFields();
            }
            reader = ((FilterLeafReader) reader).getDelegate();
        }
    }

    /**
     * Stored fields readers and doc values iterators may only be consumed by the thread that created them, so
     * they are dropped when a different thread takes over. Aborting the active run is safe: the main loop
     * relocates to the frontier.
     */
    private void ensureThreadOwnership() {
        final Thread currentThread = Thread.currentThread();
        if (ownerThread != currentThread) {
            ownerThread = currentThread;
            run = null;
            storedFieldsReaders.clear();
        }
    }

    private boolean assertDocSoftDeleted(LeafReader leafReader, int segmentDocId) throws IOException {
        final NumericDocValues ndv = leafReader.getNumericDocValues(Lucene.SOFT_DELETES_FIELD);
        if (ndv == null || ndv.advanceExact(segmentDocId) == false) {
            throw new IllegalStateException("DocValues for field [" + Lucene.SOFT_DELETES_FIELD + "] is not found");
        }
        return ndv.longValue() == 1;
    }

    /**
     * A forward walk over one leaf, started at a relocation target. Doc values iterators are forward-only, so a
     * run owns fresh iterators; the (block-caching) stored fields readers are cached on the snapshot instead so
     * that returning to a partially consumed block does not decompress it again.
     */
    private static final class ScanRun {
        final LeafReaderContext leaf;
        final int maxDoc;
        final NumericDocValues seqNoDV;
        final NumericDocValues primaryTermDV;
        final NumericDocValues versionDV;
        final NumericDocValues tombstoneDV;
        final NumericDocValues recoverySourceDV;
        int doc;
        int tailRemaining = TAIL_INACTIVE;
        int consecutiveSkips;

        ScanRun(LeafReaderContext leaf, int startDoc) throws IOException {
            this.leaf = leaf;
            this.maxDoc = leaf.reader().maxDoc();
            this.seqNoDV = Objects.requireNonNull(leaf.reader().getNumericDocValues(SeqNoFieldMapper.NAME), "SeqNoDV is missing");
            this.primaryTermDV = Objects.requireNonNull(
                leaf.reader().getNumericDocValues(SeqNoFieldMapper.PRIMARY_TERM_NAME),
                "PrimaryTermDV is missing"
            );
            this.versionDV = Objects.requireNonNull(leaf.reader().getNumericDocValues(VersionFieldMapper.NAME), "VersionDV is missing");
            this.tombstoneDV = leaf.reader().getNumericDocValues(SeqNoFieldMapper.TOMBSTONE_NAME);
            this.recoverySourceDV = leaf.reader().getNumericDocValues(SourceFieldMapper.RECOVERY_SOURCE_NAME);
            this.doc = startDoc;
        }

        /**
         * Returns the seqNo of the given doc, or -1 if it is not a root document (nested children carry no
         * primary term doc value). Must be called with ascending docIDs within a run.
         */
        long seqNoOfRootDoc(int d) throws IOException {
            if (seqNoDV.advanceExact(d) == false || primaryTermDV.advanceExact(d) == false) {
                return -1;
            }
            return seqNoDV.longValue();
        }
    }
}
