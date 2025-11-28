package fr.pierrezemb.rl.list;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.ByteString;
import fr.pierrezemb.rl.protos.ListEntry;
import fr.pierrezemb.rl.protos.ListEntryRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * Demonstrates implementing ordered list operations using FDB Record Layer.
 *
 * <h2>Linked List Time Complexity:</h2>
 * <ul>
 *   <li>lpush/rpush: O(1) - add to head/tail</li>
 *   <li>lpop/rpop: O(1) - remove from head/tail</li>
 *   <li>llen: O(1) - get length</li>
 *   <li>lindex: O(N) - traverse to position</li>
 *   <li>lset: O(N) - traverse to position</li>
 *   <li>lrange: O(S+N) - S=offset, N=range size</li>
 *   <li>linsert: O(N) - find pivot entry</li>
 *   <li>lrem: O(N) - scan for matches</li>
 * </ul>
 *
 * <h2>FDB Record Layer Time Complexity (skip-list RANK index):</h2>
 * <ul>
 *   <li>lpush/rpush: O(log N) - insert with position calculation</li>
 *   <li>lpop/rpop: O(log N) - find by rank and delete</li>
 *   <li>llen: O(1) - COUNT index</li>
 *   <li><b>lindex: O(log N)</b> - direct rank lookup (BETTER!)</li>
 *   <li><b>lset: O(log N)</b> - find by rank and update (BETTER!)</li>
 *   <li>lrange: O(log N + K) - K=range size</li>
 *   <li>linsert: O(N) - still need to scan for pivot</li>
 *   <li>lrem: O(N) - still need to scan for matches</li>
 * </ul>
 *
 * <h2>Key Design Decision:</h2>
 * Each list entry is stored as a separate record with a sparse position value.
 * The RANK index on position (grouped by list_key) enables O(log N) access by index.
 */
class ListOperationsTest {

    // Gap between position values for new entries
    // Using large gap allows many insertions before needing rebalancing
    private static final long POSITION_GAP = 1_000_000L;

    // Initial position for first entry (centered to allow lpush/rpush)
    // Using Long.MAX_VALUE / 2 gives equal room on both sides
    private static final long INITIAL_POSITION = Long.MAX_VALUE / 2;

    /**
     * RANK index on position, grouped by list_key.
     * This is the KEY index that enables O(log N) position-based access.
     *
     * The skip-list data structure maintains:
     * - BY_VALUE: ordered by (list_key, position) for range scans
     * - BY_RANK: ordinal positions within each list_key group
     */
    private static final Index IDX_POSITION_RANK = new Index(
            "idx-position-rank",
            field("position").groupBy(field("list_key")),
            IndexTypes.RANK);

    /**
     * COUNT index grouped by list_key for O(1) llen operations.
     */
    private static final Index IDX_LIST_COUNT = new Index(
            "idx-list-count",
            field("entry_id").groupBy(field("list_key")),
            IndexTypes.COUNT_NOT_NULL);

    /**
     * VALUE index on (list_key, value) for lrem and linsert pivot lookup.
     * Used when we need to find entries by their value.
     */
    private static final Index IDX_VALUE = new Index(
            "idx-value",
            concat(field("list_key"), field("value")));

    private FDBDatabase db;
    private Function<FDBRecordContext, FDBRecordStore> recordStoreProvider;
    private KeySpacePath path;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        KeySpace keySpace = new KeySpace(new DirectoryLayerDirectory("tests-rl"));
        path = keySpace.path("tests-rl", "list-ops");

        RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder()
                .setRecords(ListEntryRecord.getDescriptor());
        setupListEntry(metadataBuilder);

        RecordMetaData recordMetadata = metadataBuilder.build();

        db = FDBDatabaseFactory.instance().getDatabase();
        db.performNoOpAsync().get(2, TimeUnit.SECONDS);

        // Clear existing data
        db.run(ctx -> {
            ctx.ensureActive().clear(new Range(new byte[]{0x00}, new byte[]{(byte) 0xFE}));
            return null;
        });

        recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetadata)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();
    }

    private void setupListEntry(RecordMetaDataBuilder metadataBuilder) {
        // Primary key: (list_key, entry_id)
        metadataBuilder.getRecordType("ListEntry").setPrimaryKey(concat(
                field("list_key"),
                field("entry_id")));

        metadataBuilder.addIndex("ListEntry", IDX_POSITION_RANK);
        metadataBuilder.addIndex("ListEntry", IDX_LIST_COUNT);
        metadataBuilder.addIndex("ListEntry", IDX_VALUE);
    }

    // ==================== LIST OPERATIONS ====================

    /**
     * rpush key entry [entry ...]
     * Append one or multiple entries to the tail of the list.
     *
     * Linked List: O(1) for each entry
     * FDB: O(log N) - need to find max position via RANK index
     *
     * @return the length of the list after the push
     */
    private long rpush(FDBRecordStore store, String listKey, byte[]... values) {
        // Find current max position (tail)
        long maxPosition = getMaxPosition(store, listKey);

        for (byte[] value : values) {
            maxPosition += POSITION_GAP;
            ListEntry entry = ListEntry.newBuilder()
                    .setListKey(listKey)
                    .setEntryId(UUID.randomUUID().toString())
                    .setPosition(maxPosition)
                    .setValue(ByteString.copyFrom(value))
                    .build();
            store.saveRecord(entry);
        }

        return llen(store, listKey);
    }

    /**
     * lpush key entry [entry ...]
     * Prepend one or multiple entries to the head of the list.
     *
     * Linked List: O(1) for each entry
     * FDB: O(log N) - need to find min position via RANK index
     *
     * @return the length of the list after the push
     */
    private long lpush(FDBRecordStore store, String listKey, byte[]... values) {
        // Find current min position (head)
        long minPosition = getMinPosition(store, listKey);

        for (byte[] value : values) {
            minPosition -= POSITION_GAP;
            ListEntry entry = ListEntry.newBuilder()
                    .setListKey(listKey)
                    .setEntryId(UUID.randomUUID().toString())
                    .setPosition(minPosition)
                    .setValue(ByteString.copyFrom(value))
                    .build();
            store.saveRecord(entry);
        }

        return llen(store, listKey);
    }

    /**
     * llen key
     * Returns the length of the list.
     *
     * Linked List: O(1)
     * FDB: O(1) - using COUNT index
     */
    private long llen(FDBRecordStore store, String listKey) {
        return store.scanIndex(
                        IDX_LIST_COUNT,
                        IndexScanType.BY_GROUP,
                        TupleRange.allOf(Tuple.from(listKey)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join()
                .map(entry -> entry.getValue().getLong(0))
                .orElse(0L);
    }

    /**
     * lindex key index
     * Returns the entry at index in the list.
     * Negative indices count from the end (-1 is the last entry).
     *
     * Linked List: O(N) - must traverse the list
     * FDB: O(log N) - direct RANK index lookup!
     */
    private byte[] lindex(FDBRecordStore store, String listKey, long index) {
        long len = llen(store, listKey);
        if (len == 0) return null;

        // Handle negative indices
        long actualIndex = index >= 0 ? index : len + index;
        if (actualIndex < 0 || actualIndex >= len) return null;

        // BY_RANK scan at specific position - O(log N)!
        var entryOpt = store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.allOf(Tuple.from(listKey, actualIndex)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join();

        if (entryOpt.isEmpty()) return null;

        var record = store.loadRecord(entryOpt.get().getPrimaryKey());
        if (record == null) return null;

        ListEntry entry = ListEntry.newBuilder().mergeFrom(record.getRecord()).build();
        return entry.getValue().toByteArray();
    }

    /**
     * lrange key start stop
     * Returns the specified entries of the list.
     * Negative indices count from the end.
     *
     * Linked List: O(S+N) where S is offset distance, N is range size
     * FDB: O(log N + K) where K is range size - better for large offsets!
     */
    private List<byte[]> lrange(FDBRecordStore store, String listKey, long start, long stop) {
        long len = llen(store, listKey);
        if (len == 0) return List.of();

        // Handle negative indices
        long actualStart = start >= 0 ? start : Math.max(0, len + start);
        long actualStop = stop >= 0 ? Math.min(stop, len - 1) : len + stop;

        if (actualStart > actualStop || actualStart >= len) return List.of();

        List<byte[]> result = new ArrayList<>();

        // BY_RANK range scan - O(log N) to find start position
        store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.between(
                                Tuple.from(listKey, actualStart),
                                Tuple.from(listKey, actualStop + 1)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .forEach(indexEntry -> {
                    var record = store.loadRecord(indexEntry.getPrimaryKey());
                    if (record != null) {
                        ListEntry entry = ListEntry.newBuilder()
                                .mergeFrom(record.getRecord()).build();
                        result.add(entry.getValue().toByteArray());
                    }
                }).join();

        return result;
    }

    /**
     * lpop key
     * Removes and returns the first entry of the list.
     *
     * Linked List: O(1)
     * FDB: O(log N) - find by rank 0, then delete
     */
    private byte[] lpop(FDBRecordStore store, String listKey) {
        return store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.allOf(Tuple.from(listKey, 0L)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join()
                .flatMap(indexEntry -> {
                    var record = store.loadRecord(indexEntry.getPrimaryKey());
                    if (record != null) {
                        ListEntry entry = ListEntry.newBuilder()
                                .mergeFrom(record.getRecord()).build();
                        store.deleteRecord(indexEntry.getPrimaryKey());
                        return java.util.Optional.of(entry.getValue().toByteArray());
                    }
                    return java.util.Optional.empty();
                })
                .orElse(null);
    }

    /**
     * rpop key
     * Removes and returns the last entry of the list.
     *
     * Linked List: O(1)
     * FDB: O(log N) - find max rank, then delete
     */
    private byte[] rpop(FDBRecordStore store, String listKey) {
        long len = llen(store, listKey);
        if (len == 0) return null;

        return store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.allOf(Tuple.from(listKey, len - 1)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join()
                .flatMap(indexEntry -> {
                    var record = store.loadRecord(indexEntry.getPrimaryKey());
                    if (record != null) {
                        ListEntry entry = ListEntry.newBuilder()
                                .mergeFrom(record.getRecord()).build();
                        store.deleteRecord(indexEntry.getPrimaryKey());
                        return java.util.Optional.of(entry.getValue().toByteArray());
                    }
                    return java.util.Optional.empty();
                })
                .orElse(null);
    }

    /**
     * lset key index value
     * Sets the list entry at index to value.
     *
     * Linked List: O(N) - must traverse to position
     * FDB: O(log N) - direct RANK lookup!
     */
    private boolean lset(FDBRecordStore store, String listKey, long index, byte[] value) {
        long len = llen(store, listKey);
        if (len == 0) return false;

        long actualIndex = index >= 0 ? index : len + index;
        if (actualIndex < 0 || actualIndex >= len) return false;

        // Find entry at position - O(log N)
        return store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.allOf(Tuple.from(listKey, actualIndex)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join()
                .map(indexEntry -> {
                    var record = store.loadRecord(indexEntry.getPrimaryKey());
                    if (record != null) {
                        ListEntry oldEntry = ListEntry.newBuilder()
                                .mergeFrom(record.getRecord()).build();
                        // Update the value, keeping same position
                        ListEntry newEntry = oldEntry.toBuilder()
                                .setValue(ByteString.copyFrom(value))
                                .build();
                        store.saveRecord(newEntry);
                        return true;
                    }
                    return false;
                })
                .orElse(false);
    }

    /**
     * linsert key BEFORE|AFTER pivot value
     * Inserts value before or after the pivot entry.
     *
     * Linked List: O(N) - must scan to find pivot
     * FDB: O(N) - still need to scan by value (using VALUE index helps)
     *
     * @param before true for BEFORE, false for AFTER
     * @return length of list after insert, or -1 if pivot not found
     */
    private long linsert(FDBRecordStore store, String listKey, boolean before, byte[] pivot, byte[] value) {
        // Find the pivot entry by value - O(N) in worst case
        var pivotIndexEntry = store.scanIndex(
                        IDX_VALUE,
                        IndexScanType.BY_VALUE,
                        TupleRange.allOf(Tuple.from(listKey, pivot)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join();

        if (pivotIndexEntry.isEmpty()) {
            return -1; // Pivot not found
        }

        var pivotRecord = store.loadRecord(pivotIndexEntry.get().getPrimaryKey());
        if (pivotRecord == null) {
            return -1;
        }

        ListEntry pivotEntry = ListEntry.newBuilder()
                .mergeFrom(pivotRecord.getRecord()).build();
        long pivotPosition = pivotEntry.getPosition();

        // Find adjacent entry to calculate new position
        long newPosition;
        if (before) {
            // Get previous entry's position
            Long prevPosition = getPreviousPosition(store, listKey, pivotPosition);
            newPosition = prevPosition != null
                    ? (prevPosition + pivotPosition) / 2
                    : pivotPosition - POSITION_GAP;
        } else {
            // Get next entry's position
            Long nextPosition = getNextPosition(store, listKey, pivotPosition);
            newPosition = nextPosition != null
                    ? (pivotPosition + nextPosition) / 2
                    : pivotPosition + POSITION_GAP;
        }

        ListEntry newEntry = ListEntry.newBuilder()
                .setListKey(listKey)
                .setEntryId(UUID.randomUUID().toString())
                .setPosition(newPosition)
                .setValue(ByteString.copyFrom(value))
                .build();
        store.saveRecord(newEntry);

        return llen(store, listKey);
    }

    /**
     * lrem key count value
     * Removes count occurrences of value from the list.
     * count > 0: remove from head
     * count < 0: remove from tail
     * count = 0: remove all
     *
     * Linked List: O(N+M) where N is list length and M is number of entries removed
     * FDB: O(N) - need to scan entire list (VALUE index helps filter)
     *
     * @return number of entries removed
     */
    private long lrem(FDBRecordStore store, String listKey, long count, byte[] value) {
        List<Tuple> toDelete = new ArrayList<>();

        if (count == 0) {
            // Remove all matching entries
            store.scanIndex(
                            IDX_VALUE,
                            IndexScanType.BY_VALUE,
                            TupleRange.allOf(Tuple.from(listKey, value)),
                            null,
                            ScanProperties.FORWARD_SCAN)
                    .forEach(indexEntry -> toDelete.add(indexEntry.getPrimaryKey()))
                    .join();
        } else {
            // Need to traverse in order (by position) to respect count direction
            ScanProperties scanProps = count > 0
                    ? ScanProperties.FORWARD_SCAN
                    : ScanProperties.REVERSE_SCAN;
            long remaining = Math.abs(count);

            // Scan all entries in position order
            store.scanIndex(
                            IDX_POSITION_RANK,
                            IndexScanType.BY_VALUE,
                            TupleRange.allOf(Tuple.from(listKey)),
                            null,
                            scanProps)
                    .forEach(indexEntry -> {
                        if (toDelete.size() < remaining) {
                            var record = store.loadRecord(indexEntry.getPrimaryKey());
                            if (record != null) {
                                ListEntry entry = ListEntry.newBuilder()
                                        .mergeFrom(record.getRecord()).build();
                                if (Arrays.equals(entry.getValue().toByteArray(), value)) {
                                    toDelete.add(indexEntry.getPrimaryKey());
                                }
                            }
                        }
                    })
                    .join();
        }

        // Delete all found entries
        for (Tuple pk : toDelete) {
            store.deleteRecord(pk);
        }

        return toDelete.size();
    }

    // ==================== HELPER METHODS ====================

    private long getMaxPosition(FDBRecordStore store, String listKey) {
        long len = llen(store, listKey);
        if (len == 0) return 0;

        // Get position of last entry (rank = len - 1)
        var entryOpt = store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.allOf(Tuple.from(listKey, len - 1)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join();

        if (entryOpt.isEmpty()) return 0;

        var record = store.loadRecord(entryOpt.get().getPrimaryKey());
        if (record == null) return 0;

        return ListEntry.newBuilder().mergeFrom(record.getRecord()).build().getPosition();
    }

    private long getMinPosition(FDBRecordStore store, String listKey) {
        long len = llen(store, listKey);
        if (len == 0) return INITIAL_POSITION;

        // Get position of first entry (rank = 0)
        var entryOpt = store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_RANK,
                        TupleRange.allOf(Tuple.from(listKey, 0L)),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join();

        if (entryOpt.isEmpty()) return INITIAL_POSITION;

        var record = store.loadRecord(entryOpt.get().getPrimaryKey());
        if (record == null) return INITIAL_POSITION;

        return ListEntry.newBuilder().mergeFrom(record.getRecord()).build().getPosition();
    }

    private Long getPreviousPosition(FDBRecordStore store, String listKey, long currentPosition) {
        // Scan BY_VALUE to find entry just before currentPosition
        var entryOpt = store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_VALUE,
                        TupleRange.between(
                                Tuple.from(listKey, Long.MIN_VALUE),
                                Tuple.from(listKey, currentPosition)),
                        null,
                        ScanProperties.REVERSE_SCAN)
                .first()
                .join();

        if (entryOpt.isEmpty()) return null;

        var record = store.loadRecord(entryOpt.get().getPrimaryKey());
        if (record == null) return null;

        return ListEntry.newBuilder().mergeFrom(record.getRecord()).build().getPosition();
    }

    private Long getNextPosition(FDBRecordStore store, String listKey, long currentPosition) {
        // Scan BY_VALUE to find entry just after currentPosition
        var entryOpt = store.scanIndex(
                        IDX_POSITION_RANK,
                        IndexScanType.BY_VALUE,
                        new TupleRange(
                                Tuple.from(listKey, currentPosition),
                                Tuple.from(listKey, Long.MAX_VALUE),
                                com.apple.foundationdb.record.EndpointType.RANGE_EXCLUSIVE,
                                com.apple.foundationdb.record.EndpointType.RANGE_INCLUSIVE),
                        null,
                        ScanProperties.FORWARD_SCAN)
                .first()
                .join();

        if (entryOpt.isEmpty()) return null;

        var record = store.loadRecord(entryOpt.get().getPrimaryKey());
        if (record == null) return null;

        return ListEntry.newBuilder().mergeFrom(record.getRecord()).build().getPosition();
    }

    private byte[] toBytes(String s) {
        return s.getBytes(StandardCharsets.UTF_8);
    }

    private String fromBytes(byte[] bytes) {
        return bytes == null ? null : new String(bytes, StandardCharsets.UTF_8);
    }

    // ==================== TESTS ====================

    @Test
    void testRpushAndLrange() {
        System.out.println("\n========== Test: rpush and lrange ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            // rpush mylist "a" "b" "c"
            long len = rpush(store, "mylist", toBytes("a"), toBytes("b"), toBytes("c"));
            System.out.println("rpush mylist a b c -> " + len);
            Assertions.assertEquals(3, len);

            // lrange mylist 0 -1
            List<byte[]> all = lrange(store, "mylist", 0, -1);
            System.out.println("lrange mylist 0 -1 -> " + all.stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("a", "b", "c"), all.stream().map(this::fromBytes).toList());

            return null;
        });
    }

    @Test
    void testLpush() {
        System.out.println("\n========== Test: lpush ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            // rpush mylist "world"
            rpush(store, "mylist", toBytes("world"));

            // lpush mylist "hello"
            long len = lpush(store, "mylist", toBytes("hello"));
            System.out.println("lpush mylist hello -> " + len);
            Assertions.assertEquals(2, len);

            // lrange mylist 0 -1
            List<byte[]> all = lrange(store, "mylist", 0, -1);
            System.out.println("lrange mylist 0 -1 -> " + all.stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("hello", "world"), all.stream().map(this::fromBytes).toList());

            return null;
        });
    }

    @Test
    void testLindex() {
        System.out.println("\n========== Test: lindex (O(log N) vs Linked List O(N)) ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            // Create a list with multiple entries
            rpush(store, "mylist", toBytes("a"), toBytes("b"), toBytes("c"), toBytes("d"), toBytes("e"));

            // Test various indices
            System.out.println("lindex mylist 0 -> " + fromBytes(lindex(store, "mylist", 0)));
            Assertions.assertEquals("a", fromBytes(lindex(store, "mylist", 0)));

            System.out.println("lindex mylist 2 -> " + fromBytes(lindex(store, "mylist", 2)));
            Assertions.assertEquals("c", fromBytes(lindex(store, "mylist", 2)));

            System.out.println("lindex mylist -1 -> " + fromBytes(lindex(store, "mylist", -1)));
            Assertions.assertEquals("e", fromBytes(lindex(store, "mylist", -1)));

            System.out.println("lindex mylist -2 -> " + fromBytes(lindex(store, "mylist", -2)));
            Assertions.assertEquals("d", fromBytes(lindex(store, "mylist", -2)));

            System.out.println("lindex mylist 10 -> " + fromBytes(lindex(store, "mylist", 10)));
            Assertions.assertNull(lindex(store, "mylist", 10));

            return null;
        });
    }

    @Test
    void testLpopRpop() {
        System.out.println("\n========== Test: lpop and rpop ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            rpush(store, "mylist", toBytes("one"), toBytes("two"), toBytes("three"));

            // lpop
            byte[] popped = lpop(store, "mylist");
            System.out.println("lpop mylist -> " + fromBytes(popped));
            Assertions.assertEquals("one", fromBytes(popped));

            // rpop
            popped = rpop(store, "mylist");
            System.out.println("rpop mylist -> " + fromBytes(popped));
            Assertions.assertEquals("three", fromBytes(popped));

            // Check remaining
            List<byte[]> remaining = lrange(store, "mylist", 0, -1);
            System.out.println("lrange mylist 0 -1 -> " + remaining.stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("two"), remaining.stream().map(this::fromBytes).toList());

            return null;
        });
    }

    @Test
    void testLset() {
        System.out.println("\n========== Test: lset (O(log N) vs Linked List O(N)) ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            rpush(store, "mylist", toBytes("one"), toBytes("two"), toBytes("three"));

            // lset mylist 1 "TWO"
            boolean success = lset(store, "mylist", 1, toBytes("TWO"));
            System.out.println("lset mylist 1 TWO -> " + success);
            Assertions.assertTrue(success);

            // lset mylist -1 "THREE"
            success = lset(store, "mylist", -1, toBytes("THREE"));
            System.out.println("lset mylist -1 THREE -> " + success);
            Assertions.assertTrue(success);

            // Verify
            List<byte[]> all = lrange(store, "mylist", 0, -1);
            System.out.println("lrange mylist 0 -1 -> " + all.stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("one", "TWO", "THREE"), all.stream().map(this::fromBytes).toList());

            return null;
        });
    }

    @Test
    void testLinsert() {
        System.out.println("\n========== Test: linsert ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            rpush(store, "mylist", toBytes("Hello"), toBytes("World"));

            // linsert mylist BEFORE "World" "There"
            long len = linsert(store, "mylist", true, toBytes("World"), toBytes("There"));
            System.out.println("linsert mylist BEFORE World There -> " + len);
            Assertions.assertEquals(3, len);

            // linsert mylist AFTER "World" "!"
            len = linsert(store, "mylist", false, toBytes("World"), toBytes("!"));
            System.out.println("linsert mylist AFTER World ! -> " + len);
            Assertions.assertEquals(4, len);

            // Verify
            List<byte[]> all = lrange(store, "mylist", 0, -1);
            System.out.println("lrange mylist 0 -1 -> " + all.stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("Hello", "There", "World", "!"),
                    all.stream().map(this::fromBytes).toList());

            return null;
        });
    }

    @Test
    void testLrem() {
        System.out.println("\n========== Test: lrem ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            rpush(store, "mylist",
                    toBytes("hello"), toBytes("hello"), toBytes("foo"),
                    toBytes("hello"), toBytes("hello"));

            // lrem mylist 2 "hello" (remove 2 from head)
            long removed = lrem(store, "mylist", 2, toBytes("hello"));
            System.out.println("lrem mylist 2 hello -> " + removed);
            Assertions.assertEquals(2, removed);

            List<byte[]> all = lrange(store, "mylist", 0, -1);
            System.out.println("lrange mylist 0 -1 -> " + all.stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("foo", "hello", "hello"),
                    all.stream().map(this::fromBytes).toList());

            return null;
        });
    }

    @Test
    void testLlen() {
        System.out.println("\n========== Test: llen ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            System.out.println("llen mylist (empty) -> " + llen(store, "mylist"));
            Assertions.assertEquals(0, llen(store, "mylist"));

            rpush(store, "mylist", toBytes("a"), toBytes("b"), toBytes("c"));

            System.out.println("llen mylist -> " + llen(store, "mylist"));
            Assertions.assertEquals(3, llen(store, "mylist"));

            return null;
        });
    }

    @Test
    void testMultipleLists() {
        System.out.println("\n========== Test: Multiple Lists ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            // Create multiple lists
            rpush(store, "list1", toBytes("a"), toBytes("b"));
            rpush(store, "list2", toBytes("x"), toBytes("y"), toBytes("z"));
            lpush(store, "list3", toBytes("3"), toBytes("2"), toBytes("1"));

            // Verify each list is independent
            Assertions.assertEquals(2, llen(store, "list1"));
            Assertions.assertEquals(3, llen(store, "list2"));
            Assertions.assertEquals(3, llen(store, "list3"));

            System.out.println("list1: " + lrange(store, "list1", 0, -1).stream().map(this::fromBytes).toList());
            System.out.println("list2: " + lrange(store, "list2", 0, -1).stream().map(this::fromBytes).toList());
            System.out.println("list3: " + lrange(store, "list3", 0, -1).stream().map(this::fromBytes).toList());

            Assertions.assertEquals(List.of("a", "b"),
                    lrange(store, "list1", 0, -1).stream().map(this::fromBytes).toList());
            Assertions.assertEquals(List.of("x", "y", "z"),
                    lrange(store, "list2", 0, -1).stream().map(this::fromBytes).toList());
            // lpush reverses the order
            Assertions.assertEquals(List.of("1", "2", "3"),
                    lrange(store, "list3", 0, -1).stream().map(this::fromBytes).toList());

            return null;
        });
    }

    /**
     * Demonstrates the key advantage of FDB: O(log N) random access
     * vs linked list's O(N) for lindex in the middle of large lists.
     */
    @Test
    void testLargeListRandomAccess() {
        System.out.println("\n========== Test: Large List Random Access ==========");
        System.out.println("This demonstrates O(log N) lindex vs Linked List's O(N)");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            // Create a larger list
            int size = 100;
            byte[][] entries = new byte[size][];
            for (int i = 0; i < size; i++) {
                entries[i] = toBytes("entry-" + i);
            }
            rpush(store, "biglist", entries);

            System.out.println("Created list with " + size + " entries");

            // Access middle entry - O(log N) in FDB vs O(N) in linked list!
            long startTime = System.nanoTime();
            byte[] middle = lindex(store, "biglist", 50);
            long endTime = System.nanoTime();

            System.out.println("lindex biglist 50 -> " + fromBytes(middle));
            System.out.println("Access time: " + (endTime - startTime) / 1000 + " microseconds");

            Assertions.assertEquals("entry-50", fromBytes(middle));

            // Access last entry
            startTime = System.nanoTime();
            byte[] last = lindex(store, "biglist", -1);
            endTime = System.nanoTime();

            System.out.println("lindex biglist -1 -> " + fromBytes(last));
            System.out.println("Access time: " + (endTime - startTime) / 1000 + " microseconds");

            Assertions.assertEquals("entry-99", fromBytes(last));

            return null;
        });
    }

    /**
     * Dumps the internal FDB key-value structure for debugging.
     */
    @Test
    void testDumpKeyValuePairs() {
        System.out.println("\n========== Test: Dump Key-Value Pairs ==========");

        db.run(ctx -> {
            FDBRecordStore store = recordStoreProvider.apply(ctx);

            // Create a small list
            rpush(store, "dumplist", toBytes("first"), toBytes("second"), toBytes("third"));

            return null;
        });

        // Dump in separate transaction
        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);
            Subspace subspace = recordStore.getSubspace();

            System.out.println("\nRaw key-value pairs:");
            System.out.println("Prefix meanings: 0=STORE_INFO, 1=RECORD, 2=INDEX (B-tree), 3=INDEX (skip-list for RANK)");

            AsyncIterable<KeyValue> kvs = context.ensureActive().getRange(subspace.range());
            kvs.forEach(kv -> {
                Tuple key = Tuple.fromBytes(kv.getKey());
                System.out.println("Key: " + key + ", Value: " + Arrays.toString(kv.getValue()));
            });

            return null;
        });
    }

    /**
     * Test complexity comparison summary
     */
    @Test
    void testComplexitySummary() {
        System.out.println("\n========== List Operations: Complexity Comparison ==========");
        System.out.println();
        System.out.println("| Operation | Linked List         | FDB (RANK Index)    | Winner |");
        System.out.println("|-----------|---------------------|---------------------|--------|");
        System.out.println("| lpush     | O(1)                | O(log N)            | Linked |");
        System.out.println("| rpush     | O(1)                | O(log N)            | Linked |");
        System.out.println("| lpop      | O(1)                | O(log N)            | Linked |");
        System.out.println("| rpop      | O(1)                | O(log N)            | Linked |");
        System.out.println("| llen      | O(1)                | O(1) (COUNT index)  | Tie    |");
        System.out.println("| lindex    | O(N)                | O(log N)            | FDB    |");
        System.out.println("| lset      | O(N)                | O(log N)            | FDB    |");
        System.out.println("| lrange    | O(S+N)              | O(log N + K)        | FDB*   |");
        System.out.println("| linsert   | O(N)                | O(N)                | Tie    |");
        System.out.println("| lrem      | O(N)                | O(N)                | Tie    |");
        System.out.println();
        System.out.println("* lrange: FDB wins for large offsets (S), Linked List wins for small offsets");
        System.out.println();
        System.out.println("Key insight: FDB excels at random access in large lists!");
        System.out.println("Trade-off: Head/tail operations are O(log N) instead of O(1)");
    }
}
