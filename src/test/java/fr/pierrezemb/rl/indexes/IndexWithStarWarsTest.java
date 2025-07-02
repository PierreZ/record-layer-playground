package fr.pierrezemb.rl.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.record.*;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import fr.pierrezemb.rl.protos.Person;
import fr.pierrezemb.rl.protos.PersonRecord;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;

/**
 * Demonstrates the creation, manipulation, and querying of indexes in a FoundationDB Record Layer.
 * <p>
 * This test class serves as a reference for using the Record Layer, showcasing various index types and query patterns.
 * It uses characters from Star Wars as test data.
 *
 * <h2>Key Concepts Illustrated:</h2>
 * <ul>
 *   <li>Defining and assigning indexes with different configurations (e.g., grouped, versioned).</li>
 *   <li>Saving, loading, and deleting records in an {@link FDBRecordStore}.</li>
 *   <li>Executing queries and scans using defined indexes.</li>
 *   <li>Using {@link Tuple} for grouping and ranges.</li>
 *   <li>Setting up record metadata, including primary keys and index configurations.</li>
 *   <li>Validating results through assertions and logging.</li>
 * </ul>
 *
 * <h2>Indexes Used:</h2>
 * <ul>
 *   <li>{@link #COUNT_FAMILY_MEMBER_INDEX}: A grouped count index on family members.</li>
 *   <li>{@link #MAX_EVER_FAMILY_MEMBER_INDEX}: An index tracking the maximum age per family.</li>
 *   <li>{@link #MAX_EVER_INDEX}: An index tracking the overall maximum age.</li>
 *   <li>{@link #VERSION_INDEX}: A record version index.</li>
 *   <li>{@link #IDX_AGE_INDEX}: A simple value index on the age field.</li>
 *   <li>{@link #IDX_RANK_AGE}: A rank index on midichlorians.</li>
 * </ul>
 *
 * <p>
 * All database operations are executed within a FoundationDB transactional context.
 *
 * <h2>Test Data (Star Wars Characters):</h2>
 * <pre>
 * - Ben Solo: 18
 * - Leia Skywalker: 20
 * - Luke Skywalker: 21
 * - Anakin Skywalker: 50
 * - Han Solo: 60
 * - Obi-Wan Kenobi: 80
 * </pre>
 */
class IndexWithStarWarsTest {

    /**
     * Index that counts the number of members in each family (grouped by last name).
     * <pre><b>Example Key/Value:</b>
     * {@code
     * Key: (17, 2, "idx-count-family-member", "Skywalker"), Value: [3, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 2, "idx-count-family-member", "Solo"), Value: [2, 0, 0, 0, 0, 0, 0, 0]
     * }
     * </pre>
     */
    private static final Index COUNT_FAMILY_MEMBER_INDEX = new Index(
            "idx-count-family-member",
            Key.Expressions.field("first_name").groupBy(Key.Expressions.field("last_name")),
            IndexTypes.COUNT_NOT_NULL);

    /**
     * Index that tracks the maximum age ever recorded across all persons.
     * <pre><b>Example Key/Value:</b>
     * {@code
     * Key: (17, 2, "idx-max-age-ever"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
     * }
     * </pre>
     */
    private static final Index MAX_EVER_INDEX = new Index(
            "idx-max-age-ever",
            Key.Expressions.field("age").groupBy(Key.Expressions.empty()),
            IndexTypes.MAX_EVER_LONG
    );

    /**
     * Index that tracks the maximum age for each family.
     * <pre><b>Example Key/Value:</b>
     * {@code
     * Key: (17, 2, "idx-max-age-by-family", "Skywalker"), Value: [50, 0, 0, 0, 0, 0, 0, 0]
     * }
     * </pre>
     */
    private static final Index MAX_EVER_FAMILY_MEMBER_INDEX = new Index(
            "idx-max-age-by-family",
            Key.Expressions.field("age").groupBy(Key.Expressions.field("last_name")),
            IndexTypes.MAX_EVER_LONG
    );

    /**
     * A {@link IndexTypes#VERSION} index to track record changes over time.
     * <pre><b>Example Key/Value:</b>
     * {@code
     * Key: (17, 2, "idx-version-index", Versionstamp(...), "Skywalker", "Anakin"), Value: []
     * }
     * </pre>
     */
    private static final Index VERSION_INDEX = new Index(
            "idx-version-index",
            VersionKeyExpression.VERSION,
            IndexTypes.VERSION);

    /**
     * A simple value index on the {@code age} field.
     * <pre><b>Example Key/Value:</b>
     * {@code
     * Key: (17, 2, "idx-age", 18, "Solo", "Ben"), Value: []
     * Key: (17, 2, "idx-age", 20, "Skywalker", "Leia"), Value: []
     * }
     * </pre>
     */
    private static final Index IDX_AGE_INDEX = new Index("idx-age", Key.Expressions.field("age"));

    /**
     * A {@link IndexTypes#RANK} index on the {@code midichlorians} field.
     * This index uses a two-subspace architecture to be efficient at two different kinds of operations:
     * looking up records by their *value* and looking up records by their *position* or *rank*.
     * <p>
     * The <b>Primary Subspace</b> is a standard B-Tree index on the value itself (e.g. `midichlorians`).
     * It is identified by the subspace ID `2` in the key tuple and allows for fast `BY_VALUE` scans.
     * <p>
     * The <b>Secondary Subspace</b> is a `RankedSet` that maintains the ranking information. It is implemented as a persistent skip-list, a probabilistic data structure that allows for fast search, insertion, and deletion (`O(log N)`) in an ordered sequence. The keys with subspace ID `3` represent the internal nodes of this data structure.
     * It is identified by the subspace ID `3` in the key tuple and allows for fast `BY_RANK` scans.
     * <pre><b>Example Key/Value:</b>
     * {@code
     * Key: (17, 2, "idx-rang-age", 1, "Solo", "Han"), Value: []
     * Key: (17, 2, "idx-rang-age", 8, "Skywalker", "Leia"), Value: []
     * Key: (17, 2, "idx-rang-age", 10, "Solo", "Ben"), Value: []
     * Key: (17, 2, "idx-rang-age", 17, "Kenobi", "Obi-Wan"), Value: []
     * Key: (17, 2, "idx-rang-age", 20, "Skywalker", "Anakin"), Value: []
     * Key: (17, 2, "idx-rang-age", 30, "Skywalker", "Luke"), Value: []
     * Key: (17, 3, "idx-rang-age", 0, b""), Value: [0, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 0, b"\x15\x01"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 0, b"\x15\x08"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 0, b"\x15\x0a"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 0, b"\x15\x11"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 0, b"\x15\x14"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 0, b"\x15\x1e"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 1, b""), Value: [4, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 1, b"\x15\x14"), Value: [2, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 2, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 3, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 4, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 3, "idx-rang-age", 5, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
     * }
     * </pre>
     */
    private static final Index IDX_RANK_AGE = new Index("idx-rang-age",
            Key.Expressions.field("midichlorians").ungrouped(),
            IndexTypes.RANK);

    /**
     * Runs the main test case, demonstrating index usage.
     * This test performs the following steps:
     * <ol>
     *   <li>Sets up the database, keyspace, and record metadata.</li>
     *   <li>Clears any existing data.</li>
     *   <li>Saves a list of {@link Person} records.</li>
     *   <li>Queries the {@link #COUNT_FAMILY_MEMBER_INDEX} to get the count of members per family.</li>
     *   <li>Queries for people younger than 30 using the {@link #IDX_AGE_INDEX}.</li>
     *   <li>Queries using the {@link #IDX_RANK_AGE} to get people ranked by midichlorians.</li>
     *   <li>Dumps all raw key-value pairs to the console for inspection.</li>
     * </ol>
     *
     * @throws ExecutionException   if a future completes exceptionally
     * @throws InterruptedException if the current thread is interrupted
     * @throws TimeoutException     if a future times out
     */
    @Test
    void testIndex() throws ExecutionException, InterruptedException, TimeoutException {

        System.out.println("start testIndex");
        // Define the keyspace for our application
        KeySpace keySpace = new KeySpace(new DirectoryLayerDirectory("tests-rl"));

        // Get the path where our record store will be rooted
        KeySpacePath path = keySpace.path("tests-rl", "skywalkers");

        RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder()
                .setRecords(PersonRecord.getDescriptor());
        setupPerson(metadataBuilder);

        RecordMetaData recordMetadata = metadataBuilder.build();

        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();
        db.performNoOpAsync().get(2, TimeUnit.SECONDS);

        db.run(ctx -> {
            System.out.println("clearing the db!");
            ctx.ensureActive().clear(new Range(new byte[]{0x00}, new byte[]{(byte) 0xFE}));
            return null;
        });

        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetadata)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();

        List<Person> skywalkers = new ArrayList<>();
        skywalkers.add(Person.newBuilder().setFirstName("Anakin").setLastName("Skywalker").setMidichlorians(20).setAge(50).build());
        skywalkers.add(Person.newBuilder().setFirstName("Luke").setLastName("Skywalker").setMidichlorians(30).setAge(21).build());
        skywalkers.add(Person.newBuilder().setFirstName("Leia").setLastName("Skywalker").setMidichlorians(8).setAge(20).build());
        skywalkers.add(Person.newBuilder().setFirstName("Obi-Wan").setLastName("Kenobi").setMidichlorians(17).setAge(80).build());
        skywalkers.add(Person.newBuilder().setFirstName("Ben").setLastName("Solo").setMidichlorians(10).setAge(18).build());
        skywalkers.add(Person.newBuilder().setFirstName("Han").setLastName("Solo").setMidichlorians(1).setAge(60).build());

        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);
            for (Person skywalker : skywalkers) {
                recordStore.saveRecord(skywalker);
            }

            return null;
        });

        System.err.println("\n---------- get count per families ----------\n");

        // Get count of skywalkers
        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);
            List<String> families = new ArrayList<>();
            families.add("Solo");
            families.add("Kenobi");
            families.add("Skywalker");

            for (String family : families) {
                Tuple group = Tuple.from(family);
                boolean reverse = false;

                /**
                 * Will output:
                 * ```
                 * printing the index COUNT_FAMILY_MEMBER_INDEX:
                 * Index key: '("Solo")', index value: '(2)'
                 * Index key: '("Kenobi")', index value: '(1)'
                 * Index key: '("Skywalker")', index value: '(3)'
                 * ```
                 */
                recordStore.scanIndex(COUNT_FAMILY_MEMBER_INDEX, IndexScanType.BY_GROUP,
                                TupleRange.allOf(group), null, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN)
                        .asList().join().forEach(indexEntry -> System.out.println("Index key: '" + indexEntry.getKey() + "', index value: '" + indexEntry.getValue() + "'"));
            }
            return null;
        });

        System.out.println("\n------- get young people by age -------\n");

        // get young skywalkers
        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);

            RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(RecordQuery.newBuilder()
                    .setRecordType("Person")
                    .setFilter(Query.field("age").lessThan(30)).build());

            List<Person> result = cursor
                    .map(queriedRecord -> Person.newBuilder()
                            .mergeFrom(queriedRecord.getRecord()).build())
                    .asList().join();

            System.out.println("result: " + result);
            Assertions.assertEquals(3, result.size());
            return null;
        });

        System.out.println("\n------- get people using a rank on midichlorians -------\n");

        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);

            RecordCursor<FDBQueriedRecord<Message>> cursor = recordStore.executeQuery(RecordQuery.newBuilder()
                    .setRecordType("Person")
                    .setFilter(Query.rank("midichlorians").greaterThanOrEquals(0)).build());

            List<Person> result = cursor
                    .map(queriedRecord -> Person.newBuilder()
                            .mergeFrom(queriedRecord.getRecord()).build())
                    .asList().join();

            System.out.println("result: " + result);
            Assertions.assertEquals(6, result.size());
            // assert the order
            Assertions.assertEquals("Han", result.get(0).getFirstName());
            Assertions.assertEquals("Leia", result.get(1).getFirstName());
            Assertions.assertEquals("Ben", result.get(2).getFirstName());
            Assertions.assertEquals("Obi-Wan", result.get(3).getFirstName());
            Assertions.assertEquals("Anakin", result.get(4).getFirstName());
            Assertions.assertEquals("Luke", result.get(5).getFirstName());
            return null;
        });

        System.out.println("\n------- dump raw key-value pairs -------\n");

        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);
            Subspace subspace = recordStore.getSubspace();

            AsyncIterable<KeyValue> kvs = context.ensureActive().getRange(subspace.range());
            kvs.forEach(kv -> {
                Tuple key = Tuple.fromBytes(kv.getKey());
                /**
                 * Recap of prefixes:
                 * - element 0 is the "tenant",
                 * - element 1 is the RecordStore prefix, corresponding to FDBRecordStoreKeyspace
                 *   - 0 is STORE_INFO
                 *   - 1 is RECORD
                 *   - 2 is INDEX
                 * Will output:
                 * ```
                 * Key: (38, 0), Value: [8, 7, 16, 7, 40, -83, -63, -37, -36, -4, 50]
                 * Key: (38, 1, "Kenobi", "Obi-Wan", -1), Value: [51, 0, 0, 0, 0, 120, 42, -89, 56, 0, 0, 0, 3]
                 * Key: (38, 1, "Kenobi", "Obi-Wan", 0), Value: [10, 21, 10, 7, 79, 98, 105, 45, 87, 97, 110, 18, 6, 75, 101, 110, 111, 98, 105, 24, 80, 32, 17]
                 * Key: (38, 1, "Skywalker", "Anakin", -1), Value: [51, 0, 0, 0, 0, 120, 42, -89, 56, 0, 0, 0, 0]
                 * Key: (38, 1, "Skywalker", "Anakin", 0), Value: [10, 23, 10, 6, 65, 110, 97, 107, 105, 110, 18, 9, 83, 107, 121, 119, 97, 108, 107, 101, 114, 24, 50, 32, 20]
                 * Key: (38, 1, "Skywalker", "Leia", -1), Value: [51, 0, 0, 0, 0, 120, 42, -89, 56, 0, 0, 0, 2]
                 * Key: (38, 1, "Skywalker", "Leia", 0), Value: [10, 21, 10, 4, 76, 101, 105, 97, 18, 9, 83, 107, 121, 119, 97, 108, 107, 101, 114, 24, 20, 32, 8]
                 * Key: (38, 1, "Skywalker", "Luke", -1), Value: [51, 0, 0, 0, 0, 120, 42, -89, 56, 0, 0, 0, 1]
                 * Key: (38, 1, "Skywalker", "Luke", 0), Value: [10, 21, 10, 4, 76, 117, 107, 101, 18, 9, 83, 107, 121, 119, 97, 108, 107, 101, 114, 24, 21, 32, 30]
                 * Key: (38, 1, "Solo", "Ben", -1), Value: [51, 0, 0, 0, 0, 120, 42, -89, 56, 0, 0, 0, 4]
                 * Key: (38, 1, "Solo", "Ben", 0), Value: [10, 15, 10, 3, 66, 101, 110, 18, 4, 83, 111, 108, 111, 24, 18, 32, 10]
                 * Key: (38, 1, "Solo", "Han", -1), Value: [51, 0, 0, 0, 0, 120, 42, -89, 56, 0, 0, 0, 5]
                 * Key: (38, 1, "Solo", "Han", 0), Value: [10, 15, 10, 3, 72, 97, 110, 18, 4, 83, 111, 108, 111, 24, 60, 32, 1]
                 * Key: (38, 2, "idx-age", 18, "Solo", "Ben"), Value: []
                 * Key: (38, 2, "idx-age", 20, "Skywalker", "Leia"), Value: []
                 * Key: (38, 2, "idx-age", 21, "Skywalker", "Luke"), Value: []
                 * Key: (38, 2, "idx-age", 50, "Skywalker", "Anakin"), Value: []
                 * Key: (38, 2, "idx-age", 60, "Solo", "Han"), Value: []
                 * Key: (38, 2, "idx-age", 80, "Kenobi", "Obi-Wan"), Value: []
                 * Key: (38, 2, "idx-count-family-member", "Kenobi"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-count-family-member", "Skywalker"), Value: [3, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-count-family-member", "Solo"), Value: [2, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-max-age-by-family", "Kenobi"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-max-age-by-family", "Skywalker"), Value: [50, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-max-age-by-family", "Solo"), Value: [60, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-max-age-ever"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 2, "idx-rang-age", 1, "Solo", "Han"), Value: []
                 * Key: (38, 2, "idx-rang-age", 8, "Skywalker", "Leia"), Value: []
                 * Key: (38, 2, "idx-rang-age", 10, "Solo", "Ben"), Value: []
                 * Key: (38, 2, "idx-rang-age", 17, "Kenobi", "Obi-Wan"), Value: []
                 * Key: (38, 2, "idx-rang-age", 20, "Skywalker", "Anakin"), Value: []
                 * Key: (38, 2, "idx-rang-age", 30, "Skywalker", "Luke"), Value: []
                 * Key: (38, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x00x*\xa78\x00\x00 0), "Skywalker", "Anakin"), Value: []
                 * Key: (38, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x00x*\xa78\x00\x00 1), "Skywalker", "Luke"), Value: []
                 * Key: (38, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x00x*\xa78\x00\x00 2), "Skywalker", "Leia"), Value: []
                 * Key: (38, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x00x*\xa78\x00\x00 3), "Kenobi", "Obi-Wan"), Value: []
                 * Key: (38, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x00x*\xa78\x00\x00 4), "Solo", "Ben"), Value: []
                 * Key: (38, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x00x*\xa78\x00\x00 5), "Solo", "Han"), Value: []
                 * Key: (38, 3, "idx-rang-age", 0, b""), Value: [0, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 0, b"\x15\x01"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 0, b"\x15\x08"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 0, b"\x15\x0a"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 0, b"\x15\x11"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 0, b"\x15\x14"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 0, b"\x15\x1e"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 1, b""), Value: [4, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 1, b"\x15\x14"), Value: [2, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 2, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 3, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 4, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (38, 3, "idx-rang-age", 5, b""), Value: [6, 0, 0, 0, 0, 0, 0, 0]
                 * ```
                 */
                System.out.println("Key: " + key + ", Value: " + Arrays.toString(kv.getValue()));
            });


            return null;
        });
    }

    /**
     * Configures the metadata for the {@code Person} record type.
     * This includes setting the primary key and adding all the indexes used in this test.
     *
     * @param metadataBuilder The metadata builder to configure.
     */
    private void setupPerson(RecordMetaDataBuilder metadataBuilder) {
        metadataBuilder.getRecordType("Person").setPrimaryKey(concat(
                com.apple.foundationdb.record.metadata.Key.Expressions.field("last_name"),
                com.apple.foundationdb.record.metadata.Key.Expressions.field("first_name")));

        metadataBuilder.setStoreRecordVersions(true);
        metadataBuilder.addIndex("Person", VERSION_INDEX);
        metadataBuilder.addIndex("Person", COUNT_FAMILY_MEMBER_INDEX);
        metadataBuilder.addIndex("Person", IDX_AGE_INDEX);
        metadataBuilder.addIndex("Person", MAX_EVER_FAMILY_MEMBER_INDEX);
        metadataBuilder.addIndex("Person", MAX_EVER_INDEX);
        metadataBuilder.addIndex("Person", IDX_RANK_AGE);
    }
}