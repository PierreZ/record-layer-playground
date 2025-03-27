package fr.pierrezemb.rl.indexes;

import com.apple.foundationdb.KeyValue;
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

/**
 * IndexWithSkyWalkerTest is a test class that demonstrates the creation, manipulation, and querying of indices
 * within an FDBRecordStore using FoundationDB. The test cases focus on operations related to different index types
 * and scenarios involving record storage and retrieval.
 * <p>
 * This class illustrates several key concepts:
 * 1. Definition and assigning of indices with different configurations (e.g., grouped indices, record version indices).
 * 2. Loading, saving, and clearing records within an FDBRecordStore.
 * 3. Executing queries and scans using the defined indices.
 * 4. Using Tuple for grouping and ranging while interacting with stored records.
 * 5. Setting up metadata for records, including primary key definitions and index configurations.
 * 6. Validation of results through assertions and standard outputs to verify expected database behaviors.
 * <p>
 * The indices used in this test include:
 * - COUNT_FAMILY_MEMBER_INDEX: A grouped count index that counts members within a family.
 * - MAX_EVER_FAMILY_MEMBER_INDEX: An index for tracking the maximum age value for family members.
 * - VERSION_INDEX: A record version index to track changes in records.
 * - IDX_AGE_INDEX: A basic index that enables queries based on the `age` field.
 * <p>
 * All database operations within this test are executed in a FoundationDB transactional context. The class provides
 * a procedural flow for organizing data transactions, inserting and querying data, and validating the results.
 * <p>
 * List of Star wars characters:
 * - Ben Solo: 18
 * - Leia Skywalker: 20
 * - Luke Skywalker: 21
 * - Anakin Skywalker: 50
 * - Han Solo: 60
 * - Obi-Wan Kenobi: 80
 */
class IndexWithStarWarsTest {

    /**
     * Key: (17, 2, "idx-count-family-member", "Kenobi"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 2, "idx-count-family-member", "Skywalker"), Value: [3, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 2, "idx-count-family-member", "Solo"), Value: [2, 0, 0, 0, 0, 0, 0, 0]
     */
    private static final Index COUNT_FAMILY_MEMBER_INDEX = new Index(
            "idx-count-family-member",
            Key.Expressions.field("first_name").groupBy(Key.Expressions.field("last_name")),
            IndexTypes.COUNT_NOT_NULL);

    /**
     * Key: (17, 2, "idx-max-age-ever"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
     */
    private static final Index MAX_EVER_INDEX = new Index(
            "idx-max-age-ever",
            Key.Expressions.field("age").groupBy(Key.Expressions.empty()),
            IndexTypes.MAX_EVER_LONG
    );

    /**
     * Key: (17, 2, "idx-max-age-by-family", "Kenobi"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 2, "idx-max-age-by-family", "Skywalker"), Value: [50, 0, 0, 0, 0, 0, 0, 0]
     * Key: (17, 2, "idx-max-age-by-family", "Solo"), Value: [60, 0, 0, 0, 0, 0, 0, 0]
     */
    private static final Index MAX_EVER_FAMILY_MEMBER_INDEX = new Index(
            "idx-max-age-by-family",
            Key.Expressions.field("age").groupBy(Key.Expressions.field("last_name")),
            IndexTypes.MAX_EVER_LONG
    );

    /**
     * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xe1\xd7%\xc1\x00\x00 0), "Skywalker", "Anakin"), Value: []
     * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xe1\xd7%\xc1\x00\x00 1), "Skywalker", "Luke"), Value: []
     * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xe1\xd7%\xc1\x00\x00 2), "Skywalker", "Leia"), Value: []
     * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xe1\xd7%\xc1\x00\x00 3), "Kenobi", "Obi-Wan"), Value: []
     * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xe1\xd7%\xc1\x00\x00 4), "Solo", "Ben"), Value: []
     * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xe1\xd7%\xc1\x00\x00 5), "Solo", "Han"), Value: []
     */
    private static final Index VERSION_INDEX = new Index(
            "idx-version-index",
            VersionKeyExpression.VERSION,
            IndexTypes.VERSION);

    /**
     * Key: (17, 2, "idx-age", 18, "Solo", "Ben"), Value: []
     * Key: (17, 2, "idx-age", 20, "Skywalker", "Leia"), Value: []
     * Key: (17, 2, "idx-age", 21, "Skywalker", "Luke"), Value: []
     * Key: (17, 2, "idx-age", 50, "Skywalker", "Anakin"), Value: []
     * Key: (17, 2, "idx-age", 60, "Solo", "Han"), Value: []
     * Key: (17, 2, "idx-age", 80, "Kenobi", "Obi-Wan"), Value: []
     */
    private static final Index IDX_AGE_INDEX = new Index("idx-age", Key.Expressions.field("age"));

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

        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetadata)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();

        // clear the range
        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);
            ctx.ensureActive().clear(recordStore.getSubspace().range());
            return null;
        });

        List<Person> skywalkers = new ArrayList<>();
        skywalkers.add(Person.newBuilder().setFirstName("Anakin").setLastName("Skywalker").setAge(50).build());
        skywalkers.add(Person.newBuilder().setFirstName("Luke").setLastName("Skywalker").setAge(21).build());
        skywalkers.add(Person.newBuilder().setFirstName("Leia").setLastName("Skywalker").setAge(20).build());
        skywalkers.add(Person.newBuilder().setFirstName("Obi-Wan").setLastName("Kenobi").setAge(80).build());
        skywalkers.add(Person.newBuilder().setFirstName("Ben").setLastName("Solo").setAge(18).build());
        skywalkers.add(Person.newBuilder().setFirstName("Han").setLastName("Solo").setAge(60).build());


        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);

            for (Person skywalker : skywalkers) {
                recordStore.saveRecord(skywalker);
            }

            return null;
        });

        // Get count of skywalkers
        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);
            List<String> families = new ArrayList<>();
            families.add("Solo");
            families.add("Kenobi");
            families.add("Skywalker");

            System.out.println("printing the index COUNT_FAMILY_MEMBER_INDEX:");

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

        System.out.println("-------");

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

            /**
             * Will output:
             * ```
             * first_name: "Ben"
             * last_name: "Solo"
             * age: 18
             *
             * first_name: "Leia"
             * last_name: "Skywalker"
             * age: 20
             *
             * first_name: "Luke"
             * last_name: "Skywalker"
             * age: 21
             * ```
             */
            result.forEach(System.out::println);
            Assertions.assertEquals(3, result.size());


            return null;
        });

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
                 * Key: (17, 0), Value: [8, 7, 16, 6, 40, -78, -91, -88, -74, -35, 50]
                 * Key: (17, 1, "Kenobi", "Obi-Wan", -1), Value: [51, 0, 0, 0, 7, -54, -67, -24, 95, 0, 0, 0, 3]
                 * Key: (17, 1, "Kenobi", "Obi-Wan", 0), Value: [10, 19, 10, 7, 79, 98, 105, 45, 87, 97, 110, 18, 6, 75, 101, 110, 111, 98, 105, 24, 80]
                 * Key: (17, 1, "Skywalker", "Anakin", -1), Value: [51, 0, 0, 0, 7, -54, -67, -24, 95, 0, 0, 0, 0]
                 * Key: (17, 1, "Skywalker", "Anakin", 0), Value: [10, 21, 10, 6, 65, 110, 97, 107, 105, 110, 18, 9, 83, 107, 121, 119, 97, 108, 107, 101, 114, 24, 50]
                 * Key: (17, 1, "Skywalker", "Leia", -1), Value: [51, 0, 0, 0, 7, -54, -67, -24, 95, 0, 0, 0, 2]
                 * Key: (17, 1, "Skywalker", "Leia", 0), Value: [10, 19, 10, 4, 76, 101, 105, 97, 18, 9, 83, 107, 121, 119, 97, 108, 107, 101, 114, 24, 20]
                 * Key: (17, 1, "Skywalker", "Luke", -1), Value: [51, 0, 0, 0, 7, -54, -67, -24, 95, 0, 0, 0, 1]
                 * Key: (17, 1, "Skywalker", "Luke", 0), Value: [10, 19, 10, 4, 76, 117, 107, 101, 18, 9, 83, 107, 121, 119, 97, 108, 107, 101, 114, 24, 21]
                 * Key: (17, 1, "Solo", "Ben", -1), Value: [51, 0, 0, 0, 7, -54, -67, -24, 95, 0, 0, 0, 4]
                 * Key: (17, 1, "Solo", "Ben", 0), Value: [10, 13, 10, 3, 66, 101, 110, 18, 4, 83, 111, 108, 111, 24, 18]
                 * Key: (17, 1, "Solo", "Han", -1), Value: [51, 0, 0, 0, 7, -54, -67, -24, 95, 0, 0, 0, 5]
                 * Key: (17, 1, "Solo", "Han", 0), Value: [10, 13, 10, 3, 72, 97, 110, 18, 4, 83, 111, 108, 111, 24, 60]
                 * Key: (17, 2, "idx-age", 18, "Solo", "Ben"), Value: []
                 * Key: (17, 2, "idx-age", 20, "Skywalker", "Leia"), Value: []
                 * Key: (17, 2, "idx-age", 21, "Skywalker", "Luke"), Value: []
                 * Key: (17, 2, "idx-age", 50, "Skywalker", "Anakin"), Value: []
                 * Key: (17, 2, "idx-age", 60, "Solo", "Han"), Value: []
                 * Key: (17, 2, "idx-age", 80, "Kenobi", "Obi-Wan"), Value: []
                 * Key: (17, 2, "idx-count-family-member", "Kenobi"), Value: [1, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-count-family-member", "Skywalker"), Value: [3, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-count-family-member", "Solo"), Value: [2, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-max-age-by-family", "Kenobi"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-max-age-by-family", "Skywalker"), Value: [50, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-max-age-by-family", "Solo"), Value: [60, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-max-age-ever"), Value: [80, 0, 0, 0, 0, 0, 0, 0]
                 * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xca\xbd\xe8_\x00\x00 0), "Skywalker", "Anakin"), Value: []
                 * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xca\xbd\xe8_\x00\x00 1), "Skywalker", "Luke"), Value: []
                 * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xca\xbd\xe8_\x00\x00 2), "Skywalker", "Leia"), Value: []
                 * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xca\xbd\xe8_\x00\x00 3), "Kenobi", "Obi-Wan"), Value: []
                 * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xca\xbd\xe8_\x00\x00 4), "Solo", "Ben"), Value: []
                 * Key: (17, 2, "idx-version-index", Versionstamp(\x00\x00\x00\x07\xca\xbd\xe8_\x00\x00 5), "Solo", "Han"), Value: []
                 * ```
                 */
                System.out.println("Key: " + key + ", Value: " + Arrays.toString(kv.getValue()));
            });


            return null;
        });
    }


    private void setupPerson(RecordMetaDataBuilder metadataBuilder) {
        metadataBuilder.getRecordType("Person").setPrimaryKey(com.apple.foundationdb.record.metadata.Key.Expressions.concat(
                com.apple.foundationdb.record.metadata.Key.Expressions.field("last_name"),
                com.apple.foundationdb.record.metadata.Key.Expressions.field("first_name")));

        metadataBuilder.setStoreRecordVersions(true);
        metadataBuilder.addIndex("Person", VERSION_INDEX);
        metadataBuilder.addIndex("Person", COUNT_FAMILY_MEMBER_INDEX);
        metadataBuilder.addIndex("Person", IDX_AGE_INDEX);
        metadataBuilder.addIndex("Person", MAX_EVER_FAMILY_MEMBER_INDEX);
        metadataBuilder.addIndex("Person", MAX_EVER_INDEX);
    }
}