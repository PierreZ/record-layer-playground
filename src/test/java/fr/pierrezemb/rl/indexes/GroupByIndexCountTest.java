package fr.pierrezemb.rl.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.record.*;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
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

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

class GroupByIndexCountTest {
    private static final Index COUNT_FAMILY_MEMBER_INDEX = new Index(
            "count-family-member",
            Key.Expressions.field("first_name").groupBy(Key.Expressions.field("last_name")),
            IndexTypes.COUNT_NOT_NULL);

    private static final Index IDX_AGE_INDEX = new Index("idx-age", Key.Expressions.field("age"));

    @Test
    void testIndex() throws ExecutionException, InterruptedException, TimeoutException {
        System.out.println("start testIndex");
        // Define the keyspace for our application
        KeySpace keySpace = new KeySpace(new DirectoryLayerDirectory("tests-rl"));

        // Get the path where our record store will be rooted
        KeySpacePath path = keySpace.path("tests-rl", "index-count");

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

        Person anakin = Person.newBuilder().setFirstName("Anakin").setLastName("Skywalker").setAge(50).build();
        Person luke = Person.newBuilder().setFirstName("Luke").setLastName("Skywalker").setAge(20).build();
        Person leia = Person.newBuilder().setFirstName("Leia").setLastName("Skywalker").setAge(20).build();

        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);

            recordStore.saveRecord(anakin);
            recordStore.saveRecord(luke);
            recordStore.saveRecord(leia);

            return null;
        });

        // Get count of skywalkers
        db.run(ctx -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(ctx);
            Tuple group = Tuple.from(leia.getLastName());
            boolean reverse = false;

            recordStore.scanIndex(COUNT_FAMILY_MEMBER_INDEX, IndexScanType.BY_GROUP,
                            TupleRange.allOf(group), null, reverse ? ScanProperties.REVERSE_SCAN : ScanProperties.FORWARD_SCAN)
                    .asList().join().forEach(indexEntry -> {
                        System.out.println("key: " + indexEntry.getKey());
                        System.out.println("value: " + indexEntry.getValue());
                    });

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

            Assertions.assertEquals(2, result.size());

            result.forEach(System.out::println);

            return null;
        });

        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);
            Subspace subspace = recordStore.getSubspace();

            // TODO: scan all keys
            AsyncIterable<KeyValue> kvs = context.ensureActive().getRange(subspace.range());
            kvs.forEach(kv -> {
                Tuple key = Tuple.fromBytes(kv.getKey());
                System.out.println("Key: " + key + ", Value: " + Arrays.toString(kv.getValue()));
            });

            return null;
        });
    }


    private void setupPerson(RecordMetaDataBuilder metadataBuilder) {
        metadataBuilder.getRecordType("Person").setPrimaryKey(com.apple.foundationdb.record.metadata.Key.Expressions.concat(
                com.apple.foundationdb.record.metadata.Key.Expressions.field("first_name"),
                com.apple.foundationdb.record.metadata.Key.Expressions.field("last_name")));

        metadataBuilder.addIndex("Person", COUNT_FAMILY_MEMBER_INDEX);
        metadataBuilder.addIndex("Person", IDX_AGE_INDEX);
    }
}