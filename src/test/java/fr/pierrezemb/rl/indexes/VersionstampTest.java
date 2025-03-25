package fr.pierrezemb.rl.indexes;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.async.AsyncIterable;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.expressions.VersionKeyExpression;
import com.apple.foundationdb.record.provider.foundationdb.*;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import fr.pierrezemb.rl.protos.Person;
import fr.pierrezemb.rl.protos.PersonRecord;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

class VersionstampTest {

    private static final Index VERSION_INDEX = new Index(
            "version-index",
            VersionKeyExpression.VERSION,
            IndexTypes.VERSION);

    @Test
    void testVersionstamp() throws ExecutionException, InterruptedException, TimeoutException {
        // Define the keyspace for our application
        KeySpace keySpace = new KeySpace(new DirectoryLayerDirectory("tests-rl"));
        KeySpacePath path = keySpace.path("tests-rl", "version-test");

        RecordMetaDataBuilder metadataBuilder = RecordMetaData.newBuilder()
                .setRecords(PersonRecord.getDescriptor());
        setupPerson(metadataBuilder);

        RecordMetaData recordMetadata = metadataBuilder.build();

        // Initialize database
        FDBDatabase db = FDBDatabaseFactory.instance().getDatabase();
        db.performNoOpAsync().get(2, TimeUnit.SECONDS);

        Function<FDBRecordContext, FDBRecordStore> recordStoreProvider = context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(recordMetadata)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();

        // Create and save a person record
        Person person = Person.newBuilder()
                .setFirstName("John")
                .setLastName("Doe")
                .setAge(30)
                .build();

        // Save record in a transaction and get its version
        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);
            recordStore.saveRecord(person);
            return null;
        });

        db.run(context -> {
            FDBRecordStore recordStore = recordStoreProvider.apply(context);
            Tuple group = Tuple.from(person.getFirstName(), person.getLastName());

            Message record = recordStore.loadRecord(group).getRecord();
            Person myPerson = Person.newBuilder().mergeFrom(record).build();
            System.out.println(myPerson);

            FDBRecordVersion recordVersion = recordStore.loadRecordVersion(group).get();
            System.out.println(recordVersion.getDBVersion());

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

        metadataBuilder.setStoreRecordVersions(true);

        metadataBuilder.addIndex("Person", VERSION_INDEX);
    }
}
