package fr.pierrezemb.rl.internals;

import com.apple.foundationdb.KeyValue;
import com.apple.foundationdb.Range;
import com.apple.foundationdb.record.EvaluationContext;
import com.apple.foundationdb.record.IndexScanType;
import com.apple.foundationdb.record.RecordCursor;
import com.apple.foundationdb.record.RecordMetaData;
import com.apple.foundationdb.record.RecordMetaDataBuilder;
import com.apple.foundationdb.record.ScanProperties;
import com.apple.foundationdb.record.TupleRange;
import com.apple.foundationdb.record.metadata.Index;
import com.apple.foundationdb.record.metadata.IndexAggregateFunction;
import com.apple.foundationdb.record.metadata.IndexTypes;
import com.apple.foundationdb.record.metadata.Key;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabase;
import com.apple.foundationdb.record.provider.foundationdb.FDBDatabaseFactory;
import com.apple.foundationdb.record.provider.foundationdb.FDBQueriedRecord;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordContext;
import com.apple.foundationdb.record.provider.foundationdb.FDBRecordStore;
import com.apple.foundationdb.record.provider.foundationdb.IndexMaintainer;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.DirectoryLayerDirectory;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpace;
import com.apple.foundationdb.record.provider.foundationdb.keyspace.KeySpacePath;
import com.apple.foundationdb.record.query.RecordQuery;
import com.apple.foundationdb.record.query.expressions.Query;
import com.apple.foundationdb.subspace.Subspace;
import com.apple.foundationdb.tuple.Tuple;
import com.google.protobuf.Message;
import fr.pierrezemb.rl.protos.score.RankedScoreRecord;
import fr.pierrezemb.rl.protos.score.ScoreEntry;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;

import static com.apple.foundationdb.record.metadata.Key.Expressions.concat;
import static com.apple.foundationdb.record.metadata.Key.Expressions.field;

/**
 * ══════════════════════════════════════════════════════════════════════════════════════════════════
 *                         RANK INDEX DEEP-DIVE: AN EDUCATIONAL EXPLORATION
 * ══════════════════════════════════════════════════════════════════════════════════════════════════
 *
 * This test file serves as a comprehensive tutorial on how FDB Record Layer's RANK index works
 * internally. It is designed to be both readable documentation AND executable code.
 *
 * WHO IS THIS FOR?
 * ────────────────
 * 1. Engineers who want to understand how RANK indexes achieve O(log N) position-based access
 * 2. Engineers who want to reimplement a similar structure in another database system
 * 3. Anyone curious about probabilistic data structures in production systems
 *
 * WHAT IS A RANK INDEX?
 * ─────────────────────
 * A RANK index solves a fundamental problem: given a sorted set of values, how do you efficiently
 * answer questions about POSITION?
 *
 * Consider a leaderboard with 1 million scores. A regular B-tree index can quickly answer:
 *   - "Find all scores > 5000" (range scan)
 *   - "Does score 7500 exist?" (point lookup)
 *
 * But these questions are EXPENSIVE with a regular index:
 *   - "What is the rank of score 7500?" (must count all smaller values → O(N))
 *   - "What score is at rank 50,000?" (must skip 50,000 entries → O(N))
 *
 * The RANK index answers both in O(log N) time using a clever two-subspace architecture.
 *
 * THE TWO-SUBSPACE ARCHITECTURE
 * ─────────────────────────────
 *
 *   ┌─────────────────────────────────────────────────────────────────────────────┐
 *   │                           RANK INDEX STRUCTURE                              │
 *   ├─────────────────────────────────────────────────────────────────────────────┤
 *   │                                                                             │
 *   │  SUBSPACE 2 (Primary): Standard B-Tree Index                                │
 *   │  ════════════════════════════════════════                                   │
 *   │  • Stores [score, primary_key] → empty value                                │
 *   │  • Enables BY_VALUE scans (find scores in a range)                          │
 *   │  • Same as any VALUE index                                                  │
 *   │                                                                             │
 *   │  SUBSPACE 3 (Secondary): RankedSet Skip-List                                │
 *   │  ════════════════════════════════════════════                               │
 *   │  • Stores [level, score_tuple] → count (8-byte little-endian)               │
 *   │  • Enables BY_RANK scans (find score at position N)                         │
 *   │  • Enables RANK queries (find position of score X)                          │
 *   │  • Probabilistic skip-list with cumulative counts                           │
 *   │                                                                             │
 *   └─────────────────────────────────────────────────────────────────────────────┘
 *
 * THE SKIP-LIST: A PROBABILISTIC MARVEL
 * ──────────────────────────────────────
 *
 * The RankedSet is implemented as a persistent skip-list. Unlike a balanced tree that requires
 * complex rotations, a skip-list achieves O(log N) operations through PROBABILITY.
 *
 * Each key appears at level 0 (always), and at higher levels with decreasing probability:
 *   - Level 0: 100% of keys (always)
 *   - Level 1: ~6.25% of keys (1/16)
 *   - Level 2: ~0.4% of keys (1/256)
 *   - Level 3: ~0.02% of keys (1/4096)
 *   - ... and so on
 *
 * The level is determined by the HASH of the key:
 *   - hash & 0x0 == 0 → appears at level 0 (always true)
 *   - hash & 0xF == 0 → appears at level 1 (1/16 chance)
 *   - hash & 0xFF == 0 → appears at level 2 (1/256 chance)
 *
 * Visualization (simplified):
 *
 *   Level 3:  [sentinel]─────────────────────────────────[500]──────────────────────→
 *                  │                                        │
 *   Level 2:  [sentinel]────────────[200]─────────────[500]─────────[800]───────────→
 *                  │                    │                   │           │
 *   Level 1:  [sentinel]──[100]───[200]───[350]───[500]──[600]──[800]──[900]────────→
 *                  │        │       │       │       │      │      │      │
 *   Level 0:  [sentinel]─[100]─[150]─[200]─[350]─[400]─[500]─[600]─[750]─[800]─[900]→
 *                 ↓
 *              Count: 0   Count: 1  Count: 1 ... (each entry has a count)
 *
 * CUMULATIVE COUNTS: THE KEY INSIGHT
 * ───────────────────────────────────
 *
 * Each entry at each level stores the COUNT of elements between it and the previous entry
 * at the SAME level. This enables efficient rank computation:
 *
 *   To find rank of score X:
 *   1. Start at highest level
 *   2. Scan forward, summing counts, until next key > X
 *   3. Drop to lower level, continue summing
 *   4. Repeat until level 0 - final sum is the rank
 *
 *   To find score at rank N:
 *   1. Start at highest level with remaining_rank = N
 *   2. Scan forward, subtracting counts from remaining_rank
 *   3. When next count > remaining_rank, drop to lower level
 *   4. Repeat until level 0 - current key is the answer
 *
 * RUNNING THIS TEST
 * ─────────────────
 * Run with: ./gradlew test --tests "fr.pierrezemb.rl.internals.RankIndexDeepDiveTest"
 *
 * The output is designed to be read sequentially as a tutorial.
 *
 * @author Educational test for FDB Record Layer RANK indexes
 */
@TestMethodOrder(MethodOrderer.MethodName.class)
class RankIndexDeepDiveTest {

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                                    INDEX DEFINITIONS
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * UNGROUPED RANK INDEX: Global ranking across all records.
     *
     * The `.ungrouped()` means there's a single RankedSet for ALL scores.
     * Every score competes in one global leaderboard.
     *
     * Key expression: field("score").ungrouped()
     *   - ungrouped() = no grouping prefix, single global ranked set
     */
    private static final Index GLOBAL_RANK_INDEX = new Index(
            "idx-global-rank",
            field("score").ungrouped(),
            IndexTypes.RANK
    );

    /**
     * GROUPED RANK INDEX: Separate ranking per game.
     *
     * The `.groupBy(field("game"))` creates a SEPARATE RankedSet for each game.
     * Players are ranked only against others in the same game.
     *
     * Key expression: field("score").groupBy(field("game"))
     *   - Scores are partitioned by game
     *   - Each game has its own independent ranking
     */
    private static final Index GAME_RANK_INDEX = new Index(
            "idx-game-rank",
            field("score").groupBy(field("game")),
            IndexTypes.RANK
    );

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                                    TEST INFRASTRUCTURE
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    private FDBDatabase db;
    private KeySpace keySpace;
    private KeySpacePath basePath;

    @BeforeEach
    void setUp() throws ExecutionException, InterruptedException, TimeoutException {
        keySpace = new KeySpace(new DirectoryLayerDirectory("rank-deep-dive"));
        db = FDBDatabaseFactory.instance().getDatabase();
        db.performNoOpAsync().get(2, TimeUnit.SECONDS);

        // Clear everything for a clean slate
        db.run(ctx -> {
            ctx.ensureActive().clear(new Range(new byte[]{0x00}, new byte[]{(byte) 0xFE}));
            return null;
        });
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                                    TEST 01: INTRODUCTION
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 01: INTRODUCTION - What is a RANK Index and Why Does It Exist?
     *
     * This test demonstrates the fundamental problem that RANK indexes solve
     * and shows the basic structure of the data stored in FDB.
     */
    @Test
    void test_01_Introduction_WhatIsARankIndex() {
        printHeader("TEST 01: INTRODUCTION - What is a RANK Index?");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  THE PROBLEM: Position-Based Queries Are Expensive                              │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Imagine a leaderboard with 1 million players. With a standard B-tree index on score:

              ✓ "Find all scores between 5000 and 6000" → FAST (B-tree range scan)
              ✓ "Does score 7500 exist?" → FAST (B-tree point lookup)
              ✗ "What rank is score 7500?" → SLOW (must count ALL smaller scores!)
              ✗ "What score is at rank 50,000?" → SLOW (must skip 50,000 entries!)

            The RANK index solves this by maintaining a SECOND data structure alongside
            the B-tree: a probabilistic skip-list that tracks cumulative counts.

            Let's see it in action...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test01");
        RecordMetaData metadata = buildMetadata(GLOBAL_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Insert first score and examine the structure
        System.out.println("\n▶ STEP 1: Insert first score (Alice: 100 points)\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("alice")
                    .setPlayerName("Alice")
                    .setGame("tetris")
                    .setScore(100)
                    .build());
            return null;
        });

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            dumpAllKeyValues(ctx, store, "After inserting Alice (score=100)");
            return null;
        });

        System.out.println("""

            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  UNDERSTANDING THE OUTPUT                                                       │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Notice TWO distinct subspaces for our RANK index:

            SUBSPACE 2 (Primary B-Tree):
            • Key format: (prefix, 2, "idx-global-rank", <score>, <primary_key>)
            • Value: empty []
            • Purpose: Standard sorted index for BY_VALUE scans

            SUBSPACE 3 (RankedSet Skip-List):
            • Key format: (prefix, 3, "idx-global-rank", <level>, <tuple-encoded-score>)
            • Value: [count, 0, 0, 0, 0, 0, 0, 0] (8-byte little-endian long)
            • Purpose: Track counts for O(log N) rank operations

            The skip-list has multiple LEVELS (0-5 by default):
            • Level 0: Contains ALL scores (sentinel + each score)
            • Levels 1-5: Contain progressively fewer scores based on hash

            The empty byte array b"" is the SENTINEL - it marks the start of each level
            and initially has count=0 (will be updated as we add scores).
            """);

        // Insert second score
        System.out.println("\n▶ STEP 2: Insert second score (Bob: 250 points)\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("bob")
                    .setPlayerName("Bob")
                    .setGame("tetris")
                    .setScore(250)
                    .build());
            return null;
        });

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            dumpAllKeyValues(ctx, store, "After inserting Bob (score=250)");
            return null;
        });

        // Insert third score (in between)
        System.out.println("\n▶ STEP 3: Insert third score (Charlie: 175 points - between Alice and Bob)\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("charlie")
                    .setPlayerName("Charlie")
                    .setGame("tetris")
                    .setScore(175)
                    .build());
            return null;
        });

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            dumpAllKeyValues(ctx, store, "After inserting Charlie (score=175)");
            return null;
        });

        System.out.println("""

            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  KEY INSIGHT: Cumulative Counts                                                 │
            └─────────────────────────────────────────────────────────────────────────────────┘

            At Level 0, each score's count represents how many elements are between it
            and the PREVIOUS key at that level:

              Sentinel (b"")  → count=0 (nothing before it)
              Score 100       → count=1 (1 element: itself, since sentinel had 0)
              Score 175       → count=1 (1 element since score 100)
              Score 250       → count=1 (1 element since score 175)

            To find the RANK of score 175:
              Sum counts from sentinel to score 175 = 0 + 1 + 1 = 2
              But rank is 0-indexed, so rank = sum - 1 = 1

            Alice (100) is rank 0, Charlie (175) is rank 1, Bob (250) is rank 2.
            """);

        // Now demonstrate the queries
        System.out.println("\n▶ STEP 4: Query by RANK (position-based access)\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);

            // Query: Get players ranked 0-2 (all of them, in rank order)
            RecordQuery query = RecordQuery.newBuilder()
                    .setRecordType("ScoreEntry")
                    .setFilter(Query.rank("score").lessThanOrEquals(2L))
                    .build();

            System.out.println("Query: Players with rank <= 2 (i.e., top 3):");
            RecordCursor<FDBQueriedRecord<Message>> cursor = store.executeQuery(query);
            cursor.forEach(record -> {
                ScoreEntry entry = ScoreEntry.newBuilder().mergeFrom(record.getRecord()).build();
                System.out.println("  Rank " + getRank(store, entry.getScore()) +
                        ": " + entry.getPlayerName() + " (score=" + entry.getScore() + ")");
            }).join();

            return null;
        });

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                                TEST 02: ARCHITECTURE DEEP-DIVE
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 02: ARCHITECTURE - The Two-Subspace Design in Detail
     *
     * This test provides a detailed examination of how the primary (B-tree) and
     * secondary (RankedSet) subspaces work together.
     */
    @Test
    void test_02_Architecture_TwoSubspaceDesign() {
        printHeader("TEST 02: ARCHITECTURE - The Two-Subspace Design");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  THE TWO SUBSPACES: Complementary Data Structures                               │
            └─────────────────────────────────────────────────────────────────────────────────┘

            The RANK index maintains TWO separate data structures in FDB:

            ╔═══════════════════════════════════════════════════════════════════════════════╗
            ║  SUBSPACE 2: PRIMARY INDEX (B-Tree)                                           ║
            ╠═══════════════════════════════════════════════════════════════════════════════╣
            ║  Key:   [prefix, 2, index_name, score, primary_key...]                        ║
            ║  Value: [] (empty - the key itself contains all needed data)                  ║
            ║                                                                               ║
            ║  Purpose:                                                                     ║
            ║  • BY_VALUE scans: "Find all scores between X and Y"                          ║
            ║  • Existence checks: "Does this score exist?"                                 ║
            ║  • Ordered iteration by score value                                           ║
            ║                                                                               ║
            ║  This is identical to a standard VALUE index.                                 ║
            ╚═══════════════════════════════════════════════════════════════════════════════╝

            ╔═══════════════════════════════════════════════════════════════════════════════╗
            ║  SUBSPACE 3: SECONDARY INDEX (RankedSet / Skip-List)                          ║
            ╠═══════════════════════════════════════════════════════════════════════════════╣
            ║  Key:   [prefix, 3, index_name, level, tuple_encoded_score]                   ║
            ║  Value: [count as 8-byte little-endian long]                                  ║
            ║                                                                               ║
            ║  Purpose:                                                                     ║
            ║  • BY_RANK scans: "Find the score at position N"                              ║
            ║  • RANK_FOR_SCORE: "What is the rank of score X?"                             ║
            ║  • Efficient count operations                                                 ║
            ║                                                                               ║
            ║  This is the innovation that makes O(log N) rank operations possible.         ║
            ╚═══════════════════════════════════════════════════════════════════════════════╝

            Let's insert scores one by one and examine BOTH subspaces after each insert...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test02");
        RecordMetaData metadata = buildMetadata(GLOBAL_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Insert scores one at a time with detailed analysis
        int[] scores = {500, 200, 800, 350, 100};
        String[] players = {"Eve", "Dave", "Frank", "Grace", "Henry"};

        for (int i = 0; i < scores.length; i++) {
            final int score = scores[i];
            final String player = players[i];
            final int insertNum = i + 1;

            System.out.println("\n" + "═".repeat(80));
            System.out.println("  INSERT #" + insertNum + ": " + player + " with score " + score);
            System.out.println("═".repeat(80));

            db.run(ctx -> {
                FDBRecordStore store = storeProvider.apply(ctx);
                store.saveRecord(ScoreEntry.newBuilder()
                        .setPlayerId(player.toLowerCase())
                        .setPlayerName(player)
                        .setGame("arcade")
                        .setScore(score)
                        .build());
                return null;
            });

            db.run(ctx -> {
                FDBRecordStore store = storeProvider.apply(ctx);
                dumpSubspacesSeparately(ctx, store);
                return null;
            });

            // After each insert, show current rankings
            System.out.println("\n  Current Rankings:");
            db.run(ctx -> {
                FDBRecordStore store = storeProvider.apply(ctx);
                store.scanIndex(GLOBAL_RANK_INDEX, IndexScanType.BY_RANK,
                                TupleRange.ALL, null, ScanProperties.FORWARD_SCAN)
                        .forEach(entry -> {
                            long entryScore = entry.getKey().getLong(0);
                            System.out.println("    Rank " + getRank(store, entryScore) +
                                    ": score=" + entryScore);
                        }).join();
                return null;
            });
        }

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                            TEST 03: SKIP-LIST LEVEL STRUCTURE
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 03: SKIP-LIST INTERNALS - Understanding the Probabilistic Level Structure
     *
     * This test demonstrates how the skip-list assigns keys to levels based on their
     * hash values, creating a probabilistic balanced structure.
     */
    @Test
    void test_03_SkipList_LevelStructure() {
        printHeader("TEST 03: SKIP-LIST INTERNALS - The Level Structure");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  HOW THE SKIP-LIST ACHIEVES O(log N) WITHOUT REBALANCING                        │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Unlike balanced trees (AVL, Red-Black) that require complex rotations,
            the skip-list achieves balance through PROBABILITY.

            THE LEVEL DECISION ALGORITHM:
            ─────────────────────────────
            When inserting a score, the system computes hash(score_tuple) and checks:

              Level 0: Always include (every score appears here)
              Level 1: Include if (hash & 0x0000000F) == 0  →  ~1/16 probability
              Level 2: Include if (hash & 0x000000FF) == 0  →  ~1/256 probability
              Level 3: Include if (hash & 0x00000FFF) == 0  →  ~1/4096 probability
              Level 4: Include if (hash & 0x0000FFFF) == 0  →  ~1/65536 probability
              Level 5: Include if (hash & 0x000FFFFF) == 0  →  ~1/1048576 probability

            The masks are: LEVEL_FAN_VALUES = [0, 15, 255, 4095, 65535, 1048575, ...]
            Each level is 16x sparser than the previous.

            WHY THIS WORKS:
            ───────────────
            With ~1/16 probability per level, a set of N elements will have:
              - Level 0: N entries
              - Level 1: ~N/16 entries
              - Level 2: ~N/256 entries
              - Level 3: ~N/4096 entries
              - ...

            To traverse from start to a target, we scan ~16 entries per level on average.
            With 6 levels, that's ~96 entries for sets up to 16^6 ≈ 16 million elements!

            Let's insert 20 scores and visualize the resulting skip-list structure...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test03");
        RecordMetaData metadata = buildMetadata(GLOBAL_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Insert 20 scores with varying values
        System.out.println("\n▶ Inserting 20 scores...\n");

        int[] scores = {150, 300, 450, 600, 750, 900, 1050, 1200, 1350, 1500,
                100, 250, 400, 550, 700, 850, 1000, 1150, 1300, 1450};

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            for (int i = 0; i < scores.length; i++) {
                store.saveRecord(ScoreEntry.newBuilder()
                        .setPlayerId("player" + i)
                        .setPlayerName("Player " + i)
                        .setGame("game")
                        .setScore(scores[i])
                        .build());
            }
            return null;
        });

        // Now visualize the skip-list structure
        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            visualizeSkipList(ctx, store);
            return null;
        });

        System.out.println("""

            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  INTERPRETING THE VISUALIZATION                                                 │
            └─────────────────────────────────────────────────────────────────────────────────┘

            In the skip-list visualization above:
            • Level 0 contains ALL 20 scores (plus the sentinel)
            • Higher levels contain progressively fewer scores
            • The pattern is determined by hash(score_tuple) - it's deterministic but appears random

            The COUNTS at each entry show elements between it and the previous entry at that level:
            • At Level 0: Each score has count=1 (just itself since last entry)
            • At higher levels: Counts can be larger (accumulating elements from below)

            For rank lookup, we traverse from top level down, summing counts.
            For select (getNth), we traverse from top level down, subtracting counts.
            """);

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                          TEST 04: RANK_FOR_SCORE ALGORITHM
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 04: ALGORITHM - How RANK_FOR_SCORE Works
     *
     * This test walks through the algorithm for finding the rank of a given score,
     * showing the top-down traversal across skip-list levels.
     */
    @Test
    void test_04_Algorithm_RankForScore() {
        printHeader("TEST 04: ALGORITHM - How RANK_FOR_SCORE Works");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  THE RANK_FOR_SCORE ALGORITHM: Finding Position from Value                      │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Given a score X, find its 0-indexed position in the sorted set.

            ALGORITHM (pseudocode):
            ─────────────────────────
            function rank(score):
                rank = 0
                current_key = sentinel (empty)

                for level from (max_level - 1) down to 0:
                    while true:
                        next_entry = first entry at this level >= current_key
                        if next_entry.key > score:
                            break  // overshot, go to finer level
                        rank += next_entry.count
                        current_key = next_entry.key
                        if next_entry.key == score:
                            return rank - 1  // found it (0-indexed)

                return NOT_FOUND

            VISUAL WALKTHROUGH:
            ────────────────────
            Consider this skip-list and finding rank of score 350:

            Level 2:  [∅:0]──────────────────[500:5]────────────────────────→
                        │                       │
            Level 1:  [∅:0]─────[200:2]──────[500:3]──────[800:3]───────────→
                        │         │             │           │
            Level 0:  [∅:0]─[100:1]─[200:1]─[350:1]─[500:1]─[600:1]─[800:1]─→

            Finding rank of 350:
            1. Start at Level 2, position=[∅], rank=0
            2. Level 2: [∅:0] → rank=0, next is [500:5], but 500 > 350, drop down
            3. Level 1: [∅:0] → rank=0, next is [200:2], 200 < 350, advance
            4. Level 1: [200:2] → rank=2, next is [500:3], but 500 > 350, drop down
            5. Level 0: [200:1] already counted, next is [350:1], 350 == 350, advance
            6. Level 0: [350:1] → rank=3, FOUND!
            7. Return rank-1 = 2 (0-indexed: positions 0,1,2 = scores 100,200,350)

            Let's see this with real data...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test04");
        RecordMetaData metadata = buildMetadata(GLOBAL_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Insert scores in specific order to create predictable structure
        int[] scores = {100, 200, 350, 500, 600, 800, 950};

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            for (int i = 0; i < scores.length; i++) {
                store.saveRecord(ScoreEntry.newBuilder()
                        .setPlayerId("p" + i)
                        .setPlayerName("Player" + i)
                        .setGame("demo")
                        .setScore(scores[i])
                        .build());
            }
            return null;
        });

        System.out.println("\n▶ Current skip-list structure:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            visualizeSkipList(ctx, store);
            return null;
        });

        // Demonstrate rank lookups
        System.out.println("\n▶ Finding ranks for various scores:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);

            for (int score : new int[]{100, 350, 500, 800, 950}) {
                Long rank = getRank(store, score);
                System.out.println("  rank(" + score + ") = " + rank);
            }

            return null;
        });

        System.out.println("""

            The ranks are 0-indexed:
              score=100 → rank=0 (lowest score)
              score=200 → rank=1
              score=350 → rank=2
              score=500 → rank=3
              score=600 → rank=4
              score=800 → rank=5
              score=950 → rank=6 (highest score)
            """);

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                          TEST 05: SCORE_FOR_RANK ALGORITHM
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 05: ALGORITHM - How SCORE_FOR_RANK (getNth) Works
     *
     * This test walks through the algorithm for finding the score at a given rank,
     * showing the "subtract and narrow" traversal pattern.
     */
    @Test
    void test_05_Algorithm_ScoreForRank() {
        printHeader("TEST 05: ALGORITHM - How SCORE_FOR_RANK Works");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  THE SCORE_FOR_RANK ALGORITHM: Finding Value from Position                      │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Given a rank N (0-indexed), find the score at that position.

            ALGORITHM (pseudocode):
            ─────────────────────────
            function getNth(target_rank):
                remaining = target_rank + 1  // convert to 1-indexed count
                current_key = sentinel (empty)

                for level from (max_level - 1) down to 0:
                    while true:
                        next_entry = first entry at this level > current_key
                        if next_entry.count > remaining:
                            break  // would overshoot, need finer granularity
                        remaining -= next_entry.count
                        current_key = next_entry.key
                        if remaining == 0:
                            return current_key  // exact match

                return current_key if remaining == 0 else NOT_FOUND

            VISUAL WALKTHROUGH:
            ────────────────────
            Consider this skip-list and finding score at rank 3:

            Level 2:  [∅:0]──────────────────[500:5]────────────────────────→
                        │                       │
            Level 1:  [∅:0]─────[200:2]──────[500:3]──────[800:3]───────────→
                        │         │             │           │
            Level 0:  [∅:0]─[100:1]─[200:1]─[350:1]─[500:1]─[600:1]─[800:1]─→

            Finding score at rank 3 (0-indexed), so we need the 4th element:
            1. Start at Level 2, remaining=4, current=[∅]
            2. Level 2: next is [500:5], count(5) > remaining(4), drop down
            3. Level 1: next is [200:2], count(2) <= remaining(4)
               → remaining = 4-2 = 2, current = [200]
            4. Level 1: next is [500:3], count(3) > remaining(2), drop down
            5. Level 0: next is [350:1], count(1) <= remaining(2)
               → remaining = 2-1 = 1, current = [350]
            6. Level 0: next is [500:1], count(1) == remaining(1)
               → remaining = 1-1 = 0, current = [500], DONE!
            7. Return 500

            Let's see this with real data...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test05");
        RecordMetaData metadata = buildMetadata(GLOBAL_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Same scores as test 04 for consistency
        int[] scores = {100, 200, 350, 500, 600, 800, 950};

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            for (int i = 0; i < scores.length; i++) {
                store.saveRecord(ScoreEntry.newBuilder()
                        .setPlayerId("p" + i)
                        .setPlayerName("Player" + i)
                        .setGame("demo")
                        .setScore(scores[i])
                        .build());
            }
            return null;
        });

        System.out.println("\n▶ Current skip-list structure:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            visualizeSkipList(ctx, store);
            return null;
        });

        // Demonstrate getNth lookups via BY_RANK scans
        System.out.println("\n▶ Finding scores at various ranks:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);

            for (int rank = 0; rank < scores.length; rank++) {
                final int r = rank;
                // Use BY_RANK scan starting at the specific rank
                store.scanIndex(GLOBAL_RANK_INDEX, IndexScanType.BY_RANK,
                                TupleRange.allOf(Tuple.from(r)), null, ScanProperties.FORWARD_SCAN)
                        .first()
                        .thenAccept(entry -> {
                            if (entry.isPresent()) {
                                System.out.println("  score_at_rank(" + r + ") = " +
                                        entry.get().getKey().getLong(0));
                            }
                        }).join();
            }

            return null;
        });

        System.out.println("""

            The results match our sorted scores array:
              rank 0 → score 100
              rank 1 → score 200
              rank 2 → score 350
              rank 3 → score 500
              rank 4 → score 600
              rank 5 → score 800
              rank 6 → score 950
            """);

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                          TEST 06: GROUPED RANKINGS
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 06: GROUPED RANKING - Partitioned Leaderboards
     *
     * This test demonstrates how grouped RANK indexes maintain separate rankings
     * for different groups (e.g., per-game leaderboards).
     */
    @Test
    void test_06_GroupedRanking_PartitionedLeaderboards() {
        printHeader("TEST 06: GROUPED RANKING - Partitioned Leaderboards");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  GROUPED RANK INDEXES: Separate Leaderboards per Category                       │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Real applications often need separate rankings per category:
              • Game scores: rank within each game, not globally
              • Regional leaderboards: rank within each region
              • Time-based rankings: rank within each time period

            With a grouped RANK index, each group gets its OWN RankedSet!

            Index definition:
              field("score").groupBy(field("game"))

            This creates:
              • Primary subspace: [prefix, 2, "idx", game, score, pk] → []
              • Secondary subspace: [prefix, 3, "idx", game, level, score_tuple] → count

            The 'game' field becomes a PREFIX in both subspaces, effectively creating
            independent data structures for each game.

            Let's see this with two games: "tetris" and "pacman"...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test06");
        RecordMetaData metadata = buildMetadata(GAME_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Insert scores for two different games
        System.out.println("\n▶ Inserting scores for TETRIS:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("alice").setPlayerName("Alice")
                    .setGame("tetris").setScore(5000).build());
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("bob").setPlayerName("Bob")
                    .setGame("tetris").setScore(7500).build());
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("charlie").setPlayerName("Charlie")
                    .setGame("tetris").setScore(3000).build());
            return null;
        });

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            dumpAllKeyValues(ctx, store, "After inserting Tetris scores");
            return null;
        });

        System.out.println("\n▶ Inserting scores for PACMAN:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("dave").setPlayerName("Dave")
                    .setGame("pacman").setScore(12000).build());
            store.saveRecord(ScoreEntry.newBuilder()
                    .setPlayerId("eve").setPlayerName("Eve")
                    .setGame("pacman").setScore(8000).build());
            return null;
        });

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            dumpAllKeyValues(ctx, store, "After inserting Pacman scores");
            return null;
        });

        System.out.println("""

            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  NOTICE THE STRUCTURE                                                           │
            └─────────────────────────────────────────────────────────────────────────────────┘

            In the Secondary Subspace (3), you can see TWO separate RankedSets:

            For "pacman":
              (prefix, 3, "idx-game-rank", "pacman", 0, b"") → [0,...]  // sentinel
              (prefix, 3, "idx-game-rank", "pacman", 0, ...) → [1,...]  // scores

            For "tetris":
              (prefix, 3, "idx-game-rank", "tetris", 0, b"") → [0,...]  // sentinel
              (prefix, 3, "idx-game-rank", "tetris", 0, ...) → [1,...]  // scores

            Each game has completely independent level structures and counts!
            """);

        // Query rankings within each game
        System.out.println("\n▶ Rankings within each game:\n");

        for (String game : new String[]{"tetris", "pacman"}) {
            System.out.println("  " + game.toUpperCase() + " Leaderboard:");

            db.run(ctx -> {
                FDBRecordStore store = storeProvider.apply(ctx);

                // Scan BY_RANK within the specific game group
                store.scanIndex(GAME_RANK_INDEX, IndexScanType.BY_RANK,
                                TupleRange.allOf(Tuple.from(game)), null, ScanProperties.FORWARD_SCAN)
                        .forEach(entry -> {
                            // Entry key is (game, score, pk)
                            long score = entry.getKey().getLong(1);
                            String pk = entry.getKey().getString(2);
                            // Get rank within this game
                            Long rank = getGroupedRank(store, game, score);
                            System.out.println("    Rank " + rank + ": " + pk + " (score=" + score + ")");
                        }).join();

                return null;
            });
            System.out.println();
        }

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                          TEST 07: TUPLE ENCODING
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * TEST 07: TUPLE ENCODING - Byte-Level Details
     *
     * This test provides a deep dive into how FDB encodes values as tuples,
     * showing the actual byte representations and explaining the encoding scheme.
     */
    @Test
    void test_07_TupleEncoding_ByteLevelDetails() {
        printHeader("TEST 07: TUPLE ENCODING - Byte-Level Details");

        System.out.println("""
            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  FDB TUPLE ENCODING: Preserving Sort Order in Bytes                             │
            └─────────────────────────────────────────────────────────────────────────────────┘

            FoundationDB's tuple layer encodes values into bytes that PRESERVE SORT ORDER.
            This is crucial because FDB's B-tree sorts by raw byte comparison.

            ENCODING RULES:
            ────────────────
            • null:           0x00
            • bytes:          0x01 + escaped_bytes + 0x00
            • string:         0x02 + UTF-8 bytes + 0x00
            • int (nested):   Various prefixes based on size
            • tuple:          0x05 + encoded_elements + 0x00

            INTEGER ENCODING (the interesting part):
            ─────────────────────────────────────────
            Integers use size-prefixed encoding to maintain sort order:

              0:              0x14
              1-255:          0x15 + 1 byte
              256-65535:      0x16 + 2 bytes (big-endian)
              65536-16M:      0x17 + 3 bytes (big-endian)
              ...and so on

            Negative integers use complementary encoding:
              -1:             0x13 + 0xFE
              -256:           0x12 + 0xFE + 0xFF
              ...and so on

            This ensures: encode(-100) < encode(-1) < encode(0) < encode(1) < encode(100)

            Let's examine real tuple encodings from our skip-list...
            """);

        KeySpacePath path = keySpace.path("rank-deep-dive", "test07");
        RecordMetaData metadata = buildMetadata(GLOBAL_RANK_INDEX);
        Function<FDBRecordContext, FDBRecordStore> storeProvider = createStoreProvider(path, metadata);

        // Insert scores with varied values to show encoding
        int[] scores = {0, 1, 100, 255, 256, 1000, 10000, 65535, 65536};

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            for (int i = 0; i < scores.length; i++) {
                store.saveRecord(ScoreEntry.newBuilder()
                        .setPlayerId("p" + i)
                        .setPlayerName("Player" + i)
                        .setGame("encoding-demo")
                        .setScore(scores[i])
                        .build());
            }
            return null;
        });

        System.out.println("\n▶ Tuple encoding examples:\n");

        for (int score : scores) {
            Tuple t = Tuple.from(score);
            byte[] packed = t.pack();
            System.out.println("  Score " + String.format("%6d", score) + " → " +
                    bytesToHex(packed) + " → " + explainTupleBytes(packed));
        }

        System.out.println("""

            ┌─────────────────────────────────────────────────────────────────────────────────┐
            │  ENCODING BREAKDOWN                                                             │
            └─────────────────────────────────────────────────────────────────────────────────┘

            Prefix bytes for positive integers:
              0x14 = zero
              0x15 = 1-byte integer (1-255)
              0x16 = 2-byte integer (256-65535)
              0x17 = 3-byte integer (65536-16777215)
              0x18 = 4-byte integer
              ...

            Examples:
              score=1     → 0x15 0x01           (prefix + value)
              score=100   → 0x15 0x64           (prefix + 0x64 = 100)
              score=255   → 0x15 0xFF           (prefix + 0xFF = 255)
              score=256   → 0x16 0x01 0x00      (2-byte prefix + big-endian 256)
              score=1000  → 0x16 0x03 0xE8      (2-byte prefix + big-endian 1000)
              score=65536 → 0x17 0x01 0x00 0x00 (3-byte prefix + big-endian 65536)

            The beauty: lexicographic byte comparison equals numeric comparison!
            """);

        System.out.println("\n▶ Raw key-value dump showing encoded scores in skip-list:\n");

        db.run(ctx -> {
            FDBRecordStore store = storeProvider.apply(ctx);
            dumpRankedSetWithEncodingDetails(ctx, store);
            return null;
        });

        printFooter();
    }

    // ═══════════════════════════════════════════════════════════════════════════════════════════
    //                                    HELPER METHODS
    // ═══════════════════════════════════════════════════════════════════════════════════════════

    /**
     * Builds RecordMetaData with the specified index.
     */
    private RecordMetaData buildMetadata(Index index) {
        RecordMetaDataBuilder builder = RecordMetaData.newBuilder()
                .setRecords(RankedScoreRecord.getDescriptor());

        builder.getRecordType("ScoreEntry").setPrimaryKey(concat(
                field("game"),
                field("player_id")
        ));

        builder.addIndex("ScoreEntry", index);

        return builder.build();
    }

    /**
     * Creates a store provider function for the given path and metadata.
     */
    private Function<FDBRecordContext, FDBRecordStore> createStoreProvider(
            KeySpacePath path, RecordMetaData metadata) {
        return context -> FDBRecordStore.newBuilder()
                .setMetaDataProvider(metadata)
                .setContext(context)
                .setKeySpacePath(path)
                .createOrOpen();
    }

    /**
     * Gets the rank of a score using the global rank index.
     */
    private Long getRank(FDBRecordStore store, long score) {
        return store.evaluateAggregateFunction(
                List.of("ScoreEntry"),
                new IndexAggregateFunction(
                        com.apple.foundationdb.record.FunctionNames.RANK_FOR_SCORE,
                        GLOBAL_RANK_INDEX.getRootExpression(),
                        GLOBAL_RANK_INDEX.getName()
                ),
                TupleRange.allOf(Tuple.from(score)),
                com.apple.foundationdb.record.IsolationLevel.SERIALIZABLE
        ).thenApply(t -> t != null ? t.getLong(0) : null).join();
    }

    /**
     * Gets the rank of a score within a specific game group.
     */
    private Long getGroupedRank(FDBRecordStore store, String game, long score) {
        return store.evaluateAggregateFunction(
                List.of("ScoreEntry"),
                new IndexAggregateFunction(
                        com.apple.foundationdb.record.FunctionNames.RANK_FOR_SCORE,
                        GAME_RANK_INDEX.getRootExpression(),
                        GAME_RANK_INDEX.getName()
                ),
                TupleRange.allOf(Tuple.from(game, score)),
                com.apple.foundationdb.record.IsolationLevel.SERIALIZABLE
        ).thenApply(t -> t != null ? t.getLong(0) : null).join();
    }

    /**
     * Dumps all key-value pairs in the record store's subspace with annotations.
     */
    private void dumpAllKeyValues(FDBRecordContext ctx, FDBRecordStore store, String label) {
        System.out.println("┌" + "─".repeat(78) + "┐");
        System.out.println("│ " + label + " ".repeat(Math.max(0, 77 - label.length())) + "│");
        System.out.println("├" + "─".repeat(78) + "┤");

        Subspace subspace = store.getSubspace();
        ctx.ensureActive().getRange(subspace.range()).forEach(kv -> {
            Tuple key = subspace.unpack(kv.getKey());
            String keyStr = formatKey(key);
            String valStr = formatValue(kv.getValue(), key);
            System.out.println("│ " + padRight(keyStr + " → " + valStr, 77) + "│");
        });

        System.out.println("└" + "─".repeat(78) + "┘");
    }

    /**
     * Dumps the primary and secondary subspaces separately with clear labels.
     */
    private void dumpSubspacesSeparately(FDBRecordContext ctx, FDBRecordStore store) {
        Subspace subspace = store.getSubspace();

        // Collect entries by subspace
        List<String> primaryEntries = new ArrayList<>();
        List<String> secondaryEntries = new ArrayList<>();
        List<String> recordEntries = new ArrayList<>();

        ctx.ensureActive().getRange(subspace.range()).forEach(kv -> {
            Tuple key = subspace.unpack(kv.getKey());
            String keyStr = formatKey(key);
            String valStr = formatValue(kv.getValue(), key);
            String entry = keyStr + " → " + valStr;

            if (key.size() > 0) {
                long prefix = key.getLong(0);
                if (prefix == 1) {
                    recordEntries.add(entry);
                } else if (prefix == 2) {
                    primaryEntries.add(entry);
                } else if (prefix == 3) {
                    secondaryEntries.add(entry);
                }
            }
        });

        // Print primary subspace
        System.out.println("\n  PRIMARY SUBSPACE (2) - B-Tree Index:");
        System.out.println("  " + "─".repeat(60));
        for (String entry : primaryEntries) {
            System.out.println("  " + entry);
        }

        // Print secondary subspace
        System.out.println("\n  SECONDARY SUBSPACE (3) - RankedSet Skip-List:");
        System.out.println("  " + "─".repeat(60));
        for (String entry : secondaryEntries) {
            System.out.println("  " + entry);
        }
    }

    /**
     * Visualizes the skip-list structure as ASCII art.
     */
    private void visualizeSkipList(FDBRecordContext ctx, FDBRecordStore store) {
        Subspace subspace = store.getSubspace();

        // Collect skip-list entries by level
        Map<Integer, List<SkipListEntry>> levelEntries = new TreeMap<>();

        ctx.ensureActive().getRange(subspace.range()).forEach(kv -> {
            Tuple key = subspace.unpack(kv.getKey());
            if (key.size() > 0 && key.getLong(0) == 3) { // Secondary subspace
                // Key format: (3, index_name, level, score_bytes)
                if (key.size() >= 4) {
                    int level = (int) key.getLong(2);
                    byte[] scoreBytes = key.getBytes(3);
                    long count = decodeLittleEndianLong(kv.getValue());

                    String scoreStr = scoreBytes.length == 0 ? "∅" : decodeTupleScore(scoreBytes);

                    levelEntries.computeIfAbsent(level, k -> new ArrayList<>())
                            .add(new SkipListEntry(scoreStr, count));
                }
            }
        });

        // Print visualization
        System.out.println("  SKIP-LIST VISUALIZATION:");
        System.out.println("  " + "═".repeat(70));

        int maxLevel = levelEntries.keySet().stream().mapToInt(i -> i).max().orElse(0);

        for (int level = maxLevel; level >= 0; level--) {
            List<SkipListEntry> entries = levelEntries.getOrDefault(level, new ArrayList<>());
            StringBuilder sb = new StringBuilder();
            sb.append(String.format("  Level %d: ", level));

            for (SkipListEntry entry : entries) {
                sb.append(String.format("[%s:%d]─", entry.score, entry.count));
            }
            sb.append("→");

            System.out.println(sb);
        }

        System.out.println("  " + "═".repeat(70));

        // Print level statistics
        System.out.println("\n  Level Statistics:");
        for (int level = 0; level <= maxLevel; level++) {
            int count = levelEntries.getOrDefault(level, new ArrayList<>()).size();
            System.out.println("    Level " + level + ": " + count + " entries" +
                    (level == 0 ? " (all scores + sentinel)" : ""));
        }
    }

    /**
     * Dumps RankedSet entries with detailed encoding explanations.
     */
    private void dumpRankedSetWithEncodingDetails(FDBRecordContext ctx, FDBRecordStore store) {
        Subspace subspace = store.getSubspace();

        System.out.println("  Level 0 entries (showing tuple encoding):");
        System.out.println("  " + "─".repeat(70));

        ctx.ensureActive().getRange(subspace.range()).forEach(kv -> {
            Tuple key = subspace.unpack(kv.getKey());
            if (key.size() >= 4 && key.getLong(0) == 3 && key.getLong(2) == 0) {
                byte[] scoreBytes = key.getBytes(3);
                long count = decodeLittleEndianLong(kv.getValue());

                if (scoreBytes.length == 0) {
                    System.out.println("  Sentinel: b\"\" (empty) → count=" + count);
                } else {
                    String hex = bytesToHex(scoreBytes);
                    String decoded = decodeTupleScore(scoreBytes);
                    String explanation = explainTupleBytes(scoreBytes);
                    System.out.println("  Score " + decoded + ": " + hex +
                            " (" + explanation + ") → count=" + count);
                }
            }
        });
    }

    /**
     * Formats a key tuple for display.
     */
    private String formatKey(Tuple key) {
        StringBuilder sb = new StringBuilder("(");
        for (int i = 0; i < key.size(); i++) {
            if (i > 0) sb.append(", ");
            Object item = key.get(i);
            if (item instanceof byte[]) {
                byte[] bytes = (byte[]) item;
                if (bytes.length == 0) {
                    sb.append("b\"\"");
                } else if (bytes.length <= 4) {
                    sb.append("b\"").append(bytesToHex(bytes)).append("\"");
                } else {
                    sb.append("b\"").append(bytesToHex(bytes)).append("\"");
                }
            } else {
                sb.append(item);
            }
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * Formats a value for display based on the key type.
     */
    private String formatValue(byte[] value, Tuple key) {
        if (value == null || value.length == 0) {
            return "[]";
        }

        // Check if this is a count value (8-byte little-endian long)
        if (value.length == 8) {
            long count = decodeLittleEndianLong(value);
            // If it's a small number, show as count; otherwise show bytes
            if (count >= 0 && count < 1000000) {
                return "[count=" + count + "]";
            }
        }

        // Show raw bytes for other values
        return Arrays.toString(value);
    }

    /**
     * Decodes a little-endian 8-byte long.
     */
    private long decodeLittleEndianLong(byte[] bytes) {
        if (bytes == null || bytes.length != 8) return 0;
        return ByteBuffer.wrap(bytes).order(ByteOrder.LITTLE_ENDIAN).getLong();
    }

    /**
     * Decodes a tuple-encoded score to a string representation.
     */
    private String decodeTupleScore(byte[] bytes) {
        try {
            Tuple t = Tuple.fromBytes(bytes);
            if (t.size() > 0) {
                return String.valueOf(t.get(0));
            }
        } catch (Exception e) {
            // Fall through to hex representation
        }
        return "0x" + bytesToHex(bytes);
    }

    /**
     * Converts bytes to hex string.
     */
    private String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X", b & 0xFF));
        }
        return sb.toString();
    }

    /**
     * Explains what a tuple byte sequence means.
     */
    private String explainTupleBytes(byte[] bytes) {
        if (bytes.length == 0) return "empty tuple";
        if (bytes.length == 1 && bytes[0] == 0x14) return "zero";

        int prefix = bytes[0] & 0xFF;

        if (prefix == 0x14) {
            return "int(0)";
        } else if (prefix == 0x15) {
            return "1-byte int prefix";
        } else if (prefix == 0x16) {
            return "2-byte int prefix";
        } else if (prefix == 0x17) {
            return "3-byte int prefix";
        } else if (prefix == 0x18) {
            return "4-byte int prefix";
        }

        return "prefix=0x" + String.format("%02X", prefix);
    }

    /**
     * Pads a string to the right with spaces.
     */
    private String padRight(String s, int width) {
        if (s.length() >= width) return s.substring(0, width);
        return s + " ".repeat(width - s.length());
    }

    /**
     * Prints a section header.
     */
    private void printHeader(String title) {
        System.out.println("\n");
        System.out.println("═".repeat(80));
        System.out.println("  " + title);
        System.out.println("═".repeat(80));
        System.out.println();
    }

    /**
     * Prints a section footer.
     */
    private void printFooter() {
        System.out.println("\n" + "═".repeat(80) + "\n");
    }

    /**
     * Helper class for skip-list entries.
     */
    private static class SkipListEntry {
        final String score;
        final long count;

        SkipListEntry(String score, long count) {
            this.score = score;
            this.count = count;
        }
    }
}
