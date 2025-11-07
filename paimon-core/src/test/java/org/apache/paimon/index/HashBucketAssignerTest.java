/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.paimon.index;

import org.apache.paimon.catalog.PrimaryKeyTableTestBase;
import org.apache.paimon.data.BinaryRow;
import org.apache.paimon.io.CompactIncrement;
import org.apache.paimon.io.DataFileMeta;
import org.apache.paimon.io.DataIncrement;
import org.apache.paimon.stats.SimpleStats;
import org.apache.paimon.table.sink.CommitMessage;
import org.apache.paimon.table.sink.CommitMessageImpl;
import org.apache.paimon.table.sink.StreamTableCommit;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.UUID;

import static org.apache.paimon.io.DataFileTestUtils.row;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/** Test for {@link HashBucketAssigner}. */
public class HashBucketAssignerTest extends PrimaryKeyTableTestBase {

    private IndexFileHandler fileHandler;
    private StreamTableCommit commit;

    @BeforeEach
    public void beforeEach() throws Exception {
        fileHandler = table.store().newIndexFileHandler();
        commit = table.newStreamWriteBuilder().withCommitUser(commitUser).newCommit();
    }

    @AfterEach
    public void afterEach() throws Exception {
        commit.close();
    }

    private HashBucketAssigner createAssigner(int numChannels, int numAssigners, int assignId) {
        return new HashBucketAssigner(
                table.snapshotManager(),
                commitUser,
                fileHandler,
                numChannels,
                numAssigners,
                assignId,
                5,
                -1);
    }

    private HashBucketAssigner createAssigner(
            int numChannels, int numAssigners, int assignId, int maxBucketsNum) {
        return new HashBucketAssigner(
                table.snapshotManager(),
                commitUser,
                fileHandler,
                numChannels,
                numAssigners,
                assignId,
                5,
                maxBucketsNum);
    }

    private HashBucketAssigner createAssigner(
            int numChannels,
            int numAssigners,
            int assignId,
            long targetBucketRowNumber,
            long targetBucketSize,
            int maxBucketsNum) {
        return new HashBucketAssigner(
                table.snapshotManager(),
                commitUser,
                fileHandler,
                numChannels,
                numAssigners,
                assignId,
                targetBucketRowNumber,
                targetBucketSize,
                maxBucketsNum);
    }

    @Test
    public void testAssign() {
        HashBucketAssigner assigner = createAssigner(2, 2, 0);

        // assign
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 4)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 8)).isEqualTo(0);

        // full
        assertThat(assigner.assign(row(1), 10)).isEqualTo(2);

        // another partition
        assertThat(assigner.assign(row(2), 12)).isEqualTo(0);

        // read assigned
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);

        // not mine
        assertThatThrownBy(() -> assigner.assign(row(1), 1))
                .hasMessageContaining("This is a bug, record assign id");
    }

    @Test
    public void testAssignWithUpperBound() {
        HashBucketAssigner assigner = createAssigner(2, 2, 0, 2);

        // assign
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 4)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 8)).isEqualTo(0);

        // full
        assertThat(assigner.assign(row(1), 10)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 12)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 14)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 16)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 18)).isEqualTo(0);

        // another partition
        assertThat(assigner.assign(row(2), 12)).isEqualTo(0);

        // read assigned
        assertThat(assigner.assign(row(1), 6)).isEqualTo(0);

        // not mine
        assertThatThrownBy(() -> assigner.assign(row(1), 1))
                .hasMessageContaining("This is a bug, record assign id");

        // exceed buckets upper bound
        // partition 1
        int hash = 18;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner.assign(row(1), hash += 2);
            assertThat(bucket).isIn(0, 2);
        }
        // partition 2
        hash = 12;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner.assign(row(2), hash += 2);
            assertThat(bucket).isIn(0, 2);
        }
    }

    @Test
    public void testAssignWithUpperBoundMultiAssigners() {
        HashBucketAssigner assigner0 = createAssigner(2, 2, 0, 3);
        HashBucketAssigner assigner1 = createAssigner(2, 2, 1, 3);

        // assigner0: assign
        assertThat(assigner0.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 4)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 6)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(0);

        // assigner0: full
        assertThat(assigner0.assign(row(1), 10)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 12)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 14)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 16)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 18)).isEqualTo(2);

        // assigner0: exceed buckets upper bound
        int hash = 18;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner0.assign(row(2), hash += 2);
            assertThat(bucket).isIn(0, 2);
        }

        // assigner1: assign
        assertThat(assigner1.assign(row(1), 1)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 3)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 5)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 7)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 9)).isEqualTo(1);

        // assigner1: exceed buckets upper bound
        hash = 9;
        for (int i = 0; i < 200; i++) {
            int bucket = assigner1.assign(row(2), hash += 2);
            assertThat(bucket).isIn(1);
        }
    }

    @ParameterizedTest(name = "maxBuckets: {0}")
    @ValueSource(ints = {-1, 1, 2})
    public void testPartitionCopy(int maxBucketsNum) {
        HashBucketAssigner assigner = createAssigner(1, 1, 0, maxBucketsNum);

        BinaryRow partition = row(1);
        assertThat(assigner.assign(partition, 0)).isEqualTo(0);
        assertThat(assigner.assign(partition, 1)).isEqualTo(0);

        partition.setInt(0, 2);
        assertThat(assigner.assign(partition, 5)).isEqualTo(0);
        assertThat(assigner.assign(partition, 6)).isEqualTo(0);

        assertThat(assigner.currentPartitions()).contains(row(1));
        assertThat(assigner.currentPartitions()).contains(row(2));
    }

    private CommitMessage createCommitMessage(
            BinaryRow partition, int bucket, int totalBuckets, IndexFileMeta file) {
        return new CommitMessageImpl(
                partition,
                bucket,
                totalBuckets,
                new DataIncrement(
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(file),
                        Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    @Test
    public void testAssignRestore() throws IOException {
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5})),
                        createCommitMessage(
                                row(1),
                                2,
                                3,
                                fileHandler.hashIndex(row(1), 2).write(new int[] {4, 7}))));

        HashBucketAssigner assigner0 = createAssigner(3, 3, 0);
        HashBucketAssigner assigner2 = createAssigner(3, 3, 2);

        // read assigned
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 4)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 5)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 7)).isEqualTo(2);

        // new assign
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 11)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 14)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 17)).isEqualTo(3);
    }

    @Test
    public void testAssignRestoreWithUpperBound() throws IOException {
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5})),
                        createCommitMessage(
                                row(1),
                                2,
                                3,
                                fileHandler.hashIndex(row(1), 2).write(new int[] {4, 7}))));

        HashBucketAssigner assigner0 = createAssigner(3, 3, 0, 1);
        HashBucketAssigner assigner2 = createAssigner(3, 3, 2, 1);

        // read assigned
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 4)).isEqualTo(2);
        assertThat(assigner0.assign(row(1), 5)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 7)).isEqualTo(2);

        // new assign
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 11)).isEqualTo(0);
        assertThat(assigner0.assign(row(1), 14)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 16)).isEqualTo(2);
        // exceed buckets upper bound
        assertThat(assigner0.assign(row(1), 17)).isEqualTo(0);
    }

    @Test
    public void testAssignDecoupled() {
        HashBucketAssigner assigner1 = createAssigner(3, 2, 1);
        assertThat(assigner1.assign(row(1), 0)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 2)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 4)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 6)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 8)).isEqualTo(1);
        assertThat(assigner1.assign(row(1), 10)).isEqualTo(3);

        HashBucketAssigner assigner2 = createAssigner(3, 2, 2);
        assertThat(assigner2.assign(row(1), 1)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 3)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 5)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 7)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 9)).isEqualTo(0);
        assertThat(assigner2.assign(row(1), 11)).isEqualTo(2);

        HashBucketAssigner assigner0 = createAssigner(3, 2, 0);
        assertThat(assigner0.assign(row(2), 1)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 3)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 5)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 7)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 9)).isEqualTo(0);
        assertThat(assigner0.assign(row(2), 11)).isEqualTo(2);
    }

    @Test
    public void testIndexEliminate() throws IOException {
        HashBucketAssigner assigner = createAssigner(1, 1, 0);

        // checkpoint 0
        assertThat(assigner.assign(row(1), 0)).isEqualTo(0);
        assertThat(assigner.assign(row(2), 0)).isEqualTo(0);
        assigner.prepareCommit(0);
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessage(
                                row(1),
                                0,
                                1,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {0})),
                        createCommitMessage(
                                row(2),
                                0,
                                1,
                                fileHandler.hashIndex(row(2), 0).write(new int[] {0}))));

        assertThat(assigner.currentPartitions()).containsExactlyInAnyOrder(row(1), row(2));

        // checkpoint 1, but no commit
        assertThat(assigner.assign(row(1), 1)).isEqualTo(0);
        assigner.prepareCommit(1);
        assertThat(assigner.currentPartitions()).containsExactlyInAnyOrder(row(1));

        // checkpoint 2
        assigner.prepareCommit(2);
        assertThat(assigner.currentPartitions()).containsExactlyInAnyOrder(row(1));

        // checkpoint 3 and commit checkpoint 1
        commit.commit(
                1,
                Collections.singletonList(
                        createCommitMessage(
                                row(1),
                                0,
                                1,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {1}))));

        assigner.prepareCommit(3);
        assertThat(assigner.currentPartitions()).isEmpty();
    }

    @Test
    public void testAssignWithTargetBucketSize() throws IOException {
        // Commit initial data: bucket 0 with 100 bytes
        commit.commit(
                0,
                Collections.singletonList(
                        createCommitMessageWithDataFiles(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5}),
                                createDataFileMetaOfSize(100L))));

        // Create assigner with targetBucketSize=150 bytes
        HashBucketAssigner assigner = createAssigner(3, 3, 0, 5, 150L, -1);

        // Read assigned - should go to bucket 0 (size=100 < 150)
        assertThat(assigner.assign(row(1), 2)).isEqualTo(0);
        assertThat(assigner.assign(row(1), 5)).isEqualTo(0);

        commit.commit(
                1,
                Collections.singletonList(
                        createCommitMessageWithDataFiles(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5, 8}),
                                createDataFileMetaOfSize(80L))));
        HashBucketAssigner assigner2 = createAssigner(3, 3, 0, 5, 150L, -1);
        assertThat(assigner2.assign(row(1), 11)).isEqualTo(3);
    }

    @Test
    public void testBucketSizeWithMultipleBuckets() throws IOException {
        // Commit data to bucket 0 (100 bytes) and bucket 2 (80 bytes)
        commit.commit(
                0,
                Arrays.asList(
                        createCommitMessageWithDataFiles(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5}),
                                createDataFileMetaOfSize(100L)),
                        createCommitMessageWithDataFiles(
                                row(1),
                                2,
                                3,
                                fileHandler.hashIndex(row(1), 2).write(new int[] {4, 7}),
                                createDataFileMetaOfSize(80L))));

        // Create assigner with targetBucketSize=90 bytes
        HashBucketAssigner assigner0 = createAssigner(3, 3, 0, 5, 90L, -1);
        HashBucketAssigner assigner2 = createAssigner(3, 3, 2, 5, 90L, -1);

        // Bucket 0 (100 bytes) exceeds limit, new hash should go to new bucket
        assertThat(assigner0.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner0.assign(row(1), 8)).isEqualTo(3); // new hash, new bucket

        // Bucket 2 (80 bytes) is under limit, new hash can go to bucket 2
        assertThat(assigner2.assign(row(1), 4)).isEqualTo(2); // existing hash
        assertThat(assigner2.assign(row(1), 10)).isEqualTo(2); // new hash, same bucket
    }

    private DataFileMeta createDataFileMetaOfSize(long sizeBytes) {
        return DataFileMeta.forAppend(
                UUID.randomUUID().toString(),
                sizeBytes,
                0,
                SimpleStats.EMPTY_STATS,
                0,
                0,
                1,
                Collections.emptyList(),
                null,
                null,
                null,
                null,
                null,
                null);
    }

    private CommitMessage createCommitMessageWithDataFiles(
            BinaryRow partition,
            int bucket,
            int totalBuckets,
            IndexFileMeta indexFile,
            DataFileMeta... dataFiles) {
        return new CommitMessageImpl(
                partition,
                bucket,
                totalBuckets,
                new DataIncrement(
                        Arrays.asList(dataFiles),
                        Collections.emptyList(),
                        Collections.emptyList(),
                        Collections.singletonList(indexFile),
                        Collections.emptyList()),
                new CompactIncrement(
                        Collections.emptyList(), Collections.emptyList(), Collections.emptyList()));
    }

    /**
     * Test that bucket size is updated after compaction without new hash index.
     *
     * <p>This test demonstrates the scenario where {@code ensureBucketSizeUpdates()} is necessary:
     *
     * <ul>
     *   <li>Initial state: bucket 0 has 3 files totaling 300 bytes
     *   <li>After compaction: 3 files merged into 1 file of 100 bytes (compression effect)
     *   <li>No new hash index is created (no new keys)
     *   <li>Without {@code ensureBucketSizeUpdates()}, bucketSize would remain 300 bytes
     *   <li>With {@code ensureBucketSizeUpdates()}, bucketSize is correctly updated to 100 bytes
     * </ul>
     */
    @Test
    public void testBucketSizeUpdateAfterCompactionWithoutNewIndex() throws IOException {
        // Step 1: Commit initial data - 3 files totaling 300 bytes
        DataFileMeta file1 = createDataFileMetaOfSize(100L);
        DataFileMeta file2 = createDataFileMetaOfSize(100L);
        DataFileMeta file3 = createDataFileMetaOfSize(100L);

        commit.commit(
                0,
                Collections.singletonList(
                        createCommitMessageWithDataFiles(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5, 8}),
                                file1,
                                file2,
                                file3)));

        // Step 2: Create assigner with targetBucketSize=150 bytes
        HashBucketAssigner assigner1 = createAssigner(3, 3, 0, 5, 150L, -1);

        // Bucket 0 has 300 bytes, exceeds limit (150), new hash should create new bucket
        assertThat(assigner1.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner1.assign(row(1), 11)).isEqualTo(3); // new hash, new bucket

        // Step 3: Simulate compaction - 3 files merged into 1 file (100 bytes)
        // No new hash index because no new keys were added during compaction
        DataFileMeta compactedFile = createDataFileMetaOfSize(100L);
        CommitMessage compactionMessage =
                new CommitMessageImpl(
                        row(1),
                        0,
                        3,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Arrays.asList(file1, file2, file3), // Use same file objects
                                Collections.singletonList(compactedFile),
                                Collections.emptyList(),
                                Collections.emptyList(), // No new index files!
                                Collections.emptyList()));

        commit.commit(1, Collections.singletonList(compactionMessage));

        // Step 4: Create new assigner after compaction
        HashBucketAssigner assigner2 = createAssigner(3, 3, 0, 5, 150L, -1);

        // Now bucket 0 has only 100 bytes (< 150), new hash should go to bucket 0
        // This proves that ensureBucketSizeUpdates() correctly updated bucketSize
        assertThat(assigner2.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner2.assign(row(1), 14)).isEqualTo(0); // new hash, same bucket (size OK)
    }

    /**
     * Test that bucket size is updated after DELETE operations without new hash index.
     *
     * <p>This test demonstrates another scenario where {@code ensureBucketSizeUpdates()} is
     * necessary:
     *
     * <ul>
     *   <li>Initial state: bucket 0 has 2 files totaling 200 bytes
     *   <li>After DELETE: 1 file deleted, remaining 100 bytes
     *   <li>No new hash index is created
     *   <li>bucketSize should be updated from 200 to 100 bytes
     * </ul>
     */
    @Test
    public void testBucketSizeUpdateAfterDeleteWithoutNewIndex() throws IOException {
        // Step 1: Commit initial data - 2 files totaling 200 bytes
        DataFileMeta file1 = createDataFileMetaOfSize(100L);
        DataFileMeta file2 = createDataFileMetaOfSize(100L);

        commit.commit(
                0,
                Collections.singletonList(
                        createCommitMessageWithDataFiles(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5}),
                                file1,
                                file2)));

        // Step 2: Create assigner with targetBucketSize=150 bytes
        HashBucketAssigner assigner1 = createAssigner(3, 3, 0, 5, 150L, -1);

        // Bucket 0 has 200 bytes, exceeds limit (150)
        assertThat(assigner1.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner1.assign(row(1), 8)).isEqualTo(3); // new hash, new bucket

        // Step 3: Delete one file (no new hash index)
        CommitMessage deleteMessage =
                new CommitMessageImpl(
                        row(1),
                        0,
                        3,
                        new DataIncrement(
                                Collections.emptyList(),
                                Collections.singletonList(file1), // Delete file1
                                Collections.emptyList(),
                                Collections.emptyList(), // No new index files!
                                Collections.emptyList()),
                        CompactIncrement.emptyIncrement());

        commit.commit(1, Collections.singletonList(deleteMessage));

        // Step 4: Create new assigner after deletion
        HashBucketAssigner assigner2 = createAssigner(3, 3, 0, 5, 150L, -1);

        // Now bucket 0 has only 100 bytes (< 150), new hash should go to bucket 0
        assertThat(assigner2.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner2.assign(row(1), 11)).isEqualTo(0); // new hash, same bucket (size OK)
    }

    /**
     * Test that bucket size is updated correctly during full compaction.
     *
     * <p>This test demonstrates full compaction scenario:
     *
     * <ul>
     *   <li>Initial state: bucket 0 has many small files totaling 500 bytes
     *   <li>After full compaction: rewritten to fewer large files totaling 450 bytes (deduplication
     *       + compression)
     *   <li>No new keys, so no new hash index
     *   <li>bucketSize should be updated from 500 to 450 bytes
     * </ul>
     */
    @Test
    public void testBucketSizeUpdateAfterFullCompaction() throws IOException {
        // Step 1: Commit initial data - 5 files totaling 500 bytes
        DataFileMeta file1 = createDataFileMetaOfSize(100L);
        DataFileMeta file2 = createDataFileMetaOfSize(100L);
        DataFileMeta file3 = createDataFileMetaOfSize(100L);
        DataFileMeta file4 = createDataFileMetaOfSize(100L);
        DataFileMeta file5 = createDataFileMetaOfSize(100L);

        commit.commit(
                0,
                Collections.singletonList(
                        createCommitMessageWithDataFiles(
                                row(1),
                                0,
                                3,
                                fileHandler.hashIndex(row(1), 0).write(new int[] {2, 5, 8}),
                                file1,
                                file2,
                                file3,
                                file4,
                                file5)));

        // Step 2: Create assigner with targetBucketSize=400 bytes
        HashBucketAssigner assigner1 = createAssigner(3, 3, 0, 5, 400L, -1);

        // Bucket 0 has 500 bytes, exceeds limit (400)
        assertThat(assigner1.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner1.assign(row(1), 11)).isEqualTo(3); // new hash, new bucket

        // Step 3: Full compaction - 5 files merged into 2 files (450 bytes total)
        // Simulates deduplication and compression
        DataFileMeta compacted1 = createDataFileMetaOfSize(250L);
        DataFileMeta compacted2 = createDataFileMetaOfSize(200L);

        CommitMessage compactionMessage =
                new CommitMessageImpl(
                        row(1),
                        0,
                        3,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Arrays.asList(file1, file2, file3, file4, file5),
                                Arrays.asList(compacted1, compacted2),
                                Collections.emptyList(),
                                Collections.emptyList(), // No new index files!
                                Collections.emptyList()));

        commit.commit(1, Collections.singletonList(compactionMessage));

        // Step 4: Create new assigner after full compaction
        HashBucketAssigner assigner2 = createAssigner(3, 3, 0, 5, 400L, -1);

        // Now bucket 0 has 450 bytes, still exceeds limit (400)
        assertThat(assigner2.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner2.assign(row(1), 14)).isEqualTo(3); // new hash, still new bucket

        // Step 5: Another compaction to reduce size below limit
        DataFileMeta compacted3 = createDataFileMetaOfSize(350L);

        CommitMessage compactionMessage2 =
                new CommitMessageImpl(
                        row(1),
                        0,
                        3,
                        DataIncrement.emptyIncrement(),
                        new CompactIncrement(
                                Arrays.asList(compacted1, compacted2),
                                Collections.singletonList(compacted3),
                                Collections.emptyList(),
                                Collections.emptyList(), // No new index files!
                                Collections.emptyList()));

        commit.commit(2, Collections.singletonList(compactionMessage2));

        // Step 6: Create new assigner after second compaction
        HashBucketAssigner assigner3 = createAssigner(3, 3, 0, 5, 400L, -1);

        // Now bucket 0 has 350 bytes (< 400), new hash should go to bucket 0
        assertThat(assigner3.assign(row(1), 2)).isEqualTo(0); // existing hash
        assertThat(assigner3.assign(row(1), 17)).isEqualTo(0); // new hash, same bucket (size OK)
    }
}
