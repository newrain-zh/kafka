/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.kafka.raft.internals;

import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.errors.RecordBatchTooLargeException;
import org.apache.kafka.common.memory.MemoryPool;
import org.apache.kafka.common.message.LeaderChangeMessage;
import org.apache.kafka.common.message.SnapshotFooterRecord;
import org.apache.kafka.common.message.SnapshotHeaderRecord;
import org.apache.kafka.common.message.VotersRecord;
import org.apache.kafka.common.protocol.ObjectSerializationCache;
import org.apache.kafka.common.record.MemoryRecords;
import org.apache.kafka.common.record.MutableRecordBatch;
import org.apache.kafka.common.utils.Time;
import org.apache.kafka.raft.errors.BufferAllocationException;
import org.apache.kafka.raft.errors.NotLeaderException;
import org.apache.kafka.server.common.serialization.RecordSerde;

import java.io.Closeable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantLock;

public class BatchAccumulator<T> implements Closeable {
    @FunctionalInterface
    public interface MemoryRecordsCreator {
        MemoryRecords create(
            long baseOffset,
            int epoch,
            Compression compression,
            ByteBuffer byteBuffer
        );
    }

    private final int epoch;
    private final Time time;
    private final int lingerMs;
    private final int maxBatchSize;
    private final int maxNumberOfBatches;
    private final Compression compression;
    private final MemoryPool memoryPool;
    private final RecordSerde<T> serde;

    private final SimpleTimer lingerTimer = new SimpleTimer();
    private final AtomicLong drainOffset = new AtomicLong(Long.MAX_VALUE);
    private final ConcurrentLinkedQueue<CompletedBatch<T>> completed = new ConcurrentLinkedQueue<>();
    private volatile DrainStatus drainStatus = DrainStatus.NONE;

    // These fields are protected by the append lock
    private final ReentrantLock appendLock = new ReentrantLock();
    private long nextOffset;
    private BatchBuilder<T> currentBatch;

    private enum DrainStatus {
        STARTED, FINISHED, NONE
    }

    public BatchAccumulator(
        int epoch,
        long baseOffset,
        int lingerMs,
        int maxBatchSize,
        int maxNumberOfBatches,
        MemoryPool memoryPool,
        Time time,
        Compression compression,
        RecordSerde<T> serde
    ) {
        this.epoch = epoch;
        this.lingerMs = lingerMs;
        this.maxBatchSize = maxBatchSize;
        this.maxNumberOfBatches = maxNumberOfBatches;
        this.memoryPool = memoryPool;
        this.time = time;
        this.compression = compression;
        this.serde = serde;
        this.nextOffset = baseOffset;
    }

    /**
     * Append to the accumulator.
     * 附加到累加器。
     *
     * @param epoch the leader epoch to append at
     * @param records the records to append
     * @param delayDrain whether the records could be drained
     * @return the offset of the last record
     *
     * @throws NotLeaderException indicates that an append operation cannot be completed because the
     *         provided leader epoch was too old
     * @throws IllegalArgumentException indicates that an append operation cannot be completed
     *         because the provided leader epoch was too new
     * @throws IllegalStateException if the number of accumulated batches reaches the maximum
     *         number of batches
     */
    public long append(int epoch, List<T> records, boolean delayDrain) {
        int numberOfCompletedBatches = completed.size();
        if (epoch < this.epoch) {
            throw new NotLeaderException("Append failed because the given epoch " + epoch + " is stale. " +
                    "Current leader epoch = " + this.epoch());
        } else if (epoch > this.epoch) {
            throw new IllegalArgumentException("Attempt to append from epoch " + epoch +
                " which is larger than the current epoch " + this.epoch);
        } else if (numberOfCompletedBatches >= maxNumberOfBatches) {
            throw new IllegalStateException(
                String.format(
                    "Attempting to append records when the number of batches %s reached %s",
                    numberOfCompletedBatches,
                    maxNumberOfBatches
                )
            );
        }

        ObjectSerializationCache serializationCache = new ObjectSerializationCache();

        appendLock.lock();
        try {
            long lastOffset = nextOffset + records.size() - 1;
            maybeCompleteDrain();

            BatchBuilder<T> batch = null;
            batch = maybeAllocateBatch(records, serializationCache);
            if (batch == null) {
                throw new BufferAllocationException("Append failed because we failed to allocate memory to write the batch");
            }

            if (delayDrain) {
                // The user asked to not drain these records. If the drainOffset is not already set,
                // then set the record at the current end offset (nextOffset) as maximum offset
                // that can be drained.
                drainOffset.compareAndSet(Long.MAX_VALUE, nextOffset);
            }

            for (T record : records) {
                batch.appendRecord(record, serializationCache);
            }

            maybeResetLinger();

            nextOffset = lastOffset + 1;
            return lastOffset;
        } finally {
            appendLock.unlock();
        }
    }

    private void maybeResetLinger() {
        if (!lingerTimer.isRunning()) {
            lingerTimer.reset(time.milliseconds() + lingerMs);
        }
    }

    private BatchBuilder<T> maybeAllocateBatch(
        Collection<T> records,
        ObjectSerializationCache serializationCache
    ) {
        if (currentBatch == null) {
            startNewBatch();
        }

        if (currentBatch != null) {
            OptionalInt bytesNeeded = currentBatch.bytesNeeded(records, serializationCache);
            if (bytesNeeded.isPresent() && bytesNeeded.getAsInt() > maxBatchSize) {
                throw new RecordBatchTooLargeException(
                    String.format(
                        "The total record(s) size of %d exceeds the maximum allowed batch size of %d",
                        bytesNeeded.getAsInt(),
                        maxBatchSize
                    )
                );
            } else if (bytesNeeded.isPresent()) {
                completeCurrentBatch();
                startNewBatch();
            }
        }

        return currentBatch;
    }

    private void completeCurrentBatch() {
        MemoryRecords data = currentBatch.build();
        completed.add(new CompletedBatch<>(
            currentBatch.baseOffset(),
            currentBatch.records(),
            data,
            memoryPool,
            currentBatch.initialBuffer()
        ));
        currentBatch = null;
    }

    /**
     * Allows draining of all batches.
     */
    public void allowDrain() {
        drainOffset.set(Long.MAX_VALUE);
    }

    /**
     * Append a control batch from a supplied memory record.
     *
     * See the {@code valueCreator} parameter description for requirements on this function.
     *
     * @param valueCreator a function that uses the passed buffer to create the control
     *        batch that will be appended. The memory records returned must contain one
     *        control batch and that control batch have at least one record.
     * @return the last of offset of the records created
     */
    public long appendControlMessages(MemoryRecordsCreator valueCreator) {
        appendLock.lock();
        try {
            ByteBuffer buffer = memoryPool.tryAllocate(maxBatchSize);
            if (buffer != null) {
                try {
                    forceDrain();
                    MemoryRecords memoryRecords = valueCreator.create(
                        nextOffset,
                        epoch,
                        compression,
                        buffer
                    );

                    int numberOfRecords = validateMemoryRecordsAndReturnCount(memoryRecords);

                    completed.add(
                        new CompletedBatch<>(
                            nextOffset,
                            numberOfRecords,
                            memoryRecords,
                            memoryPool,
                            buffer
                        )
                    );
                    nextOffset += numberOfRecords;
                } catch (Exception e) {
                    // Release the buffer now since the buffer was not stored in completed for a delayed release
                    memoryPool.release(buffer);
                    throw e;
                }
            } else {
                throw new IllegalStateException("Could not allocate buffer for the control record");
            }

            return nextOffset - 1;
        } finally {
            appendLock.unlock();
        }
    }

    private int validateMemoryRecordsAndReturnCount(MemoryRecords memoryRecords) {
        // Confirm that it is one control batch and it is at least one control record
        Iterator<MutableRecordBatch> batches = memoryRecords.batches().iterator();
        if (!batches.hasNext()) {
            throw new IllegalArgumentException("valueCreator didn't create a batch");
        }

        MutableRecordBatch batch = batches.next();
        Integer numberOfRecords = batch.countOrNull();
        if (!batch.isControlBatch()) {
            throw new IllegalArgumentException("valueCreator didn't create a control batch");
        } else if (batch.baseOffset() != nextOffset) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected a base offset of %d but got %d",
                    nextOffset,
                    batch.baseOffset()
                )
            );
        } else if (batch.partitionLeaderEpoch() != epoch) {
            throw new IllegalArgumentException(
                String.format(
                    "Expected a partition leader epoch of %d but got %d",
                    epoch,
                    batch.partitionLeaderEpoch()
                )
            );
        } else if (numberOfRecords == null) {
            throw new IllegalArgumentException("valueCreator didn't create a batch with the count");
        } else if (numberOfRecords < 1) {
            throw new IllegalArgumentException("valueCreator didn't create at least one control record");
        } else if (batches.hasNext()) {
            throw new IllegalArgumentException("valueCreator created more than one batch");
        }

        return numberOfRecords;
    }

    /**
     * Append a {@link VotersRecord} record to the batch
     *
     * @param voters the record to append
     * @param currentTimestamp the current time in milliseconds
     * @return the last of offset of the records created
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public long appendVotersRecord(
        VotersRecord voters,
        long currentTimestamp
    ) {
        return appendControlMessages((baseOffset, epoch, compression, buffer) ->
            MemoryRecords.withVotersRecord(
                baseOffset,
                currentTimestamp,
                epoch,
                buffer,
                voters
            )
        );
    }


    /**
     * Append a {@link LeaderChangeMessage} record to the batch
     *
     * @param leaderChangeMessage The message to append
     * @param currentTimestamp The current time in milliseconds
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public void appendLeaderChangeMessage(
        LeaderChangeMessage leaderChangeMessage,
        long currentTimestamp
    ) {
        appendControlMessages((baseOffset, epoch, compression, buffer) ->
            MemoryRecords.withLeaderChangeMessage(
                baseOffset,
                currentTimestamp,
                epoch,
                buffer,
                leaderChangeMessage
            )
        );
    }


    /**
     * Append a {@link SnapshotHeaderRecord} record to the batch
     *
     * @param snapshotHeaderRecord The record to append
     * @param currentTimestamp The current time in milliseconds
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public void appendSnapshotHeaderRecord(
        SnapshotHeaderRecord snapshotHeaderRecord,
        long currentTimestamp
    ) {
        appendControlMessages((baseOffset, epoch, compression, buffer) ->
            MemoryRecords.withSnapshotHeaderRecord(
                baseOffset,
                currentTimestamp,
                epoch,
                buffer,
                snapshotHeaderRecord
            )
        );
    }

    /**
     * Append a {@link SnapshotFooterRecord} record to the batch
     *
     * @param snapshotFooterRecord The record to append
     * @param currentTimestamp The current time in milliseconds
     * @throws IllegalStateException on failure to allocate a buffer for the record
     */
    public void appendSnapshotFooterRecord(
        SnapshotFooterRecord snapshotFooterRecord,
        long currentTimestamp
    ) {
        appendControlMessages((baseOffset, epoch, compression, buffer) ->
            MemoryRecords.withSnapshotFooterRecord(
                baseOffset,
                currentTimestamp,
                epoch,
                buffer,
                snapshotFooterRecord
            )
        );
    }

    public void forceDrain() {
        appendLock.lock();
        try {
            drainStatus = DrainStatus.STARTED;
            maybeCompleteDrain();
        } finally {
            appendLock.unlock();
        }
    }

    private void maybeCompleteDrain() {
        if (drainStatus == DrainStatus.STARTED) {
            if (currentBatch != null && currentBatch.nonEmpty()) {
                completeCurrentBatch();
            }
            // Reset the timer to a large value. The linger clock will begin
            // ticking after the next append.
            // 将计时器重置为较大的值。linger clock 将在下一次 append 后开始滴答作响。
            lingerTimer.reset(Long.MAX_VALUE);
            drainStatus = DrainStatus.FINISHED;
        }
    }

    private void startNewBatch() {
        ByteBuffer buffer = memoryPool.tryAllocate(maxBatchSize);
        if (buffer != null) {
            currentBatch = new BatchBuilder<>(
                buffer,
                serde,
                compression,
                nextOffset,
                time.milliseconds(),
                epoch,
                maxBatchSize
            );
        }
    }

    /**
     * Check whether there are any batches which need to be drained now.
     *
     * @param currentTimeMs current time in milliseconds
     * @return true if there are batches ready to drain, false otherwise
     */
    public boolean needsDrain(long currentTimeMs) {
        return timeUntilDrain(currentTimeMs) <= 0;
    }

    /**
     * Check the time remaining until the next needed drain. If the accumulator
     * is empty, then {@link Long#MAX_VALUE} will be returned.
     *
     * @param currentTimeMs current time in milliseconds
     * @return the delay in milliseconds before the next expected drain
     */
    public long timeUntilDrain(long currentTimeMs) {
        boolean drainableBatches = Optional.ofNullable(completed.peek())
            .map(batch -> batch.drainable(drainOffset.get()))
            .orElse(false);
        if (drainableBatches) {
            return 0;
        } else {
            return lingerTimer.remainingMs(currentTimeMs);
        }
    }

    /**
     * Get the leader epoch, which is constant for each instance.
     *
     * @return the leader epoch
     */
    public int epoch() {
        return epoch;
    }

    /**
     * Drain completed batches. The caller is expected to first check whether
     * {@link #needsDrain(long)} returns true in order to avoid unnecessary draining.
     *
     * Note on thread-safety: this method is safe in the presence of concurrent
     * appends, but it assumes a single thread is responsible for draining.
     *
     * This call will not block, but the drain may require multiple attempts before
     * it can be completed if the thread responsible for appending is holding the
     * append lock. In the worst case, the append will be completed on the next
     * call to {@link #append(int, List, boolean)} following the
     * initial call to this method.
     *
     * The caller should respect the time to the next flush as indicated by
     * {@link #timeUntilDrain(long)}.
     *
     * @return the list of completed batches
     */
    public List<CompletedBatch<T>> drain() {
        return drain(drainOffset.get());
    }

    private List<CompletedBatch<T>> drain(long drainOffset) {
        // Start the drain if it has not been started already
        if (drainStatus == DrainStatus.NONE) {
            drainStatus = DrainStatus.STARTED;
        }

        // Complete the drain ourselves if we can acquire the lock
        if (appendLock.tryLock()) {
            try {
                maybeCompleteDrain();
            } finally {
                appendLock.unlock();
            }
        }

        // If the drain has finished, then all of the batches will be completed
        if (drainStatus == DrainStatus.FINISHED) {
            drainStatus = DrainStatus.NONE;
            return drainCompleted(drainOffset);
        } else {
            return List.of();
        }
    }

    private List<CompletedBatch<T>> drainCompleted(long drainOffset) {
        List<CompletedBatch<T>> res = new ArrayList<>();
        while (true) {
            CompletedBatch<T> batch = completed.peek();
            if (batch == null || !batch.drainable(drainOffset)) {
                return res;
            } else {
                // The batch can be drained so remove the batch and add it to the result.
                completed.poll();
                res.add(batch);
            }
        }
    }

    public boolean isEmpty() {
        // The linger timer begins running when we have pending batches.
        // We use this to infer when the accumulator is empty to avoid the
        // need to acquire the append lock.
        return !lingerTimer.isRunning();
    }

    @Override
    public void close() {
        // Acquire the lock so that drain is guaranteed to complete the current batch
        appendLock.lock();
        List<CompletedBatch<T>> unwritten;
        try {
            unwritten = drain(Long.MAX_VALUE);
        } finally {
            appendLock.unlock();
        }
        unwritten.forEach(CompletedBatch::release);
    }

    public static class CompletedBatch<T> {
        public final long baseOffset;
        public final int numRecords;
        public final Optional<List<T>> records;
        public final MemoryRecords data;
        private final MemoryPool pool;
        // Buffer that was allocated by the MemoryPool (pool). This may not be the buffer used in
        // the MemoryRecords (data) object.
        private final ByteBuffer initialBuffer;

        private CompletedBatch(
            long baseOffset,
            List<T> records,
            MemoryRecords data,
            MemoryPool pool,
            ByteBuffer initialBuffer
        ) {
            this.baseOffset = baseOffset;
            this.records = Optional.of(records);
            this.numRecords = records.size();
            this.data = data;
            this.pool = pool;
            this.initialBuffer = initialBuffer;

            validateContruction();
        }

        private CompletedBatch(
            long baseOffset,
            int numRecords,
            MemoryRecords data,
            MemoryPool pool,
            ByteBuffer initialBuffer
        ) {
            this.baseOffset = baseOffset;
            this.records = Optional.empty();
            this.numRecords = numRecords;
            this.data = data;
            this.pool = pool;
            this.initialBuffer = initialBuffer;

            validateContruction();
        }

        private void validateContruction() {
            Objects.requireNonNull(data.firstBatch(), "Expected memory records to contain one batch");

            if (numRecords <= 0) {
                throw new IllegalArgumentException(
                    String.format("Completed batch must contain at least one record: %s", numRecords)
                );
            }
        }

        public int sizeInBytes() {
            return data.sizeInBytes();
        }

        public void release() {
            pool.release(initialBuffer);
        }

        public long appendTimestamp() {
            // 1. firstBatch is not null because data has one and only one batch
            // 2. maxTimestamp is the append time of the batch. This needs to be changed
            //    to return the LastContainedLogTimestamp of the SnapshotHeaderRecord
            return data.firstBatch().maxTimestamp();
        }

        public boolean drainable(long drainOffset) {
            return baseOffset + numRecords - 1 < drainOffset;
        }
    }

    private static class SimpleTimer {
        // We use an atomic long so that the Raft IO thread can query the linger
        // time without any locking
        private final AtomicLong deadlineMs = new AtomicLong(Long.MAX_VALUE);

        boolean isRunning() {
            return deadlineMs.get() != Long.MAX_VALUE;
        }

        void reset(long deadlineMs) {
            this.deadlineMs.set(deadlineMs);
        }

        long remainingMs(long currentTimeMs) {
            return Math.max(0, deadlineMs.get() - currentTimeMs);
        }
    }
}