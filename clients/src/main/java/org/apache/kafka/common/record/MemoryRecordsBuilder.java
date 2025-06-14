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
package org.apache.kafka.common.record;

import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.compress.Compression;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.message.*;
import org.apache.kafka.common.protocol.MessageUtil;
import org.apache.kafka.common.protocol.types.Struct;
import org.apache.kafka.common.utils.ByteBufferOutputStream;
import org.apache.kafka.common.utils.Utils;

import java.io.DataOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import static org.apache.kafka.common.utils.Utils.wrapNullable;

/**
 * This class is used to write new log data in memory, i.e. this is the write path for {@link MemoryRecords}.
 * It transparently handles compression and exposes methods for appending new records, possibly with message
 * format conversion.
 * <p>
 * In cases where keeping memory retention low is important and there's a gap between the time that record appends stop
 * and the builder is closed (e.g. the Producer), it's important to call `closeForRecordAppends` when the former happens.
 * This will release resources like compression buffers that can be relatively large (64 KB for LZ4).
 */
public class MemoryRecordsBuilder implements AutoCloseable {
    private static final float            COMPRESSION_RATE_ESTIMATION_FACTOR = 1.05f;
    private static final DataOutputStream CLOSED_STREAM                      = new DataOutputStream(new OutputStream() {
        @Override
        public void write(int b) {
            throw new IllegalStateException("MemoryRecordsBuilder is closed for record appends");
        }
    });

    private final TimestampType          timestampType;
    private final Compression            compression; // 压缩算法
    // Used to hold a reference to the underlying ByteBuffer so that we can write the record batch header and access
    // the written bytes. ByteBufferOutputStream allocates a new ByteBuffer if the existing one is not large enough,
    // so it's not safe to hold a direct reference to the underlying ByteBuffer.
    private final ByteBufferOutputStream bufferStream; // 底层字节缓冲区，动态扩容以适应消息写入，用于构建内存中的消息批次
    private final byte                   magic;
    private final int                    initialPosition;
    private final long                   baseOffset; // 批次中第一条消息的绝对偏移量，用于计算相对偏移量 offset delta
    private final long                   logAppendTime; // 日志追加时间
    private final boolean                isControlBatch;
    private final int                    partitionLeaderEpoch; // 分区领导者的 epoch ，防止过期 Leader写入
    private final int                    writeLimit; // 批次最大允许字节数（含压缩），超过该值会触发批次发送或拆分
    private final int                    batchHeaderSizeInBytes; // 批次头大小(不含消息体)
    private final long                   deleteHorizonMs;

    // Use a conservative estimate of the compression ratio. The producer overrides this using statistics
    // from previous batches before appending any records.
    private float estimatedCompressionRatio = 1.0F;

    // Used to append records, may compress data on the fly
    // 用于追加记录，可以动态压缩数据
    // 以下变量都是运行时的状态属性
    private DataOutputStream appendStream;
    private boolean          isTransactional;
    private long             producerId;
    private short            producerEpoch;
    private int              baseSequence;
    private int              uncompressedRecordsSizeInBytes; // Number of bytes (excluding the header) written before compression
    private int              numRecords;
    private float            actualCompressionRatio;
    private long             maxTimestamp;
    private long             offsetOfMaxTimestamp = -1;
    private Long             lastOffset           = null; // 批次中最后一条消息的绝对偏移量，用于偏移量连续性校验
    private Long             baseTimestamp        = null;

    private MemoryRecords builtRecords; //构建完成的批次对象，调用 build()后生产，表示批次已经就绪
    private boolean       aborted = false; // 批次是否中止（如生产者关闭），中止后不可再修改

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream, byte magic, Compression compression, TimestampType timestampType, long baseOffset, long logAppendTime, long producerId, short producerEpoch, int baseSequence, boolean isTransactional, boolean isControlBatch, int partitionLeaderEpoch, int writeLimit, long deleteHorizonMs) {
        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestampType == TimestampType.NO_TIMESTAMP_TYPE)
            throw new IllegalArgumentException("TimestampType must be set for magic >= 0");
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            if (isTransactional)
                throw new IllegalArgumentException("Transactional records are not supported for magic " + magic);
            if (isControlBatch)
                throw new IllegalArgumentException("Control records are not supported for magic " + magic);
            if (compression.type() == CompressionType.ZSTD)
                throw new IllegalArgumentException("ZStandard compression is not supported for magic " + magic);
            if (deleteHorizonMs != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Delete horizon timestamp is not supported for magic " + magic);
        }

        this.magic                          = magic;
        this.timestampType                  = timestampType;
        this.compression                    = compression;
        this.baseOffset                     = baseOffset;
        this.logAppendTime                  = logAppendTime;
        this.numRecords                     = 0;
        this.uncompressedRecordsSizeInBytes = 0;
        this.actualCompressionRatio         = 1;
        this.maxTimestamp                   = RecordBatch.NO_TIMESTAMP;
        this.producerId                     = producerId;
        this.producerEpoch                  = producerEpoch;
        this.baseSequence                   = baseSequence;
        this.isTransactional                = isTransactional;
        this.isControlBatch                 = isControlBatch;
        this.deleteHorizonMs                = deleteHorizonMs;
        this.partitionLeaderEpoch           = partitionLeaderEpoch;
        this.writeLimit                     = writeLimit;
        this.initialPosition                = bufferStream.position();
        this.batchHeaderSizeInBytes         = AbstractRecords.recordBatchHeaderSizeInBytes(magic, compression.type());

        bufferStream.position(initialPosition + batchHeaderSizeInBytes);
        this.bufferStream = bufferStream;
        this.appendStream = new DataOutputStream(compression.wrapForOutput(this.bufferStream, magic));

        if (hasDeleteHorizonMs()) {
            this.baseTimestamp = deleteHorizonMs;
        }
    }

    public MemoryRecordsBuilder(ByteBufferOutputStream bufferStream, byte magic, Compression compression, TimestampType timestampType, long baseOffset, long logAppendTime, long producerId, short producerEpoch, int baseSequence, boolean isTransactional, boolean isControlBatch, int partitionLeaderEpoch, int writeLimit) {
        this(bufferStream, magic, compression, timestampType, baseOffset, logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit, RecordBatch.NO_TIMESTAMP);
    }

    /**
     * Construct a new builder.
     *
     * @param buffer               The underlying buffer to use (note that this class will allocate a new buffer if necessary
     *                             to fit the records appended)
     * @param magic                The magic value to use
     * @param compression          The compression codec to use
     * @param timestampType        The desired timestamp type. For magic > 0, this cannot be {@link TimestampType#NO_TIMESTAMP_TYPE}.
     * @param baseOffset           The initial offset to use for
     * @param logAppendTime        The log append time of this record set. Can be set to NO_TIMESTAMP if CREATE_TIME is used.
     * @param producerId           The producer ID associated with the producer writing this record set
     * @param producerEpoch        The epoch of the producer
     * @param baseSequence         The sequence number of the first record in this set
     * @param isTransactional      Whether or not the records are part of a transaction
     * @param isControlBatch       Whether or not this is a control batch (e.g. for transaction markers)
     * @param partitionLeaderEpoch The epoch of the partition leader appending the record set to the log
     * @param writeLimit           The desired limit on the total bytes for this record set (note that this can be exceeded
     *                             when compression is used since size estimates are rough, and in the case that the first
     *                             record added exceeds the size).
     */
    public MemoryRecordsBuilder(ByteBuffer buffer, byte magic, Compression compression, TimestampType timestampType, long baseOffset, long logAppendTime, long producerId, short producerEpoch, int baseSequence, boolean isTransactional, boolean isControlBatch, int partitionLeaderEpoch, int writeLimit) {
        this(new ByteBufferOutputStream(buffer), magic, compression, timestampType, baseOffset, logAppendTime, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, partitionLeaderEpoch, writeLimit);
    }

    public ByteBuffer buffer() {
        return bufferStream.buffer();
    }

    public int initialCapacity() {
        return bufferStream.initialCapacity();
    }

    public double compressionRatio() {
        return actualCompressionRatio;
    }

    public Compression compression() {
        return compression;
    }

    public boolean isControlBatch() {
        return isControlBatch;
    }

    public boolean isTransactional() {
        return isTransactional;
    }

    public final boolean hasDeleteHorizonMs() {
        return magic >= RecordBatch.MAGIC_VALUE_V2 && deleteHorizonMs >= 0L;
    }

    /**
     * Close this builder and return the resulting buffer.
     *
     * @return The built log buffer
     */
    public MemoryRecords build() {
        if (aborted) {
            throw new IllegalStateException("Attempting to build an aborted record batch");
        }
        close();
        return builtRecords;
    }


    /**
     * There are three cases of finding max timestamp to return:
     * 1) version 0: The max timestamp is NO_TIMESTAMP (-1)
     * 2) LogAppendTime: All records have same timestamp, and so the max timestamp is equal to logAppendTime
     * 3) CreateTime: The max timestamp of record
     * <p>
     * Let's talk about OffsetOfMaxTimestamp. There are some paths that we don't try to find the OffsetOfMaxTimestamp
     * to avoid expensive records iteration. Those paths include follower append and index recovery. In order to
     * avoid inconsistent time index, we let all paths find shallowOffsetOfMaxTimestamp instead of OffsetOfMaxTimestamp.
     * <p>
     * Let's define the shallowOffsetOfMaxTimestamp: It is last offset of the batch having max timestamp. If there are
     * many batches having same max timestamp, we pick up the earliest batch.
     * <p>
     * There are five cases of finding shallowOffsetOfMaxTimestamp to return:
     * 1) version 0: It is always the -1
     * 2) LogAppendTime with single batch: It is the offset of last record
     * 3) LogAppendTime with many single-record batches: Those single-record batches have same max timestamp, so we return
     * the base offset, which is equal to the last offset of earliest batch
     * 4) CreateTime with single batch: We return offset of last record to follow the spec we mentioned above. Of course,
     * we do have the OffsetOfMaxTimestamp for this case, but we want to make all paths
     * find the shallowOffsetOfMaxTimestamp rather than offsetOfMaxTimestamp
     * 5) CreateTime with many single-record batches: Each batch is composed of single record, and hence offsetOfMaxTimestamp
     * is equal to the last offset of earliest batch with max timestamp
     */
    public RecordsInfo info() {
        if (timestampType == TimestampType.LOG_APPEND_TIME) {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                // maxTimestamp => case 2
                // shallowOffsetOfMaxTimestamp => case 2
                return new RecordsInfo(logAppendTime, lastOffset);
            else
                // maxTimestamp => case 2
                // shallowOffsetOfMaxTimestamp => case 3
                return new RecordsInfo(logAppendTime, baseOffset);
        } else if (maxTimestamp == RecordBatch.NO_TIMESTAMP) {
            // maxTimestamp => case 1
            // shallowOffsetOfMaxTimestamp => case 1
            return new RecordsInfo(RecordBatch.NO_TIMESTAMP, -1);
        } else {
            if (compression.type() != CompressionType.NONE || magic >= RecordBatch.MAGIC_VALUE_V2)
                // maxTimestamp => case 3
                // shallowOffsetOfMaxTimestamp => case 4
                return new RecordsInfo(maxTimestamp, lastOffset);
            else
                // maxTimestamp => case 3
                // shallowOffsetOfMaxTimestamp => case 5
                return new RecordsInfo(maxTimestamp, offsetOfMaxTimestamp);
        }
    }

    public int numRecords() {
        return numRecords;
    }

    /**
     * Return the sum of the size of the batch header (always uncompressed) and the records (before compression).
     */
    public int uncompressedBytesWritten() {
        return uncompressedRecordsSizeInBytes + batchHeaderSizeInBytes;
    }

    public void setProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (isClosed()) {
            // Sequence numbers are assigned when the batch is closed while the accumulator is being drained.
            // If the resulting ProduceRequest to the partition leader failed for a retriable error, the batch will
            // be re queued. In this case, we should not attempt to set the state again, since changing the producerId and sequence
            // once a batch has been sent to the broker risks introducing duplicates.
            throw new IllegalStateException("Trying to set producer state of an already closed batch. This indicates a bug on the client.");
        }
        this.producerId      = producerId;
        this.producerEpoch   = producerEpoch;
        this.baseSequence    = baseSequence;
        this.isTransactional = isTransactional;
    }

    public void overrideLastOffset(long lastOffset) {
        if (builtRecords != null)
            throw new IllegalStateException("Cannot override the last offset after the records have been built");
        this.lastOffset = lastOffset;
    }

    /**
     * Release resources required for record appends (e.g. compression buffers). Once this method is called, it's only
     * possible to update the RecordBatch header.
     */
    public void closeForRecordAppends() {
        if (appendStream != CLOSED_STREAM) {
            try {
                appendStream.close();
            } catch (IOException e) {
                throw new KafkaException(e);
            } finally {
                appendStream = CLOSED_STREAM;
            }
        }
    }

    public void abort() {
        closeForRecordAppends();
        buffer().position(initialPosition);
        aborted = true;
    }

    public void reopenAndRewriteProducerState(long producerId, short producerEpoch, int baseSequence, boolean isTransactional) {
        if (aborted) throw new IllegalStateException("Should not reopen a batch which is already aborted.");
        builtRecords         = null;
        this.producerId      = producerId;
        this.producerEpoch   = producerEpoch;
        this.baseSequence    = baseSequence;
        this.isTransactional = isTransactional;
    }


    public void close() {
        if (aborted)
            throw new IllegalStateException("Cannot close MemoryRecordsBuilder as it has already been aborted");

        if (builtRecords != null) return;

        validateProducerState();

        closeForRecordAppends();

        if (numRecords == 0L) {
            buffer().position(initialPosition);
            builtRecords = MemoryRecords.EMPTY;
        } else {
            if (magic > RecordBatch.MAGIC_VALUE_V1) this.actualCompressionRatio = (float) writeDefaultBatchHeader() / this.uncompressedRecordsSizeInBytes;
            else if (compression.type() != CompressionType.NONE) this.actualCompressionRatio = (float) writeLegacyCompressedWrapperHeader() / this.uncompressedRecordsSizeInBytes;

            ByteBuffer buffer = buffer().duplicate();
            buffer.flip();
            buffer.position(initialPosition);
            builtRecords = MemoryRecords.readableRecords(buffer.slice());
        }
    }

    private void validateProducerState() {
        if (isTransactional && producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("Cannot write transactional messages without a valid producer ID");

        if (producerId != RecordBatch.NO_PRODUCER_ID) {
            if (producerEpoch == RecordBatch.NO_PRODUCER_EPOCH)
                throw new IllegalArgumentException("Invalid negative producer epoch");

            if (baseSequence < 0 && !isControlBatch)
                throw new IllegalArgumentException("Invalid negative sequence number used");

            if (magic < RecordBatch.MAGIC_VALUE_V2)
                throw new IllegalArgumentException("Idempotent messages are not supported for magic " + magic);
        }
    }

    /**
     * Write the header to the default batch.
     *
     * @return the written compressed bytes.
     */
    private int writeDefaultBatchHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int        pos    = buffer.position();
        buffer.position(initialPosition);
        int size              = pos - initialPosition;
        int writtenCompressed = size - DefaultRecordBatch.RECORD_BATCH_OVERHEAD;
        int offsetDelta       = (int) (lastOffset - baseOffset);

        final long maxTimestamp;
        if (timestampType == TimestampType.LOG_APPEND_TIME) maxTimestamp = logAppendTime;
        else maxTimestamp = this.maxTimestamp;

        DefaultRecordBatch.writeHeader(buffer, baseOffset, offsetDelta, size, magic, compression.type(), timestampType, baseTimestamp, maxTimestamp, producerId, producerEpoch, baseSequence, isTransactional, isControlBatch, hasDeleteHorizonMs(), partitionLeaderEpoch, numRecords);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Write the header to the legacy batch.
     *
     * @return the written compressed bytes.
     */
    private int writeLegacyCompressedWrapperHeader() {
        ensureOpenForRecordBatchWrite();
        ByteBuffer buffer = bufferStream.buffer();
        int        pos    = buffer.position();
        buffer.position(initialPosition);

        int wrapperSize       = pos - initialPosition - Records.LOG_OVERHEAD;
        int writtenCompressed = wrapperSize - LegacyRecord.recordOverhead(magic);
        AbstractLegacyRecordBatch.writeHeader(buffer, lastOffset, wrapperSize);

        long timestamp = timestampType == TimestampType.LOG_APPEND_TIME ? logAppendTime : maxTimestamp;
        LegacyRecord.writeCompressedRecordHeader(buffer, magic, wrapperSize, timestamp, compression.type(), timestampType);

        buffer.position(pos);
        return writtenCompressed;
    }

    /**
     * Append a new record at the given offset.
     * 在给定的偏移量处附加一条新记录。
     */
    private void appendWithOffset(long offset, // 记录在日志中的绝对偏移量
                                  boolean isControlRecord,//是否为控制记录（如事务标记）
                                  long timestamp, //  记录的时间戳
                                  ByteBuffer key, // 消息 key
                                  ByteBuffer value, // 消息 value
                                  Header[] headers) {
        try {
            if (isControlRecord != isControlBatch)
                throw new IllegalArgumentException("Control records can only be appended to control batches");

            if (lastOffset != null && offset <= lastOffset)
                throw new IllegalArgumentException(String.format("Illegal offset %d following previous offset %d " + "(Offsets must increase monotonically).", offset, lastOffset));

            if (timestamp < 0 && timestamp != RecordBatch.NO_TIMESTAMP)
                throw new IllegalArgumentException("Invalid negative timestamp " + timestamp);

            if (magic < RecordBatch.MAGIC_VALUE_V2 && headers != null && headers.length > 0)
                throw new IllegalArgumentException("Magic v" + magic + " does not support record headers");

            if (baseTimestamp == null) baseTimestamp = timestamp;

            if (magic > RecordBatch.MAGIC_VALUE_V1) {
                appendDefaultRecord(offset, timestamp, key, value, headers);
            } else {
                appendLegacyRecord(offset, timestamp, key, value, magic);
            }
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a new record at the given offset.
     *
     * @param offset    The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     * @param headers   The record headers if there are any
     */
    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the given offset.
     *
     * @param offset    The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     * @param headers   The record headers if there are any
     */
    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        appendWithOffset(offset, false, timestamp, key, value, headers);
    }

    /**
     * Append a new record at the given offset.
     *
     * @param offset    The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     */
    public void appendWithOffset(long offset, long timestamp, byte[] key, byte[] value) {
        appendWithOffset(offset, timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     *
     * @param offset    The absolute offset of the record in the log buffer
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     */
    public void appendWithOffset(long offset, long timestamp, ByteBuffer key, ByteBuffer value) {
        appendWithOffset(offset, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the given offset.
     *
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     */
    public void appendWithOffset(long offset, SimpleRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a control record at the given offset. The control record type must be known or
     * this method will raise an error.
     *
     * @param offset The absolute offset of the record in the log buffer
     * @param record The record to append
     */
    public void appendControlRecordWithOffset(long offset, SimpleRecord record) {
        short             typeId = ControlRecordType.parseTypeId(record.key());
        ControlRecordType type   = ControlRecordType.fromTypeId(typeId);
        if (type == ControlRecordType.UNKNOWN)
            throw new IllegalArgumentException("Cannot append record with unknown control record type " + typeId);

        appendWithOffset(offset, true, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a new record at the next sequential offset.
     *
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     */
    public void append(long timestamp, ByteBuffer key, ByteBuffer value) {
        append(timestamp, key, value, Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     *
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     * @param headers   The record headers if there are any
     */
    public void append(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        appendWithOffset(nextSequentialOffset(), timestamp, key, value, headers);
    }

    /**
     * Append a new record at the next sequential offset.
     *
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     */
    public void append(long timestamp, byte[] key, byte[] value) {
        append(timestamp, wrapNullable(key), wrapNullable(value), Record.EMPTY_HEADERS);
    }

    /**
     * Append a new record at the next sequential offset.
     *
     * @param timestamp The record timestamp
     * @param key       The record key
     * @param value     The record value
     * @param headers   The record headers if there are any
     */
    public void append(long timestamp, byte[] key, byte[] value, Header[] headers) {
        append(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Append a new record at the next sequential offset.
     *
     * @param record The record to append
     */
    public void append(SimpleRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    /**
     * Append a control record at the next sequential offset.
     *
     * @param timestamp The record timestamp
     * @param type      The control record type (cannot be UNKNOWN)
     * @param value     The control record value
     */
    public void appendControlRecord(long timestamp, ControlRecordType type, ByteBuffer value) {
        Struct     keyStruct = type.recordKey();
        ByteBuffer key       = ByteBuffer.allocate(keyStruct.sizeOf());
        keyStruct.writeTo(key);
        key.flip();
        appendWithOffset(nextSequentialOffset(), true, timestamp, key, value, Record.EMPTY_HEADERS);
    }

    public void appendEndTxnMarker(long timestamp, EndTransactionMarker marker) {
        if (producerId == RecordBatch.NO_PRODUCER_ID)
            throw new IllegalArgumentException("End transaction marker requires a valid producerId");
        if (!isTransactional)
            throw new IllegalArgumentException("End transaction marker depends on batch transactional flag being enabled");
        ByteBuffer value = marker.serializeValue();
        appendControlRecord(timestamp, marker.controlType(), value);
    }

    public void appendLeaderChangeMessage(long timestamp, LeaderChangeMessage leaderChangeMessage) {
        if (partitionLeaderEpoch == RecordBatch.NO_PARTITION_LEADER_EPOCH) {
            throw new IllegalArgumentException("Partition leader epoch must be valid, but get " + partitionLeaderEpoch);
        }
        appendControlRecord(timestamp, ControlRecordType.LEADER_CHANGE, MessageUtil.toByteBufferAccessor(leaderChangeMessage, ControlRecordUtils.LEADER_CHANGE_CURRENT_VERSION).buffer());
    }

    public void appendSnapshotHeaderMessage(long timestamp, SnapshotHeaderRecord snapshotHeaderRecord) {
        appendControlRecord(timestamp, ControlRecordType.SNAPSHOT_HEADER, MessageUtil.toByteBufferAccessor(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_HEADER_CURRENT_VERSION).buffer());
    }

    public void appendSnapshotFooterMessage(long timestamp, SnapshotFooterRecord snapshotHeaderRecord) {
        appendControlRecord(timestamp, ControlRecordType.SNAPSHOT_FOOTER, MessageUtil.toByteBufferAccessor(snapshotHeaderRecord, ControlRecordUtils.SNAPSHOT_FOOTER_CURRENT_VERSION).buffer());
    }

    public void appendKRaftVersionMessage(long timestamp, KRaftVersionRecord kraftVersionRecord) {
        appendControlRecord(timestamp, ControlRecordType.KRAFT_VERSION, MessageUtil.toByteBufferAccessor(kraftVersionRecord, ControlRecordUtils.KRAFT_VERSION_CURRENT_VERSION).buffer());
    }

    public void appendVotersMessage(long timestamp, VotersRecord votersRecord) {
        appendControlRecord(timestamp, ControlRecordType.KRAFT_VOTERS, MessageUtil.toByteBufferAccessor(votersRecord, ControlRecordUtils.KRAFT_VOTERS_CURRENT_VERSION).buffer());
    }

    /**
     * Add a legacy record without doing offset/magic validation (this should only be used in testing).
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, LegacyRecord record) {
        ensureOpenForRecordAppend();
        try {
            int size = record.sizeInBytes();
            AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

            ByteBuffer buffer = record.buffer().duplicate();
            appendStream.write(buffer.array(), buffer.arrayOffset(), buffer.limit());

            recordWritten(offset, record.timestamp(), size + Records.LOG_OVERHEAD);
        } catch (IOException e) {
            throw new KafkaException("I/O exception when writing to the append stream, closing", e);
        }
    }

    /**
     * Append a record without doing offset/magic validation (this should only be used in testing).
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendUncheckedWithOffset(long offset, SimpleRecord record) throws IOException {
        if (magic >= RecordBatch.MAGIC_VALUE_V2) {
            int  offsetDelta = (int) (offset - baseOffset);
            long timestamp   = record.timestamp();
            if (baseTimestamp == null) baseTimestamp = timestamp;

            int sizeInBytes = DefaultRecord.writeTo(appendStream, offsetDelta, timestamp - baseTimestamp, record.key(), record.value(), record.headers());
            recordWritten(offset, timestamp, sizeInBytes);
        } else {
            LegacyRecord legacyRecord = LegacyRecord.create(magic, record.timestamp(), Utils.toNullableArray(record.key()), Utils.toNullableArray(record.value()));
            appendUncheckedWithOffset(offset, legacyRecord);
        }
    }

    /**
     * Append a record at the next sequential offset.
     *
     * @param record the record to add
     */
    public void append(Record record) {
        appendWithOffset(record.offset(), isControlBatch, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Append a log record using a different offset
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, Record record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value(), record.headers());
    }

    /**
     * Add a record with a given offset. The record must have a magic which matches the magic use to
     * construct this builder and the offset must be greater than the last appended record.
     *
     * @param offset The offset of the record
     * @param record The record to add
     */
    public void appendWithOffset(long offset, LegacyRecord record) {
        appendWithOffset(offset, record.timestamp(), record.key(), record.value());
    }

    /**
     * Append the record at the next consecutive offset. If no records have been appended yet, use the base
     * offset of this builder.
     *
     * @param record The record to add
     */
    public void append(LegacyRecord record) {
        appendWithOffset(nextSequentialOffset(), record);
    }

    private void appendDefaultRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) throws IOException {
        ensureOpenForRecordAppend();
        int  offsetDelta    = (int) (offset - baseOffset);
        long timestampDelta = timestamp - baseTimestamp;
        int  sizeInBytes    = DefaultRecord.writeTo(appendStream, offsetDelta, timestampDelta, key, value, headers);
        recordWritten(offset, timestamp, sizeInBytes);
    }

    private long appendLegacyRecord(long offset, long timestamp, ByteBuffer key, ByteBuffer value, byte magic) throws IOException {
        ensureOpenForRecordAppend();

        int size = LegacyRecord.recordSize(magic, key, value);
        AbstractLegacyRecordBatch.writeHeader(appendStream, toInnerOffset(offset), size);

        if (timestampType == TimestampType.LOG_APPEND_TIME) timestamp = logAppendTime;
        long crc = LegacyRecord.write(appendStream, magic, timestamp, key, value, CompressionType.NONE, timestampType);
        recordWritten(offset, timestamp, size + Records.LOG_OVERHEAD);
        return crc;
    }

    private long toInnerOffset(long offset) {
        // use relative offsets for compressed messages with magic v1
        if (magic > 0 && compression.type() != CompressionType.NONE) return offset - baseOffset;
        return offset;
    }

    private void recordWritten(long offset, long timestamp, int size) {
        if (numRecords == Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum number of records per batch exceeded, max records: " + Integer.MAX_VALUE);
        if (offset - baseOffset > Integer.MAX_VALUE)
            throw new IllegalArgumentException("Maximum offset delta exceeded, base offset: " + baseOffset + ", last offset: " + offset);

        numRecords += 1;
        uncompressedRecordsSizeInBytes += size;
        lastOffset = offset;

        if (magic > RecordBatch.MAGIC_VALUE_V0 && timestamp > maxTimestamp) {
            maxTimestamp         = timestamp;
            offsetOfMaxTimestamp = offset;
        }
    }

    private void ensureOpenForRecordAppend() {
        if (appendStream == CLOSED_STREAM)
            throw new IllegalStateException("Tried to append a record, but MemoryRecordsBuilder is closed for record appends");
    }

    private void ensureOpenForRecordBatchWrite() {
        if (isClosed())
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is closed");
        if (aborted)
            throw new IllegalStateException("Tried to write record batch header, but MemoryRecordsBuilder is aborted");
    }

    /**
     * Get an estimate of the number of bytes written (based on the estimation factor hard-coded in {@link CompressionType}).
     *
     * @return The estimated number of bytes written
     */
    private int estimatedBytesWritten() {
        if (compression.type() == CompressionType.NONE) {
            return batchHeaderSizeInBytes + uncompressedRecordsSizeInBytes;
        } else {
            // estimate the written bytes to the underlying byte buffer based on uncompressed written bytes
            return batchHeaderSizeInBytes + (int) (uncompressedRecordsSizeInBytes * estimatedCompressionRatio * COMPRESSION_RATE_ESTIMATION_FACTOR);
        }
    }

    /**
     * Set the estimated compression ratio for the memory records builder.
     */
    public void setEstimatedCompressionRatio(float estimatedCompressionRatio) {
        this.estimatedCompressionRatio = estimatedCompressionRatio;
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     * 检查我们是否有空间容纳包含给定键值对的新记录。如果未附加任何记录，则返回 true。
     */
    public boolean hasRoomFor(long timestamp, byte[] key, byte[] value, Header[] headers) {
        return hasRoomFor(timestamp, wrapNullable(key), wrapNullable(value), headers);
    }

    /**
     * Check if we have room for a new record containing the given key/value pair. If no records have been
     * appended, then this returns true.
     * <p>
     * Note that the return value is based on the estimate of the bytes written to the compressor, which may not be
     * accurate if compression is used. When this happens, the following append may cause dynamic buffer
     * re-allocation in the underlying byte buffer stream.
     */
    public boolean hasRoomFor(long timestamp, ByteBuffer key, ByteBuffer value, Header[] headers) {
        if (isFull()) // 判断批次是否已满            return false;

            // We always allow at least one record to be appended (the ByteBufferOutputStream will grow as needed)
            if (numRecords == 0) return true;

        final int recordSize;
        if (magic < RecordBatch.MAGIC_VALUE_V2) {
            recordSize = Records.LOG_OVERHEAD + LegacyRecord.recordSize(magic, key, value);
        } else {
            int  nextOffsetDelta = lastOffset == null ? 0 : (int) (lastOffset - baseOffset + 1);
            long timestampDelta  = baseTimestamp == null ? 0 : timestamp - baseTimestamp;
            recordSize = DefaultRecord.sizeInBytes(nextOffsetDelta, timestampDelta, key, value, headers);
        }

        // Be conservative and not take compression of the new record into consideration.
        return this.writeLimit >= estimatedBytesWritten() + recordSize;
    }

    /**
     * Check if we have room for a given number of bytes.
     */
    public boolean hasRoomFor(int estimatedRecordsSize) {
        if (isFull()) return false;
        return this.writeLimit >= estimatedBytesWritten() + estimatedRecordsSize;
    }

    public int maxAllowedBytes() {
        return this.writeLimit - this.batchHeaderSizeInBytes;
    }

    public boolean isClosed() {
        return builtRecords != null;
    }

    public boolean isFull() {
        // note that the write limit is respected only after the first record is added which ensures we can always
        // create non-empty batches (this is used to disable batching when the producer's batch size is set to 0).
        return appendStream == CLOSED_STREAM || (this.numRecords > 0 && this.writeLimit <= estimatedBytesWritten());
    }

    /**
     * Get an estimate of the number of bytes written to the underlying buffer. The returned value
     * is exactly correct if the record set is not compressed or if the builder has been closed.
     */
    public int estimatedSizeInBytes() {
        return builtRecords != null ? builtRecords.sizeInBytes() : estimatedBytesWritten();
    }

    public byte magic() {
        return magic;
    }

    private long nextSequentialOffset() {
        return lastOffset == null ? baseOffset : lastOffset + 1;
    }

    public static class RecordsInfo {
        public final long maxTimestamp;
        public final long shallowOffsetOfMaxTimestamp;

        public RecordsInfo(long maxTimestamp, long shallowOffsetOfMaxTimestamp) {
            this.maxTimestamp                = maxTimestamp;
            this.shallowOffsetOfMaxTimestamp = shallowOffsetOfMaxTimestamp;
        }
    }

    /**
     * Return the producer id of the RecordBatches created by this builder.
     */
    public long producerId() {
        return this.producerId;
    }

    public short producerEpoch() {
        return this.producerEpoch;
    }

    public int baseSequence() {
        return this.baseSequence;
    }
}