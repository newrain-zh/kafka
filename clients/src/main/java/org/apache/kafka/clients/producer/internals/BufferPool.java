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
package org.apache.kafka.clients.producer.internals;

import org.apache.kafka.clients.producer.BufferExhaustedException;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.metrics.Metrics;
import org.apache.kafka.common.metrics.Sensor;
import org.apache.kafka.common.metrics.stats.Meter;
import org.apache.kafka.common.utils.Time;

import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;


/**
 * A pool of ByteBuffers kept under a given memory limit. This class is fairly specific to the needs of the producer. In
 * particular it has the following properties:
 * <ol>
 * <li>There is a special "poolable size" and buffers of this size are kept in a free list and recycled
 * <li>It is fair. That is all memory is given to the longest waiting thread until it has sufficient memory. This
 * prevents starvation or deadlock when a thread asks for a large chunk of memory and needs to block until multiple
 * buffers are deallocated.
 * </ol>
 */
public class BufferPool {

    static final String WAIT_TIME_SENSOR_NAME = "bufferpool-wait-time";

    private final long              totalMemory; // 内存池总容量，由 buffer.memory 参数设置，控制生成生产者的最大内存
    private final int               poolableSize; // 缓冲池中可复用缓冲区的大小，batch.size 参数设置，此大小的 ByteBuffer 缓冲区大小会被缓冲复用
    private final ReentrantLock     lock;
    private final Deque<ByteBuffer> free; // 缓冲池中可复用缓冲区队列
    private final Deque<Condition>  waiters; //  等待获取缓冲区的线程队列
    /**
     * Total available memory is the sum of nonPooledAvailableMemory and the number of byte buffers in free * poolableSize.
     */
    private       long              nonPooledAvailableMemory; // 非池化可用内存，用于存储超过 poolableSize 的临时分配内存（释放后直接归还至此，不缓存）
    private final Metrics           metrics; // 监控指标（如等待时间、内存使用率），用于性能分析和调优
    private final Time              time; // 时间工具类
    private final Sensor            waitTime; // 记录线程再内存分配时的阻塞时间
    private       boolean           closed; // 标记内存池是否已经关闭（关闭后拒绝新分配，释放所有资源）

    /**
     * Create a new buffer pool
     *
     * @param memory        The maximum amount of memory that this buffer pool can allocate
     * @param poolableSize  The buffer size to cache in the free list rather than deallocating
     * @param metrics       instance of Metrics
     * @param time          time instance
     * @param metricGrpName logical group name for metrics
     */
    public BufferPool(long memory, int poolableSize, Metrics metrics, Time time, String metricGrpName) {
        this.poolableSize             = poolableSize;
        this.lock                     = new ReentrantLock();
        this.free                     = new ArrayDeque<>();
        this.waiters                  = new ArrayDeque<>();
        this.totalMemory              = memory;
        this.nonPooledAvailableMemory = memory;
        this.metrics                  = metrics;
        this.time                     = time;
        this.waitTime                 = this.metrics.sensor(WAIT_TIME_SENSOR_NAME);
        MetricName rateMetricName = metrics.metricName("bufferpool-wait-ratio",
                metricGrpName,
                "The fraction of time an appender waits for space allocation.");
        MetricName totalNsMetricName = metrics.metricName("bufferpool-wait-time-ns-total",
                metricGrpName,
                "The total time in nanoseconds an appender waits for space allocation.");

        Sensor     bufferExhaustedRecordSensor    = metrics.sensor("buffer-exhausted-records");
        MetricName bufferExhaustedRateMetricName  = metrics.metricName("buffer-exhausted-rate", metricGrpName, "The average per-second number of record sends that are dropped due to buffer exhaustion");
        MetricName bufferExhaustedTotalMetricName = metrics.metricName("buffer-exhausted-total", metricGrpName, "The total number of record sends that are dropped due to buffer exhaustion");
        bufferExhaustedRecordSensor.add(new Meter(bufferExhaustedRateMetricName, bufferExhaustedTotalMetricName));

        this.waitTime.add(new Meter(TimeUnit.NANOSECONDS, rateMetricName, totalNsMetricName));
        this.closed = false;
    }

    /**
     * Allocate a buffer of the given size. This method blocks if there is not enough memory and the buffer pool
     * is configured with blocking mode.
     *
     * @param size             The buffer size to allocate in bytes
     * @param maxTimeToBlockMs The maximum time in milliseconds to block for buffer memory to be available
     * @return The buffer
     * @throws InterruptedException     If the thread is interrupted while blocked
     * @throws IllegalArgumentException if size is larger than the total memory controlled by the pool (and hence we would block
     *                                  forever)
     */
    public ByteBuffer allocate(int size, long maxTimeToBlockMs) throws InterruptedException {
        if (size > this.totalMemory)
            throw new IllegalArgumentException("Attempt to allocate " + size
                    + " bytes, but there is a hard limit of "
                    + this.totalMemory
                    + " on memory allocations.");

        ByteBuffer buffer = null;
        this.lock.lock();

        if (this.closed) {
            this.lock.unlock();
            throw new KafkaException("Producer closed while allocating memory");
        }

        try {
            // check if we have a free buffer of the right size pooled
            if (size == poolableSize && !this.free.isEmpty())
                return this.free.pollFirst();

            // now check if the request is immediately satisfiable with the
            // memory on hand or if we need to block
            int freeListSize = freeSize() * this.poolableSize;
            if (this.nonPooledAvailableMemory + freeListSize >= size) {
                // we have enough unallocated or pooled memory to immediately
                // satisfy the request, but need to allocate the buffer
                // 我们有足够的未分配或汇总的内存以立即满足请求，但需要分配缓冲区
                freeUp(size);
                this.nonPooledAvailableMemory -= size;
            } else {
                // we are out of memory and will have to block
                int       accumulated = 0;
                Condition moreMemory  = this.lock.newCondition();
                try {
                    long remainingTimeToBlockNs = TimeUnit.MILLISECONDS.toNanos(maxTimeToBlockMs);
                    this.waiters.addLast(moreMemory);
                    // loop over and over until we have a buffer or have reserved
                    // enough memory to allocate one
                    while (accumulated < size) {
                        long    startWaitNs = time.nanoseconds();
                        long    timeNs;
                        boolean waitingTimeElapsed;
                        try {
                            waitingTimeElapsed = !moreMemory.await(remainingTimeToBlockNs, TimeUnit.NANOSECONDS);
                        } finally {
                            long endWaitNs = time.nanoseconds();
                            timeNs = Math.max(0L, endWaitNs - startWaitNs);
                            recordWaitTime(timeNs);
                        }

                        if (this.closed)
                            throw new KafkaException("Producer closed while allocating memory");

                        if (waitingTimeElapsed) {
                            this.metrics.sensor("buffer-exhausted-records").record();
                            throw new BufferExhaustedException("Failed to allocate " + size + " bytes within the configured max blocking time "
                                    + maxTimeToBlockMs + " ms. Total memory: " + totalMemory() + " bytes. Available memory: " + availableMemory()
                                    + " bytes. Poolable size: " + poolableSize() + " bytes");
                        }

                        remainingTimeToBlockNs -= timeNs;

                        // check if we can satisfy this request from the free list,
                        // otherwise allocate memory
                        if (accumulated == 0 && size == this.poolableSize && !this.free.isEmpty()) {
                            // just grab a buffer from the free list
                            buffer      = this.free.pollFirst();
                            accumulated = size;
                        } else {
                            // we'll need to allocate memory, but we may only get
                            // part of what we need on this iteration
                            freeUp(size - accumulated);
                            int got = (int) Math.min(size - accumulated, this.nonPooledAvailableMemory);
                            this.nonPooledAvailableMemory -= got;
                            accumulated += got;
                        }
                    }
                    // Don't reclaim memory on throwable since nothing was thrown
                    accumulated = 0;
                } finally {
                    // When this loop was not able to successfully terminate don't loose available memory
                    this.nonPooledAvailableMemory += accumulated;
                    this.waiters.remove(moreMemory);
                }
            }
        } finally {
            // signal any additional waiters if there is more memory left
            // over for them
            try {
                if (!(this.nonPooledAvailableMemory == 0 && this.free.isEmpty()) && !this.waiters.isEmpty())
                    this.waiters.peekFirst().signal();
            } finally {
                // Another finally... otherwise find bugs complains
                lock.unlock();
            }
        }

        if (buffer == null)
            return safeAllocateByteBuffer(size);
        else
            return buffer;
    }

    // Protected for testing
    protected void recordWaitTime(long timeNs) {
        this.waitTime.record(timeNs, time.milliseconds());
    }

    /**
     * Allocate a buffer.  If buffer allocation fails (e.g. because of OOM) then return the size count back to
     * available memory and signal the next waiter if it exists.
     */
    private ByteBuffer safeAllocateByteBuffer(int size) {
        boolean error = true;
        try {
            ByteBuffer buffer = allocateByteBuffer(size);
            error = false;
            return buffer;
        } finally {
            if (error) {
                this.lock.lock();
                try {
                    this.nonPooledAvailableMemory += size;
                    if (!this.waiters.isEmpty())
                        this.waiters.peekFirst().signal();
                } finally {
                    this.lock.unlock();
                }
            }
        }
    }

    // Protected for testing.
    protected ByteBuffer allocateByteBuffer(int size) {
        return ByteBuffer.allocate(size);
    }

    /**
     * Attempt to ensure we have at least the requested number of bytes of memory for allocation by deallocating pooled
     * buffers (if needed)
     */
    private void freeUp(int size) {
        while (!this.free.isEmpty() && this.nonPooledAvailableMemory < size)
            this.nonPooledAvailableMemory += this.free.pollLast().capacity();
    }

    /**
     * Return buffers to the pool. If they are of the poolable size add them to the free list, otherwise just mark the
     * memory as free.
     *
     * @param buffer The buffer to return
     * @param size   The size of the buffer to mark as deallocated, note that this may be smaller than buffer.capacity
     *               since the buffer may re-allocate itself during in-place compression
     */
    public void deallocate(ByteBuffer buffer, int size) {
        lock.lock();
        try {
            if (size == this.poolableSize && size == buffer.capacity()) {
                buffer.clear();
                this.free.add(buffer);
            } else {
                this.nonPooledAvailableMemory += size;
            }
            Condition moreMem = this.waiters.peekFirst();
            if (moreMem != null)
                moreMem.signal();
        } finally {
            lock.unlock();
        }
    }

    public void deallocate(ByteBuffer buffer) {
        if (buffer != null)
            deallocate(buffer, buffer.capacity());
    }

    /**
     * the total free memory both unallocated and in the free list
     */
    public long availableMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory + freeSize() * (long) this.poolableSize;
        } finally {
            lock.unlock();
        }
    }

    // Protected for testing.
    protected int freeSize() {
        return this.free.size();
    }

    /**
     * Get the unallocated memory (not in the free list or in use)
     */
    public long unallocatedMemory() {
        lock.lock();
        try {
            return this.nonPooledAvailableMemory;
        } finally {
            lock.unlock();
        }
    }

    /**
     * The number of threads blocked waiting on memory
     */
    public int queued() {
        lock.lock();
        try {
            return this.waiters.size();
        } finally {
            lock.unlock();
        }
    }

    /**
     * The buffer size that will be retained in the free list after use
     */
    public int poolableSize() {
        return this.poolableSize;
    }

    /**
     * The total memory managed by this pool
     */
    public long totalMemory() {
        return this.totalMemory;
    }

    // package-private method used only for testing
    Deque<Condition> waiters() {
        return this.waiters;
    }

    /**
     * Closes the buffer pool. Memory will be prevented from being allocated, but may be deallocated. All allocations
     * awaiting available memory will be notified to abort.
     */
    public void close() {
        this.lock.lock();
        this.closed = true;
        try {
            for (Condition waiter : this.waiters)
                waiter.signal();
        } finally {
            this.lock.unlock();
        }
    }
}