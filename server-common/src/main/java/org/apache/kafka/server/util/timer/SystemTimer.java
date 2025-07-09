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
package org.apache.kafka.server.util.timer;

import org.apache.kafka.common.utils.KafkaThread;
import org.apache.kafka.common.utils.ThreadUtils;
import org.apache.kafka.common.utils.Time;

import java.util.concurrent.DelayQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class SystemTimer implements Timer {
    public static final String SYSTEM_TIMER_THREAD_PREFIX = "executor-";

    // timeout timer
    private final ExecutorService           taskExecutor;
    private final DelayQueue<TimerTaskList> delayQueue;
    private final AtomicInteger             taskCounter;
    private final TimingWheel               timingWheel;

    // Locks used to protect data structures while ticking
    // 用于保护数据结构的锁
    private final ReentrantReadWriteLock           readWriteLock = new ReentrantReadWriteLock();
    private final ReentrantReadWriteLock.ReadLock  readLock      = readWriteLock.readLock();
    private final ReentrantReadWriteLock.WriteLock writeLock     = readWriteLock.writeLock();

    public SystemTimer(String executorName) {
        this(executorName, 1, 20, Time.SYSTEM.hiResClockMs());
    }

    public SystemTimer(String executorName, long tickMs, int wheelSize, long startMs) {
        this.taskExecutor = Executors.newFixedThreadPool(1, runnable -> KafkaThread.nonDaemon(SYSTEM_TIMER_THREAD_PREFIX + executorName, runnable));
        this.delayQueue   = new DelayQueue<>();
        this.taskCounter  = new AtomicInteger(0);
        // startMs = Time.SYSTEM.hiResClockMs()
        this.timingWheel  = new TimingWheel(tickMs, wheelSize, startMs, taskCounter, delayQueue);
    }

    // 添加任务
    public void add(TimerTask timerTask) {
        readLock.lock();
        try {
            addTimerTaskEntry(new TimerTaskEntry(timerTask, timerTask.delayMs + Time.SYSTEM.hiResClockMs()));
        } finally {
            readLock.unlock();
        }
    }

    private void addTimerTaskEntry(TimerTaskEntry timerTaskEntry) {
        if (!timingWheel.add(timerTaskEntry)) {
            // Already expired or cancelled
            // 已经过期或取消
            if (!timerTaskEntry.cancelled()) { // 如果任务没有取消
                taskExecutor.submit(timerTaskEntry.timerTask); // 执行任务
            }
        }
    }

    /**
     * Advances the clock if there is an expired bucket. If there isn't any expired bucket when called,
     * waits up to timeoutMs before giving up.
     */
    // SystemTimer
    // 推进时间指针，通过 Timer（定时器）
    public boolean advanceClock(long timeoutMs) throws InterruptedException {
        TimerTaskList bucket = delayQueue.poll(timeoutMs, TimeUnit.MILLISECONDS); // 获取到期的bucket
        if (bucket != null) { // 如果 bucket 不为空
            writeLock.lock();
            try {
                while (bucket != null) { // 循环处理bucket
                    timingWheel.advanceClock(bucket.getExpiration()); // 推进时间指针
                    // 处理 buket中的任务 这里会将做降层处理
                    // 这里将添加任务、提交任务的逻辑都在此处。
                    // addTimerTaskEntry 中判断了任务是否过期，如果任务过期且未取消，则执行任务。
                    // 新任务添加、到期任务重新分配（降层处理）、立即执行任务
                    bucket.flush(this::addTimerTaskEntry);
                    // 获取下一个bucket
                    bucket = delayQueue.poll();
                }
            } finally {
                writeLock.unlock();
            }
            return true;
        } else {
            return false;
        }
    }

    public int size() {
        return taskCounter.get();
    }

    @Override
    public void close() {
        ThreadUtils.shutdownExecutorServiceQuietly(taskExecutor, 5, TimeUnit.SECONDS);
    }

    // visible for testing
    boolean isTerminated() {
        return taskExecutor.isTerminated();
    }
}