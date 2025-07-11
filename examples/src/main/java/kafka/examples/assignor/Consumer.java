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
package kafka.examples.assignor;

import kafka.examples.Utils;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.AuthorizationException;
import org.apache.kafka.common.errors.RecordDeserializationException;
import org.apache.kafka.common.errors.UnsupportedVersionException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static java.util.Collections.singleton;

/**
 * A simple consumer thread that subscribes to a topic, fetches new records and prints them.
 * The thread does not stop until all records are completed or an exception is raised.
 */
public class Consumer extends Thread implements ConsumerRebalanceListener {
    private final    String           bootstrapServers;
    private final    List<String>     topic;
    private final    String           groupId;
    private final    Optional<String> instanceId;
    private final    boolean          readCommitted;
    private final    int              numRecords;
    private final    CountDownLatch   latch;
    private volatile boolean          closed;
    private          int              remainingRecords;
    private final    String           clientId;

    public Consumer(String threadName, String bootstrapServers, List<String> topic, String groupId, Optional<String> instanceId, boolean readCommitted, int numRecords, CountDownLatch latch, String clientId) {
        super(threadName);
        this.bootstrapServers = bootstrapServers;
        this.topic            = topic;
        this.groupId          = groupId;
        this.instanceId       = instanceId;
        this.readCommitted    = readCommitted;
        this.numRecords       = numRecords;
        this.remainingRecords = numRecords;
        this.latch            = latch;
        this.clientId         = clientId;
    }

    @Override
    public void run() {
        // the consumer instance is NOT thread safe
        try (KafkaConsumer<Integer, String> consumer = createKafkaConsumer()) {
            // subscribes to a list of topics to get dynamically assigned partitions
            // this class implements the rebalance listener that we pass here to be notified of such events
            consumer.subscribe(topic, this);
            Utils.printOut("Subscribed to %s", topic);
            while (!closed) {
                try {
                    // if required, poll updates partition assignment and invokes the configured rebalance listener
                    // then tries to fetch records sequentially using the last committed offset or auto.offset.reset policy
                    // returns immediately if there are records or times out returning an empty record set
                    // the next poll must be called within session.timeout.ms to avoid group rebalance
                    // 如果需要，轮询会更新分区分配并调用配置的再平衡侦听器，然后尝试使用上次提交的偏移量按顺序获取记录，
                    // 否则 auto.offset.reset 策略会立即返回，如果有记录或超时返回空记录集，则必须在 session.timeout.ms 内调用下一次轮询以避免组再平衡
                    ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
                    for (ConsumerRecord<Integer, String> record : records) {
                        Utils.maybePrintRecord(numRecords, record);
                    }
                } catch (AuthorizationException | UnsupportedVersionException | RecordDeserializationException e) {
                    // we can't recover from these exceptions
                    Utils.printErr(e.getMessage());
                    shutdown();
                } catch (OffsetOutOfRangeException | NoOffsetForPartitionException e) {
                    // invalid or no offset found without auto.reset.policy
                    Utils.printOut("Invalid or no offset found, using latest");
                    consumer.seekToEnd(e.partitions());
                    consumer.commitSync();
                } catch (KafkaException e) {
                    // log the exception and try to continue
                    Utils.printErr(e.getMessage());
                }
            }
        } catch (Throwable e) {
            Utils.printErr("Unhandled exception");
            e.printStackTrace();
        }
        System.out.println("消费消息");
        Utils.printOut("Fetched %d records", numRecords - remainingRecords);
        shutdown();
    }

    public void shutdown() {
        if (!closed) {
            closed = true;
            latch.countDown();
        }
    }

    public KafkaConsumer<Integer, String> createKafkaConsumer() {
        Properties props = new Properties();
        // bootstrap server config is required for consumer to connect to brokers
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        // client id is not required, but it's good to track the source of requests beyond just ip/port
        // by allowing a logical application name to be included in server-side request logging
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        // consumer group id is required when we use subscribe(topics) for group management
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        // sets static membership to improve availability (e.g. rolling restart)
        instanceId.ifPresent(id -> props.put(ConsumerConfig.GROUP_INSTANCE_ID_CONFIG, id));
        // disables auto commit when EOS is enabled, because offsets are committed with the transaction
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, readCommitted ? "false" : "true");
        // key and value are just byte arrays, so we need to set appropriate deserializers
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        if (readCommitted) {
            // skips ongoing and aborted transactions
            props.put(ConsumerConfig.ISOLATION_LEVEL_CONFIG, "read_committed");
        }
        // sets the reset offset policy in case of invalid or no offset
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return new KafkaConsumer<>(props);
    }

    @Override
    public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
        Utils.printOut("Revoked partitions: %s", partitions);
        // this can be used to commit pending offsets when using manual commit and EOS is disabled
    }

    @Override
    public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
        Utils.printOut("Assigned partitions: %s", partitions);
        // this can be used to read the offsets from an external store or some other initialization
    }

    @Override
    public void onPartitionsLost(Collection<TopicPartition> partitions) {
        Utils.printOut("Lost partitions: %s", partitions);
        // this is called when partitions are reassigned before we had a chance to revoke them gracefully
        // we can't commit pending offsets because these partitions are probably owned by other consumers already
        // nevertheless, we may need to do some other cleanup
    }
}