package kafka.examples.assignor;

import kafka.examples.KafkaProperties;
import kafka.examples.Producer;
import kafka.examples.Utils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
/*
bin/kafka-consumer-groups.sh --describe \
  --group assignor-group \
  --bootstrap-server localhost:9092
 */
public class RangeAssignorDemo {
    public static final String TOPIC_NAME = "assignor-topic";
    public static final String GROUP_NAME = "assignor-group";

    public static void main(String[] args) {

        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 7, TOPIC_NAME);
//        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 7, TOPIC_NAME);

        CountDownLatch latch = new CountDownLatch(1);

        Producer producerThread1 = new Producer("producer1", KafkaProperties.BOOTSTRAP_SERVERS, TOPIC_NAME, true, null, false, 1000, -1, latch);
        producerThread1.start();

        for (int i = 0; i < 3; i++) {
            Consumer consumerThread = new Consumer("consumer-" + i, KafkaProperties.BOOTSTRAP_SERVERS, List.of(TOPIC_NAME), GROUP_NAME, Optional.empty(), false, 1, latch, "client-" + i);
            consumerThread.start();
        }
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}