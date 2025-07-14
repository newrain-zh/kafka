package kafka.examples.assignor;

import kafka.examples.KafkaProperties;
import kafka.examples.Utils;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;

/*
bin/kafka-consumer-groups.sh --describe \
  --group assignor-group \
  --bootstrap-server localhost:9092
 */
public class CooperativeStickyAssignorDemo {
    public static final String TOPIC_NAME = "cooperativeSticky-assignor-topic";
    public static final String GROUP_NAME = "cooperativeSticky-assignor-group";

    public static void main(String[] args) throws InterruptedException {
        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 4, TOPIC_NAME);
        CountDownLatch latch          = new CountDownLatch(2);
        Consumer       consumerThread = new Consumer("consumer-" + 0, KafkaProperties.BOOTSTRAP_SERVERS, List.of(TOPIC_NAME), GROUP_NAME, Optional.empty(), false, 1, latch, "client-" + 0);
        consumerThread.start();
        Thread.sleep(10000);
        Consumer consumerThread1 = new Consumer("consumer-" + 1, KafkaProperties.BOOTSTRAP_SERVERS, List.of(TOPIC_NAME), GROUP_NAME, Optional.empty(), false, 1, latch, "client-" + 1);
        consumerThread1.start();
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}