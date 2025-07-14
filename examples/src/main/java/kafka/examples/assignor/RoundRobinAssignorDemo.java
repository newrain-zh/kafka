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
public class RoundRobinAssignorDemo {
    public static final String TOPIC_NAME = "RoundRobin-assignor-topic";
    public static final String GROUP_NAME = "RoundRobin-assignor-group";

    public static void main(String[] args) {
        String topicOne   = TOPIC_NAME + "-one";
        String topicTwo   = TOPIC_NAME + "-two";
        String topicThree = TOPIC_NAME + "-three";
        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 1, topicOne);
        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 2, topicTwo);
        Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 3, topicThree);

        CountDownLatch latch = new CountDownLatch(1);

        Producer producerThread1 = new Producer("producer1", KafkaProperties.BOOTSTRAP_SERVERS, topicOne, true, null, false, 1000, -1, latch);
        producerThread1.start();

        Producer producerThread2 = new Producer("producer1", KafkaProperties.BOOTSTRAP_SERVERS, topicTwo, true, null, false, 1000, -1, latch);
        producerThread2.start();

        Producer producerThread3 = new Producer("producer1", KafkaProperties.BOOTSTRAP_SERVERS, topicThree, true, null, false, 1000, -1, latch);
        producerThread3.start();

        for (int i = 0; i < 3; i++) {
            if (i == 0) {
                Consumer consumerThread = new Consumer("consumer-" + i, KafkaProperties.BOOTSTRAP_SERVERS, List.of(topicOne), GROUP_NAME, Optional.empty(), false, 1, latch, "client-" + i);
                consumerThread.start();
            } else if (i == 1) {
                Consumer consumerThread = new Consumer("consumer-" + i, KafkaProperties.BOOTSTRAP_SERVERS, List.of(topicOne, topicTwo), GROUP_NAME, Optional.empty(), false, 1, latch, "client-" + i);
                consumerThread.start();
            } else {
                Consumer consumerThread = new Consumer("consumer-" + i, KafkaProperties.BOOTSTRAP_SERVERS, List.of(topicOne, topicTwo, topicThree), GROUP_NAME, Optional.empty(), false, 1, latch, "client-" + i);
                consumerThread.start();
            }

        }
        try {
            Thread.sleep(50000);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}