package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * 批量发送示例
 * bin/kafka-topics.sh --create \
 *     --bootstrap-server localhost:9092 \
 *     --topic compaction \
 *     --partitions 1 \
 *     --replication-factor 1
 */
public class BatchProducer {

    public static void main(String[] args) throws RuntimeException, ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "500");       // 等待 200ms 批量更多消息
        props.put("batch.size", "32768");    // 设置更大的批次大小
//        props.put("log.cleanup.policy", "compact");    // 设置更大的批次大小
//        props.put("compression.type", "gzip");  // 可选：压缩以验证 batch

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
       for (;;){
            List<Future<RecordMetadata>>  list     = new ArrayList<>();
            for (int i = 0; i < 100000; i++) {
//            RecordMetadata recordMetadata = producer.send(new ProducerRecord<>("test-batch", "key" + i, "value" + i)).get();
                producer.send(new ProducerRecord<>("test-compaction", "key" + i, "value" + i));
//            System.out.println(i + ": offset" + recordMetadata.offset());
            }
            Thread.sleep(10000);
        }

/*        try {
//            Thread.sleep(2000);
            for (Future<RecordMetadata> future : list) {
                RecordMetadata recordMetadata = future.get();
                System.out.println("topic:" + recordMetadata.topic() + " partition:" + recordMetadata.partition() + " offset:" + recordMetadata.offset());
            }
        } catch (InterruptedException | ExecutionException e) {
            throw new RuntimeException(e);
        }*/

//        producer.flush();
//        producer.close();
    }
}