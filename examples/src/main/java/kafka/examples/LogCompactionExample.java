package kafka.examples;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class LogCompactionExample {


    public static void main(String[] args) throws RuntimeException, ExecutionException, InterruptedException {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("linger.ms", "1");       // 等待 200ms 批量更多消息
        props.put("batch.size", "32768");    // 设置更大的批次大小
//        props.put("compression.type", "gzip");  // 可选：压缩以验证 batch

        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        RecordMetadata                recordMetadata1 = producer.send(new ProducerRecord<>("log-compaction", "value 1")).get();
        RecordMetadata                recordMetadata2 = producer.send(new ProducerRecord<>("log-compaction", "hasKEY","value 1")).get();
        System.out.println(recordMetadata1);

    }
}