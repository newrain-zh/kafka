package kafka.examples;

public class CreateTopicDemo {

    public static final String TOPIC_NAME = "create-topic";
    public static final String GROUP_NAME = "my-group";

    public static void main(String[] args) {
        try {
            // stage 1: clean any topics left from previous runs
            Utils.recreateTopics(KafkaProperties.BOOTSTRAP_SERVERS, 3, TOPIC_NAME);
        } catch (Throwable e) {
            e.printStackTrace();
        }
    }
}