package multithread;

public class KafkaMultiTest {

    public static void main(String[] args) {
        KafkaMultiConsumer processor = new KafkaMultiConsumer();
        try {
            processor.init(3);
        }catch (Exception exp) {
            processor.shutdown();
        }
    }
}

