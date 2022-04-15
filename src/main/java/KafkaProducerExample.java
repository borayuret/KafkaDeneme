import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducerExample {

    public static void main(String[] args) {

        String bootstrap_servers = "localhost:9092,localhost:9093,localhost:9094";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");




        KafkaProducer<String,String> first_producer = new KafkaProducer<>(properties);

        ProducerRecord<String, String> record = new ProducerRecord<>("test-topic-replicated"," Async Mesaj 2");

        // Sending data
        first_producer.send(record);

        // async olduğu için flush ve close yapacağız.
        first_producer.flush();
        first_producer.close();

    }

}
