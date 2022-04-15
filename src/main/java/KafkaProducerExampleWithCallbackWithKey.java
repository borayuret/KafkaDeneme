import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerExampleWithCallbackWithKey {

    public static void main(String[] args) throws ExecutionException, InterruptedException {

        String bootstrap_servers = "localhost:9092,localhost:9093,localhost:9094";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, "10");
        properties.setProperty(ProducerConfig.RETRY_BACKOFF_MS_CONFIG, "3000");


        KafkaProducer<String,String> first_producer = new KafkaProducer<>(properties);

        for (int i = 0; i < 1; i++) {



                ProducerRecord<String, String> record = new ProducerRecord<>("test-topic-replicated", "A", "Ulus"+i);

                first_producer.send(record, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {

                        Logger logger = LoggerFactory.getLogger(KafkaProducerExampleWithCallbackWithKey.class);

                        if(e == null){
                            logger.info("Topic:" + recordMetadata.topic());
                            logger.info("Partition:" + recordMetadata.partition());
                            logger.info("Offset:" + recordMetadata.offset());
                            logger.info("Timestamp:" + recordMetadata.timestamp());
                        }
                        else{
                            logger.error("Hata:" + e.getMessage());
                        }


                    }
                }).get();



        }

        // async olduğu için flush ve close yapacağız.
        first_producer.flush();
        first_producer.close();

    }

}
