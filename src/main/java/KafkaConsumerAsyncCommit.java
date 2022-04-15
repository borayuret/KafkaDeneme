import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Arrays;
import java.util.Map;
import java.util.Properties;

public class KafkaConsumerAsyncCommit {

    public static void main(String[] args)
            //throws InterruptedException
    {

        String bootstrap_servers = "localhost:9092,localhost:9093,localhost:9094";
        String groupId = "group1";
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap_servers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        //properties.setProperty(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, "5000");
        //properties.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "15000");
        properties.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");




        KafkaConsumer<String,String> consumer = new KafkaConsumer<>(properties);

        consumer.subscribe(Arrays.asList("test-topic-replicated"));

        while(true){
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            //Thread.sleep(6000);

            for (ConsumerRecord<String, String> record: records) {

                System.out.println("Key:" + record.key());
                System.out.println("Value:" + record.value());
                System.out.println("Partition:" + record.partition());
                System.out.println("Offset:" + record.offset());
                System.out.println("Timestamp:" + record.timestamp());

                System.out.println("****************************************************************************");
            }


            if(records.count() > 0){
                consumer.commitAsync((offsets, exception) -> {

                    if (exception != null){

                        System.err.println("Async hatası oluştu:" + exception.getMessage());
                    }
                    else
                    {
                        System.out.println("%%%%%%%%%%%%%%%%%% OFFSET COMMIT %%%%%%%%%%%%%%%%%%%%%%%%%%%%");
                    }

                });

            }



        }



    }

}
