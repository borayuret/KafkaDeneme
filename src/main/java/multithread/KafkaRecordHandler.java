package multithread;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class KafkaRecordHandler implements Runnable {
    private ConsumerRecord<String, String> record;

    public KafkaRecordHandler(ConsumerRecord<String, String> record) {
        this.record = record;
    }

    @Override
    public void run() { // this is where further processing happens
        System.out.println("value = "+record.value() +" Thread id = "+ Thread.currentThread().getId());
    }
}
