package br.com.benchmark.kafka.integracaokafka;

import java.util.Collections;
import java.util.Iterator;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerThread  implements Runnable {

private static final String KAFKA_BROKER = "localhost:9092";

private final KafkaConsumer<String, String> consumer;

    public ConsumerThread(String topic, String name) {
        Properties props = new Properties();
        props.put("bootstrap.servers", ConsumerThread.KAFKA_BROKER);
        props.put("group.id", "NewConsumer");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms", "6000");
        props.put("session.timeout.ms", "30000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        this.consumer = new KafkaConsumer(props);
        this.consumer.subscribe(Collections.singletonList(topic));
    }


    public void run() {
        try {
            boolean isRunning = true;
            while (isRunning) {
                ConsumerRecords<String,String> records= consumer.poll(10L);
//                System.out.println("Partition Assignment to this Consumer: "+consumer.assignment());
                Iterator it = records.iterator();
                while(it.hasNext()) {
                    ConsumerRecord record = (ConsumerRecord)it.next();
                    System.out.println("Received message2 from thread : "+Thread.currentThread().getName()+"(" + record.key() + ", " + (String)record.value() + ") at offset " + record.offset());
                }
            }
            consumer.close();
        }
        catch (Exception e) {
            e.printStackTrace();
        }
    }
}