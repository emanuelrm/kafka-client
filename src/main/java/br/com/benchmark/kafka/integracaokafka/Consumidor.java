package br.com.benchmark.kafka.integracaokafka;

import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Consumidor {
	
	public static void main(String[] args) {
		
	    Properties props = new Properties();
	     props.put("bootstrap.servers", "localhost:9092");
	     props.put("group.id", "test");
	     props.put("enable.auto.commit", "true");
	     props.put("auto.commit.interval.ms", "1000");
	     props.put("session.timeout.ms", "30000");
	     props.put("auto.offset.reset", "earliest");
	     props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
	     KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("newtopic"));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(1000);
	         for (ConsumerRecord<String, String> record : records) {
//	             System.out.printf("offset = %d, key = %s, value = %s", record.offset(), record.key(), record.value());
	        	 System.out.printf("Consumidor 1 :" + record.value());
	         	 System.out.print("\n");
	         }
	     }
	}

}
