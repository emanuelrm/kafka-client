package br.com.benchmark.kafka.integracaokafka;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Produtor {
	
	public static void main(String[] args) {
		
		 Properties props = new Properties();
		 props.put("bootstrap.servers", "localhost:9092");
		 props.put("acks", "all");
		 props.put("retries", 0);
		 props.put("batch.size", 16384);
		 props.put("auto.commit.interval.ms", 1000);
		 props.put("linger.ms", 1);
		 props.put("buffer.memory", 33554432);
		 props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 props.put("block.on.buffer.full", "true");
		 
		 Producer<String, String> producer = new KafkaProducer<>(props);
		 for(int i = 1; i < 10; i++) {
		     producer.send(new ProducerRecord<String, String>("newtopic", "prod - " + Integer.toString(i)));
		 }

//		 producer.send(new ProducerRecord<String, String>("newtopic", "newmsg2"));
		 System.out.println("Fim");
		 producer.close();
	}

}
