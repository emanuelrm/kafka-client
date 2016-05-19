package br.com.benchmark.kafka.integracaokafka;

public class ConsumerDemo {

	public static void main(String[] args) {

//		KafkaConsumerRunner k = new KafkaConsumerRunner();
//		k.run();
		
		ConsumerThread t = new ConsumerThread("newtopic", "t");
		t.run();
	}
}