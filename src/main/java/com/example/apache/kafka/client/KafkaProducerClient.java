package com.example.apache.kafka.client;

import java.util.Date;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class KafkaProducerClient {

	public static void main(String[] args) throws Exception {

		String topicName = "My_1st_Replicated_Kafka_Topic";

		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		Producer<String, String> producer = new KafkaProducer<String, String>(props);
		producer.send(new ProducerRecord<String, String>(topicName, "start-" + Math.random(), "message starts" + new Date().toString()));

		for (int i = 0; i < 10; i++)
			producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(i), Integer.toString( i+100)));
		
		producer.send(new ProducerRecord<String, String>(topicName, "end-" + Math.random(), "message starts" + new Date().toString()));
		System.out.println("Message sent successfully");
		
		producer.close();
	}
}