package com.madlabs.kafka;

import java.util.Properties;
import java.util.Random;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProducerDemo {

	private static final Logger log = LoggerFactory.getLogger(ProducerDemo.class);

	public static void main(String[] args) {
		log.info("Test log");
		
		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", StringSerializer.class.getName());
		props.setProperty("batch.size", "400");
		//props.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);
		ProducerRecord<String, String> pRecord = new ProducerRecord<>("demo_java", "Sample Message");

		for (int i = 0; i < 10; i++) {
			send(producer, i);
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		producer.flush();
		producer.close();

	}

	public static void send(KafkaProducer<String, String> producer, int id) {

		String topic = "demo_java";

		for (int i = 0; i < 30; i++) {

			String key = "id_" + id;
			String value = "Sample Message - " + new Random().nextInt();

			ProducerRecord<String, String> pRecord = new ProducerRecord<>(topic, key, value);
			producer.send(pRecord, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exc) {
					if (exc == null) {
						log.info("Received new metadata :" + " Topic : " + metadata.topic() + "Key :" + key
								+ " Partition :" + metadata.partition() + " Offset : " + metadata.offset()
								+ " Timestamp :" + metadata.timestamp());
					}
				}
			});
		}

	}

}
