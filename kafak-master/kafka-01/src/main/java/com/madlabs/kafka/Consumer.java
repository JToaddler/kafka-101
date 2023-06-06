package com.madlabs.kafka;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	private static final Logger log = LoggerFactory.getLogger(ConsumerShutdownhook.class);

	public static void main(String[] args) {

		log.info("Test log");

		String groupId = "kaka-master-consumer-app";
		String topic = "demo_java";

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
		props.setProperty("value.deserializer", StringDeserializer.class.getName());
		props.setProperty("group.id", groupId);
		props.setProperty("auto.offset.reset", "earliest"); // none, latest

		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));
		while (true) {
			log.info("Polling for message");
			ConsumerRecords<String, String> cRecords = consumer.poll(Duration.ofSeconds(2));
			for (ConsumerRecord<String, String> cRecord : cRecords) {
				log.info("Mesage received");
				String key = cRecord.key();
				String value = cRecord.value();
				log.info("Key :" + key + ", Value :" + value + ", Offset: " + cRecord.offset() + ", Partition :"
						+ cRecord.partition());
			}
		}

	}
}
