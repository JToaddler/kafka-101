package com.madlabs.kafka.test;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Consumer {

	private static final Logger log = LoggerFactory.getLogger(Consumer.class);

	public static void main(String[] args) throws InterruptedException {

		log.info("Test log");

		//String groupId = "kaka-standalone-consumer-app-odd";
		String groupId = "kaka-standalone-consumer-app";
		String topic = "standalone-test";

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.deserializer", StringDeserializer.class.getName());
		props.setProperty("value.deserializer", StringDeserializer.class.getName());
		props.setProperty("group.id", groupId);
		props.setProperty("auto.offset.reset", "latest"); // none, latest
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList(topic));

		boolean isRead = false;
		while (!isRead) {
			log.info("Polling for message");
			TimeUnit.SECONDS.sleep(5);
			ConsumerRecords<String, String> cRecords = consumer.poll(Duration.ofSeconds(2));
			log.info("No of records received : " + cRecords.count());
			for (ConsumerRecord<String, String> cRecord : cRecords) {
				log.info("Mesage received");
				String key = cRecord.key();
				String value = cRecord.value();
				log.info("Key :" + key + ", Value :" + value + ", Offset: " + cRecord.offset() + ", Partition :"
						+ cRecord.partition());

			}
			if (cRecords.count() > 0)
				isRead = true;
		}
		log.info("Committing");
		consumer.commitSync();
		consumer.close();
		log.info("Consumer shoutdown");
	}

}
