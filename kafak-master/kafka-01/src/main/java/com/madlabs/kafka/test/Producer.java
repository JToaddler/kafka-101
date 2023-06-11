package com.madlabs.kafka.test;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
	private static final Logger log = LoggerFactory.getLogger(Producer.class);

	private static final String topic = "standalone-test";

	public static void main(String[] args) throws InterruptedException {

		log.info("Test log");

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "localhost:9092");
		props.setProperty("key.serializer", StringSerializer.class.getName());
		props.setProperty("value.serializer", StringSerializer.class.getName());
		props.setProperty("batch.size", "400");
		// props.setProperty("partitioner.class",
		// RoundRobinPartitioner.class.getName());

		KafkaProducer<String, String> producer = new KafkaProducer<>(props);

		for (int i = 10; i < 20; i++) {

			String key = "id_" + i;
			String value = "Sample Message - " + i;

			ProducerRecord<String, String> pRecord = new ProducerRecord<>(topic, key, value);
			producer.send(pRecord, new Callback() {

				@Override
				public void onCompletion(RecordMetadata metadata, Exception exc) {
					if (exc == null) {
						log.info("Received ACK metadata :" + " Topic : " + metadata.topic() + "Key :" + key
								+ " Partition :" + metadata.partition() + " Offset : " + metadata.offset()
								+ " Timestamp :" + metadata.timestamp());
					}
				}
			});
		}
		TimeUnit.SECONDS.sleep(3);
	}

}
