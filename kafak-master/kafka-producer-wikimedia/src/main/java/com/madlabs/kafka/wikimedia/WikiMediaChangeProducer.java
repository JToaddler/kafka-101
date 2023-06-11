package com.madlabs.kafka.wikimedia;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

public class WikiMediaChangeProducer {

	private static final Logger log = LoggerFactory.getLogger(WikiMediaChangeProducer.class);

	final static String bootstrapServer = "localhost:9092";
	final static String topic = "wikimedia.recentchanges";
	final static String URL = "https://stream.wikimedia.org/v2/stream/recentchange";

	public static void main(String[] args) throws InterruptedException {

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServer);
		props.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		props.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		// set high throughput settings
		props.setProperty(ProducerConfig.LINGER_MS_CONFIG, "500");
		props.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024));
		props.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");

		KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);

		EventHandler eventHandler = new WikiMediaChangeHandler(producer, topic);
		EventSource.Builder builder = new EventSource.Builder(eventHandler, URI.create(URL));

		EventSource eventSource = builder.build();
		eventSource.start();
		TimeUnit.SECONDS.sleep(10);
		log.info("Main thread execution completed");
	}
}
