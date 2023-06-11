package com.madlabs.kafka.wikimedia;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

public class WikiMediaChangeHandler implements EventHandler {

	private static final Logger log = LoggerFactory.getLogger(WikiMediaChangeHandler.class);

	KafkaProducer<String, String> kafkaProducer;
	String topic;

	public WikiMediaChangeHandler(KafkaProducer<String, String> producer, String topic) {
		this.kafkaProducer = producer;
		this.topic = topic;
	}

	@Override
	public void onOpen() throws Exception {

	}

	@Override
	public void onClosed() throws Exception {
		kafkaProducer.close();
	}

	@Override
	public void onMessage(String event, MessageEvent messageEvent) throws Exception {

		ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, messageEvent.getData());
		kafkaProducer.send(record);
		log.info("Data Sent :" + messageEvent.getData());
	}

	@Override
	public void onComment(String comment) {
		// TODO Auto-generated method stub

	}

	@Override
	public void onError(Throwable t) {
		log.error("Error in the stream :" + t);

	}

}
