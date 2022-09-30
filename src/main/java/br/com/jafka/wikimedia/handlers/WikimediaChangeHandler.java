package br.com.jafka.wikimedia.handlers;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.MessageEvent;

import lombok.Builder;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;

@Slf4j
@Builder
@RequiredArgsConstructor
public class WikimediaChangeHandler implements EventHandler {
	
	final private KafkaProducer<String,String> producer;
	final private String topic;

	public void onOpen() throws Exception {
		log.info("############## STREAM IS OPENED ##############");
	}

	public void onClosed() throws Exception {
		this.producer.close();
		log.info("############## STREAM IS CLOSED ##############");
	}

	public void onMessage(String event, MessageEvent messageEvent) throws Exception {
		
		log.info(messageEvent.getData());
		
		final ProducerRecord<String, String> record = new ProducerRecord<String, String>(this.topic, messageEvent.getData());
		this.producer.send(record);
		
	}

	public void onComment(String comment) throws Exception {
		// TODO Auto-generated method stub
	}

	public void onError(Throwable t) {
		log.error("Error STREAM READING",t);
	}

}
