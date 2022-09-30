package br.com.jafka.wikimedia.producers;

import java.net.URI;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import com.launchdarkly.eventsource.EventHandler;
import com.launchdarkly.eventsource.EventSource;

import br.com.jafka.wikimedia.handlers.WikimediaChangeHandler;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class WikimediaChangesProducer {

	public static void main(String[] args) throws InterruptedException {
		final String SERVER = "172.29.229.90:9092";
		final String TOPIC = "jafka.wikimedia.recentchange";
		final URI URL_STREAM = URI.create("https://stream.wikimedia.org/v2/stream/recentchange");

		log.info(TOPIC);
		
		final Properties properties = new Properties();
		/*
		 * properties.setProperty("boostrap.serves", "127.0.0.1:9092");
		 * properties.setProperty("key.serializer", null);
		 * properties.setProperty("value.serializer", null);
		 */
		
		properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, SERVER);
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, Boolean.TRUE.toString());
		
		final KafkaProducer<String,String> producer = new KafkaProducer<String,String>(properties);
		
		final EventHandler event = WikimediaChangeHandler.builder()
				.producer(producer)
				.topic(TOPIC)
				.build();
		
		final EventSource source = new EventSource.Builder(event, URL_STREAM).build();
		
		source.start();
		
		TimeUnit.MINUTES.sleep(1);
	}
}
