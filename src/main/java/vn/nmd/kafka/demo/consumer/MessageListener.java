package vn.nmd.kafka.demo.consumer;

import java.util.concurrent.CountDownLatch;

import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.TopicPartition;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Component;

import vn.nmd.kafka.demo.dto.Greeting;

@Component
public class MessageListener {

	public CountDownLatch latch = new CountDownLatch(10);

	public CountDownLatch partitionLatch = new CountDownLatch(10);

	public CountDownLatch filterLatch = new CountDownLatch(10);

	public CountDownLatch greetingLatch = new CountDownLatch(10);

	@KafkaListener(topics = "${message.topic.name}", groupId = "${message.group.id.foo}", containerFactory = "fooKafkaListenerContainerFactory")
	public void listenGroupFoo(String message) {
		System.out.println("Received Message in group 'foo': " + message);
		latch.countDown();
	}

	@KafkaListener(topics = "${message.topic.name}", groupId = "${message.group.id.bar}", containerFactory = "barKafkaListenerContainerFactory")
	public void listenGroupBar(String message) {
		System.out.println("Received Message in group 'bar': " + message);
		latch.countDown();
	}

	@KafkaListener(topics = "${message.topic.name}", containerFactory = "headersKafkaListenerContainerFactory")
	public void listenWithHeaders(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
		System.out.println("Received Message: " + message + " from partition: " + partition);
		latch.countDown();
	}

	@KafkaListener(topicPartitions = @TopicPartition(topic = "${partitioned.topic.name}", partitions = { "0",
			"3" }), containerFactory = "partitionsKafkaListenerContainerFactory")
	public void listenToPartition(@Payload String message, @Header(KafkaHeaders.RECEIVED_PARTITION) int partition) {
		System.out.println("Received Message: " + message + " from partition: " + partition);
		this.partitionLatch.countDown();
	}

	@KafkaListener(topics = "${filtered.topic.name}", containerFactory = "filterKafkaListenerContainerFactory")
	public void listenWithFilter(String message) {
		System.out.println("Received Message in filtered listener: " + message);
		this.filterLatch.countDown();
	}

	@KafkaListener(topics = "${greeting.topic.name}", containerFactory = "greetingKafkaListenerContainerFactory")
	public void greetingListener(Greeting greeting) {
		System.out.println("Received greeting message: " + greeting);
		this.greetingLatch.countDown();
	}
}
