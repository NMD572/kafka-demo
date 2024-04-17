package vn.nmd.kafka.demo.producer;

import java.util.concurrent.CompletableFuture;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import vn.nmd.kafka.demo.dto.Greeting;

@Component
public class MessageProducer {

	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	@Autowired
	private KafkaTemplate<String, Greeting> greetingKafkaTemplate;

	@Value(value = "${message.topic.name}")
	private String topicName;

	@Value(value = "${partitioned.topic.name}")
	private String partitionedTopicName;

	@Value(value = "${filtered.topic.name}")
	private String filteredTopicName;

	@Value(value = "${greeting.topic.name}")
	private String greetingTopicName;

	public void sendMessage(String message) {

		CompletableFuture<SendResult<String, String>> future = kafkaTemplate.send(topicName, message);
		future.whenComplete((result, ex) -> {

			if (ex == null) {
				System.out.println(
						"Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "]");
			} else {
				System.out.println("Unable to send message=[" + message + "] due to : " + ex.getMessage());
			}
		});
	}

	public void sendMessageToPartition(String message, int partition) {
		kafkaTemplate.send(partitionedTopicName, partition, null, message);
	}

	public void sendMessageToFiltered(String message) {
		kafkaTemplate.send(filteredTopicName, message);
	}

	public void sendGreetingMessage(Greeting greeting) {
		greetingKafkaTemplate.send(greetingTopicName, greeting);
	}
}
