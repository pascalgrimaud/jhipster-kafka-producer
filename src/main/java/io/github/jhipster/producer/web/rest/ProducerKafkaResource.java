package io.github.jhipster.producer.web.rest;

import io.github.jhipster.producer.service.ProducerKafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/producer-kafka")
public class ProducerKafkaResource {

    private final Logger log = LoggerFactory.getLogger(ProducerKafkaResource.class);

    private ProducerKafkaProducer kafkaProducer;

    public ProducerKafkaResource(ProducerKafkaProducer kafkaProducer) {
        this.kafkaProducer = kafkaProducer;
    }

    @PostMapping("/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
        log.debug("REST request to send to Kafka topic the message : {}", message);
        this.kafkaProducer.send(message);
    }
}
