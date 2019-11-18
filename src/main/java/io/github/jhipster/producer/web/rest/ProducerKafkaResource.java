package io.github.jhipster.producer.web.rest;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jhipster.producer.service.ProducerKafkaProducer;
import io.github.jhipster.producer.service.JsonKafkaProducer;
import io.github.jhipster.producer.web.rest.vm.FooVM;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/producer-kafka")
public class ProducerKafkaResource {

    private final Logger log = LoggerFactory.getLogger(ProducerKafkaResource.class);

    private ProducerKafkaProducer kafkaProducer;

    private JsonKafkaProducer jsonProducer;

    public ProducerKafkaResource(ProducerKafkaProducer kafkaProducer, JsonKafkaProducer jsonProducer) {
        this.kafkaProducer = kafkaProducer;
        this.jsonProducer = jsonProducer;
    }

    @PostMapping("/publish")
    public void sendMessageToKafkaTopic(@RequestParam("message") String message) {
        log.debug("REST request to send to Kafka topic the message : {}", message);
        this.kafkaProducer.send(message);
    }

    @PostMapping("/publish-json")
    public void sendJsonMessageToKafkaTopic(@RequestBody FooVM foo) {
        log.debug("REST request to send to Kafka topic the json message : {}", foo);
        ObjectMapper objectMapper = new ObjectMapper();
        this.jsonProducer.send(objectMapper.valueToTree(foo));
    }
}
