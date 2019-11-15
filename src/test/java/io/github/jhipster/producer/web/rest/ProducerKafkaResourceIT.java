package io.github.jhipster.producer.web.rest;

import io.github.jhipster.producer.ProducerApp;
import io.github.jhipster.producer.service.ProducerKafkaProducer;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.KafkaContainer;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;

@SpringBootTest(classes = ProducerApp.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ProducerKafkaResourceIT {

    private MockMvc restMockMvc;

    private static boolean started = false;

    private static KafkaContainer kafkaContainer;

    @Autowired
    private ProducerKafkaProducer producer;


    private static final int MAX_ATTEMPT = 5;

    @BeforeAll
    public static void startServer() {
        if (!started) {
            startTestcontainer();
            started = true;
        }
    }

    private static void startTestcontainer() {
        kafkaContainer = new KafkaContainer("5.3.1");
        kafkaContainer.start();
        System.setProperty("kafkaBootstrapServers", kafkaContainer.getBootstrapServers());
    }

    @BeforeEach
    public void setup() {
        ProducerKafkaResource kafkaResource = new ProducerKafkaResource(producer);

        this.restMockMvc = MockMvcBuilders.standaloneSetup(kafkaResource)
            .build();

        producer.init();
    }


}

