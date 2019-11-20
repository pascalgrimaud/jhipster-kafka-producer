package io.github.jhipster.producer.web.rest;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.github.jhipster.producer.ProducerApp;
import io.github.jhipster.producer.service.ProducerKafkaProducer;
import io.github.jhipster.producer.service.JsonKafkaProducer;
import io.github.jhipster.producer.web.rest.vm.FooVM;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.shaded.com.google.common.collect.ImmutableMap;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;

import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

@SpringBootTest(classes = ProducerApp.class)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_CLASS)
public class ProducerKafkaResourceIT {

    private MockMvc restMockMvc;

    private static boolean started = false;

    private static KafkaContainer kafkaContainer;

    private KafkaConsumer<String, String> stringConsumer;

    private KafkaConsumer<String, JsonNode> jsonConsumer;

    @Autowired
    private ProducerKafkaProducer stringProducer;

    @Autowired
    private JsonKafkaProducer jsonProducer;

    private static final int MAX_ATTEMPT = 5;

    public static final String JSON_MESSAGE_TOPIC = "json_message_topic";

    public static final String STRING_MESSAGE_TOPIC = "string_message_topic";


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

        ProducerKafkaResource kafkaResource = new ProducerKafkaResource(stringProducer, jsonProducer);

        this.restMockMvc = MockMvcBuilders.standaloneSetup(kafkaResource)
            .build();

        stringProducer.init();

        stringConsumer = new KafkaConsumer<>(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "group_id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            new StringDeserializer(),
            new StringDeserializer()
        );
        stringConsumer.subscribe(Collections.singletonList(STRING_MESSAGE_TOPIC));
        stringConsumer.poll(Duration.ofSeconds(0));


        jsonProducer.setBootstrapServers(kafkaContainer.getBootstrapServers());
        jsonProducer.init();

       jsonConsumer = new KafkaConsumer<>(
            ImmutableMap.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "group_id",
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest"
            ),
            new StringDeserializer(),
            new JsonDeserializer()
        );

       jsonConsumer.subscribe(Collections.singletonList(JSON_MESSAGE_TOPIC));
       jsonConsumer.poll(Duration.ofSeconds(0));

    }


    @AfterEach
    public void teardown() {
        stringConsumer.close();
        jsonConsumer.close();

    }

    @Test
    public void producedStringMessageHasBeenConsumed() throws Exception {

        restMockMvc.perform(post("/api/producer-kafka/publish?message=test"))
            .andExpect(status().isOk());

        ConsumerRecords<String, String> records = stringConsumer.poll(Duration.ofSeconds(3));


        Map<MetricName, ? extends Metric> metrics = stringConsumer.metrics();

        Metric recordsConsumedTotalMetric = metrics.entrySet().stream()
            .filter(entry -> "records-consumed-total".equals(entry.getKey().name()))
            .findFirst()
            .get()
            .getValue();

        Double expectedTotalConsumedMessage = 1.0;
        Double totalConsumedMessage;
        int attempt = 0;
        do {
            totalConsumedMessage = (Double) recordsConsumedTotalMetric.metricValue();
            Thread.sleep(200);
        } while (!totalConsumedMessage.equals(expectedTotalConsumedMessage) && attempt++ < MAX_ATTEMPT);

        Assertions.assertThat(attempt).isLessThan(MAX_ATTEMPT);
        Assertions.assertThat(totalConsumedMessage).isEqualTo(expectedTotalConsumedMessage);
    }

    @Test
    public void producedJsonMessageHasBeenConsumed() throws Exception {

        FooVM foo = new FooVM();
        foo.setField1("Test");
        foo.setField2("Test");
        foo.setNumber(1);

        restMockMvc.perform(post("/api/producer-kafka/publish-json")
            .contentType(TestUtil.APPLICATION_JSON_UTF8)
            .content(TestUtil.convertObjectToJsonBytes(foo)))
            .andExpect(status().isOk());

        ConsumerRecords<String, JsonNode> records = jsonConsumer.poll(Duration.ofSeconds(3));

        Map<MetricName, ? extends Metric> metrics = jsonConsumer.metrics();

        Metric recordsConsumedTotalMetric = metrics.entrySet().stream()
            .filter(entry -> "records-consumed-total".equals(entry.getKey().name()))
            .findFirst()
            .get()
            .getValue();

        Double expectedTotalConsumedMessage = 1.0;
        Double totalConsumedMessage;
        int attempt = 0;
        do {
            totalConsumedMessage = (Double) recordsConsumedTotalMetric.metricValue();
            Thread.sleep(200);
        } while (!totalConsumedMessage.equals(expectedTotalConsumedMessage) && attempt++ < MAX_ATTEMPT);

        Assertions.assertThat(attempt).isLessThan(MAX_ATTEMPT);
        Assertions.assertThat(totalConsumedMessage).isEqualTo(expectedTotalConsumedMessage);
    }





}

