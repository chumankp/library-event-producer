package com.chuman.kafka.controller;

import com.chuman.kafka.domain.Book;
import com.chuman.kafka.domain.LibraryEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.*;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.test.EmbeddedKafkaBroker;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.TestPropertySource;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@EmbeddedKafka(topics = {"library-events"}, partitions = 3)
@TestPropertySource(properties = {"spring.kafka.producer.bootstrap-servers=${spring.embedded.kafka.brokers}",
        "spring.kafka.admin.properties.bootstrap.servers=${spring.embedded.kafka.brokers}"})
class LibraryEventControllerIntegrationTest {

    @Autowired
    TestRestTemplate testRestTemplate;
    @Autowired
    EmbeddedKafkaBroker embeddedKafkaBroker;


    private Consumer<Integer, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> config = new HashMap<>(KafkaTestUtils.consumerProps("group1", "true", embeddedKafkaBroker));
        consumer = new DefaultKafkaConsumerFactory<>(config, new IntegerDeserializer(), new StringDeserializer()).createConsumer();
        embeddedKafkaBroker.consumeFromAllEmbeddedTopics(consumer);
    }

    @AfterEach
    void tearDown() {
        consumer.close();
    }

    @Test
    @Timeout(5)
     void createLibraryEventTest() throws InterruptedException {
        //given
        Book book = Book.builder().bookId(123).bookAuthor("chuman")
                .bookName("kafka useing spring boot")
                .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                .libraryEventId(null)
                .book(book).build();

        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent);
        //when
        ResponseEntity<LibraryEvent> responseEntity = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.POST, request, LibraryEvent.class);
        //then
        assertEquals(HttpStatus.CREATED, responseEntity.getStatusCode());

        String expectedRecord = "{\"libraryEventId\":null,\"libraryEventType\":\"NEW\",\"book\":{\"bookId\":123,\"bookName\":\"kafka useing spring boot\",\"bookAuthor\":\"chuman\"}}";
        ConsumerRecord<Integer, String> consumerRecord =KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = consumerRecord.value();
        assertEquals(expectedRecord, value);
    }

    @Test
    @Timeout(5)
     void updateLibraryEventTest(){
        //given
        Book book = Book.builder()
                    .bookId(456)
                    .bookAuthor("chuman")
                    .bookName("kafka useing spring boot")
                    .build();
        LibraryEvent libraryEvent = LibraryEvent.builder()
                    .libraryEventId(123)
                    .book(book).build();
        HttpHeaders headers = new HttpHeaders();
        headers.set("content-type", MediaType.APPLICATION_JSON.toString());
        HttpEntity<LibraryEvent> request = new HttpEntity<>(libraryEvent);
        //when
        ResponseEntity<LibraryEvent> responseEntity = testRestTemplate.exchange("/v1/libraryevent", HttpMethod.PUT, request, LibraryEvent.class);

        assertEquals(HttpStatus.OK,responseEntity.getStatusCode());

        String expectedRecord = "{\"libraryEventId\":123,\"libraryEventType\":\"UPDATE\",\"book\":{\"bookId\":456,\"bookName\":\"kafka useing spring boot\",\"bookAuthor\":\"chuman\"}}";
        ConsumerRecord<Integer, String> consumerRecord =KafkaTestUtils.getSingleRecord(consumer, "library-events");
        String value = consumerRecord.value();
        assertEquals(expectedRecord, value);
    }
}
